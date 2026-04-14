// Package streamhub provides a Redis-backed stream broker that decouples
// producer lifetime from consumer (HTTP connection) lifetime.
//
// Chunks are stored in Redis Streams so that any instance can replay and
// subscribe. Cancel signals are broadcast via Redis Pub/Sub so that the
// instance holding the runtime receives the stop request regardless of
// which instance the cancel HTTP request lands on.
//
// Each registration carries a unique generation ID used as a fencing
// token: stale producers that lost their lease cannot pollute a newer
// generation's stream or tear down its infrastructure.
package streamhub

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/rueidis"
	"github.com/yomomo-ai/agent-core/pkg/identity/uuidutil"
)

// KEYS[1] = meta key, KEYS[2] = chunks key
// ARGV[1] = active TTL seconds, ARGV[2] = generation ID
var registerScript = rueidis.NewLuaScript(`
local status = redis.call("HGET", KEYS[1], "status")
if status == false or status == "done" then
    redis.call("DEL", KEYS[1], KEYS[2])
    redis.call("HSET", KEYS[1], "status", "active", "gen", ARGV[2])
    redis.call("EXPIRE", KEYS[1], ARGV[1])
    redis.call("EXPIRE", KEYS[2], ARGV[1])
    return 1
end
return 0
`)

// publishScript atomically checks generation before writing a chunk.
// Returns 1 if the chunk was written, 0 if the generation is stale.
// KEYS[1] = meta key, KEYS[2] = chunks key
// ARGV[1] = generation, ARGV[2] = chunk data, ARGV[3] = TTL seconds
var publishScript = rueidis.NewLuaScript(`
if redis.call("HGET", KEYS[1], "gen") ~= ARGV[1] then return 0 end
redis.call("XADD", KEYS[2], "*", "d", ARGV[2])
redis.call("EXPIRE", KEYS[1], tonumber(ARGV[3]))
redis.call("EXPIRE", KEYS[2], tonumber(ARGV[3]))
return 1
`)

// Hub manages [LiveStream]s backed by Redis.
type Hub struct {
	client rueidis.Client

	mu     sync.Mutex
	locals map[string]*localState
}

// localState holds per-session data that only makes sense on the
// instance that owns the producer goroutine.
type localState struct {
	generation string
	cancelSub  context.CancelFunc // stops heartbeat + Pub/Sub listener
}

// New creates a Hub backed by the given Redis client.
func New(client rueidis.Client) *Hub {
	return &Hub{
		client: client,
		locals: make(map[string]*localState),
	}
}

// Register atomically creates a new [LiveStream] for the given session.
// It returns the stream and true if a new one was created, or a proxy
// to the existing in-progress stream and false.
// The caller MUST NOT start a producer when created == false.
// Returns a non-nil error only when Redis is unavailable.
func (h *Hub) Register(sessionID string, cancelRuntime func()) (*LiveStream, bool, error) {
	ctx := context.Background()
	mk := metaKey(sessionID)
	ck := chunksKey(sessionID)
	gen := uuidutil.NewV7String()

	result, err := registerScript.Exec(ctx, h.client,
		[]string{mk, ck}, []string{fmt.Sprint(activeTTL), gen},
	).AsInt64()
	if err != nil {
		return nil, false, fmt.Errorf("streamhub: register %s: %w", sessionID, err)
	}
	if result == 0 {
		return newLiveStream(h, sessionID, ""), false, nil
	}

	subCtx, cancelSub := context.WithCancel(context.Background())

	// Background heartbeat: refresh TTL + poll durable cancel flag.
	go h.heartbeat(subCtx, sessionID, gen, cancelRuntime)
	// Pub/Sub listener for instant cancel delivery.
	go h.listenCancel(subCtx, sessionID, cancelRuntime)

	h.mu.Lock()
	if old, ok := h.locals[sessionID]; ok {
		old.cancelSub() // stop stale generation's heartbeat + Pub/Sub listener
	}
	h.locals[sessionID] = &localState{
		generation: gen,
		cancelSub:  cancelSub,
	}
	h.mu.Unlock()

	return newLiveStream(h, sessionID, gen), true, nil
}

// Get returns a stream proxy for sessionID, or nil if no stream exists
// in Redis.
func (h *Hub) Get(sessionID string) *LiveStream {
	ctx := context.Background()
	status, err := h.client.Do(ctx,
		h.client.B().Hget().Key(metaKey(sessionID)).Field("status").Build(),
	).ToString()
	if err != nil || status == "" {
		return nil
	}
	return newLiveStream(h, sessionID, "")
}

// Active returns the set of session IDs that currently have a
// non-completed stream.
func (h *Hub) Active(sessionIDs []string) map[string]bool {
	if len(sessionIDs) == 0 {
		return nil
	}
	ctx := context.Background()
	cmds := make(rueidis.Commands, 0, len(sessionIDs))
	for _, id := range sessionIDs {
		cmds = append(cmds, h.client.B().Hget().Key(metaKey(id)).Field("status").Build())
	}
	results := h.client.DoMulti(ctx, cmds...)

	out := make(map[string]bool, len(sessionIDs))
	for i, resp := range results {
		if s, _ := resp.ToString(); s == "active" {
			out[sessionIDs[i]] = true
		}
	}
	return out
}

// Remove immediately evicts a stream's Redis keys and local state.
func (h *Hub) Remove(sessionID string) {
	ctx := context.Background()
	h.client.Do(ctx, h.client.B().Del().Key(chunksKey(sessionID), metaKey(sessionID)).Build())
	h.cleanupLocal(sessionID, "")
}

// cleanupLocal removes local cancel state and stops the heartbeat +
// Pub/Sub listener. If generation is non-empty, only cleans up if it
// matches (fencing). Empty generation means unconditional cleanup
// (used by Remove for error paths).
func (h *Hub) cleanupLocal(sessionID, generation string) {
	h.mu.Lock()
	ls, ok := h.locals[sessionID]
	if ok && (generation == "" || ls.generation == generation) {
		delete(h.locals, sessionID)
	} else {
		ok = false // don't cancel a different generation's listener
	}
	h.mu.Unlock()

	if ok && ls.cancelSub != nil {
		ls.cancelSub()
	}
}

// heartbeat periodically refreshes the TTL on session keys and polls
// the durable cancel flag. Runs every 2 seconds. Stops immediately if
// the generation in Redis no longer matches (stale producer).
func (h *Hub) heartbeat(ctx context.Context, sessionID, generation string, cancelRuntime func()) {
	mk := metaKey(sessionID)
	ck := chunksKey(sessionID)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Fencing: stop if we're no longer the current owner.
			gen, _ := h.client.Do(ctx, h.client.B().Hget().Key(mk).Field("gen").Build()).ToString()
			if gen != generation {
				return
			}
			h.client.DoMulti(ctx,
				h.client.B().Expire().Key(mk).Seconds(activeTTL).Build(),
				h.client.B().Expire().Key(ck).Seconds(activeTTL).Build(),
			)
			if v, _ := h.client.Do(ctx, h.client.B().Hget().Key(mk).Field("cancel").Build()).ToString(); v == "1" {
				cancelRuntime()
				return
			}
		}
	}
}

// listenCancel subscribes to the cancel Pub/Sub channel for a session
// and invokes cancelRuntime when a message arrives.
func (h *Hub) listenCancel(ctx context.Context, sessionID string, cancelRuntime func()) {
	_ = h.client.Receive(ctx,
		h.client.B().Subscribe().Channel(cancelChannel(sessionID)).Build(),
		func(msg rueidis.PubSubMessage) { cancelRuntime() },
	)
}

// --- helpers ---

const keyPrefix = "streamhub:"

func chunksKey(sessionID string) string     { return keyPrefix + sessionID + ":chunks" }
func metaKey(sessionID string) string       { return keyPrefix + sessionID + ":meta" }
func cancelChannel(sessionID string) string { return keyPrefix + sessionID + ":cancel" }

