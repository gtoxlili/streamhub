// Package streamhub provides resumable LLM streaming for Go, backed by Redis.
//
// It is meant for the case where the code producing a stream and the code
// consuming it don't share the same lifetime — they might not even be on
// the same instance. Chunks are stored in Redis Streams so reconnecting
// subscribers can replay what they missed, and cancel signals are delivered
// via Redis Pub/Sub so a generation can be stopped from anywhere.
//
// Each producer gets a generation ID as a fencing token, and only one
// producer can own a session at a time.
package streamhub

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gtoxlili/streamhub/pkg/identity/uuidutil"
	"github.com/redis/rueidis"
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

// publishScript checks generation and status before writing a chunk.
// Returns 1 when the write succeeds, 0 when the generation is stale or the
// stream has already been closed.
// KEYS[1] = meta key, KEYS[2] = chunks key
// ARGV[1] = generation, ARGV[2] = chunk data, ARGV[3] = TTL seconds
var publishScript = rueidis.NewLuaScript(`
local meta = redis.call("HMGET", KEYS[1], "status", "gen")
if meta[1] ~= "active" or meta[2] ~= ARGV[1] then return 0 end
redis.call("XADD", KEYS[2], "*", "d", ARGV[2])
redis.call("EXPIRE", KEYS[1], tonumber(ARGV[3]))
redis.call("EXPIRE", KEYS[2], tonumber(ARGV[3]))
return 1
`)

// setMetadataScript writes metadata only when the caller still owns the stream.
// KEYS[1] = meta key
// ARGV[1] = generation, ARGV[2] = metadata JSON
var setMetadataScript = rueidis.NewLuaScript(`
if redis.call("HGET", KEYS[1], "gen") ~= ARGV[1] then return 0 end
redis.call("HSET", KEYS[1], "metadata", ARGV[2])
return 1
`)

// closeScript atomically marks the stream as done and trims TTL, but only
// when the caller is still the current owner. Without this, a slow Close
// coming from a stale producer could race a takeover and overwrite the
// new generation's status/TTL.
// KEYS[1] = meta key, KEYS[2] = chunks key
// ARGV[1] = generation, ARGV[2] = post-close TTL seconds
var closeScript = rueidis.NewLuaScript(`
if redis.call("HGET", KEYS[1], "gen") ~= ARGV[1] then return 0 end
redis.call("HSET", KEYS[1], "status", "done")
redis.call("EXPIRE", KEYS[1], tonumber(ARGV[2]))
redis.call("EXPIRE", KEYS[2], tonumber(ARGV[2]))
return 1
`)

// Hub manages Redis-backed [LiveStream] values.
type Hub struct {
	client rueidis.Client

	mu     sync.Mutex
	locals map[string]*localState
}

// localState keeps local-only state for the current producer.
type localState struct {
	generation string
	cancelSub  context.CancelFunc // stops heartbeat and Pub/Sub listener
}

// New creates a Hub from a Redis client.
func New(client rueidis.Client) *Hub {
	return &Hub{
		client: client,
		locals: make(map[string]*localState),
	}
}

// Register tries to create a new [LiveStream] for sessionID.
// If created is false, the caller must not start another producer.
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

	// Keep the lease alive and watch for cancel.
	go h.heartbeat(subCtx, sessionID, gen, cancelRuntime)
	// Deliver cancel as soon as Pub/Sub sees it.
	go h.listenCancel(subCtx, sessionID, cancelRuntime)

	h.mu.Lock()
	if old, ok := h.locals[sessionID]; ok {
		old.cancelSub() // stop the old generation's local loops
	}
	h.locals[sessionID] = &localState{
		generation: gen,
		cancelSub:  cancelSub,
	}
	h.mu.Unlock()

	return newLiveStream(h, sessionID, gen), true, nil
}

// Get returns a stream proxy for sessionID, or nil if it does not exist.
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

// Active reports which session IDs still have an unfinished stream.
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

// Remove deletes a stream's Redis keys and local state.
func (h *Hub) Remove(sessionID string) {
	ctx := context.Background()
	h.client.Do(ctx, h.client.B().Del().Key(chunksKey(sessionID), metaKey(sessionID)).Build())
	h.cleanupLocal(sessionID, "")
}

// cleanupLocal clears local state and stops the local background loops.
// If generation is set, it must match the current owner.
func (h *Hub) cleanupLocal(sessionID, generation string) {
	h.mu.Lock()
	ls, ok := h.locals[sessionID]
	if ok && (generation == "" || ls.generation == generation) {
		delete(h.locals, sessionID)
	} else {
		ok = false // do not stop another generation's listener
	}
	h.mu.Unlock()

	if ok && ls.cancelSub != nil {
		ls.cancelSub()
	}
}

// heartbeat refreshes TTL and checks the durable cancel flag.
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
			// Stop if we are no longer the current owner. The lease likely
			// expired and another Register has taken over, so tell the
			// runtime to bail out — its Publishes are silent no-ops now.
			// Also clean up our local entry; the normal Close path won't
			// run for this producer, and leaving locals[sid] would leak
			// forever across heartbeat terminations.
			gen, _ := h.client.Do(ctx, h.client.B().Hget().Key(mk).Field("gen").Build()).ToString()
			// If ctx was cancelled (normal Close via cleanupLocal), bail
			// before acting on a partial HGET — otherwise a cancelled-out
			// empty gen would fire a spurious cancelRuntime, and the
			// subsequent TTL refresh would undo Close's 60s post-done trim.
			if ctx.Err() != nil {
				return
			}
			if gen != generation {
				cancelRuntime()
				h.cleanupLocal(sessionID, generation)
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

// listenCancel listens on the session cancel channel.
func (h *Hub) listenCancel(ctx context.Context, sessionID string, cancelRuntime func()) {
	_ = h.client.Receive(ctx,
		h.client.B().Subscribe().Channel(cancelChannel(sessionID)).Build(),
		func(msg rueidis.PubSubMessage) { cancelRuntime() },
	)
}

const keyPrefix = "streamhub:"

// Hash tag "{sessionID}" forces meta and chunks keys into the same slot so
// Lua scripts that touch both don't hit CROSSSLOT in Redis Cluster.
func chunksKey(sessionID string) string     { return keyPrefix + "{" + sessionID + "}:chunks" }
func metaKey(sessionID string) string       { return keyPrefix + "{" + sessionID + "}:meta" }
func cancelChannel(sessionID string) string { return keyPrefix + "{" + sessionID + "}:cancel" }
