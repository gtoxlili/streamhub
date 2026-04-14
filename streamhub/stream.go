package streamhub

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/rueidis"
)

const (
	// xreadBlock is the XREAD BLOCK timeout. Keep it short so that
	// xreadLoop can detect stream completion promptly.
	xreadBlock = 500 * time.Millisecond
	// keyTTL is how long Redis keys are kept after Close for late
	// xreadLoop iterations on other instances to detect "done".
	keyTTL = 60 * time.Second
)

// LiveStream is a Redis-backed session stream proxy.
// Producer-side instances carry a generation used for fencing;
// subscriber-only proxies (from Get) have an empty generation.
type LiveStream struct {
	client     rueidis.Client
	sessionID  string
	generation string // fencing token; empty for subscriber-only proxies
	hub        *Hub

	mu     sync.Mutex
	subs   map[uint64]*subscriber
	nextID uint64

	closeOnce sync.Once
	closeCh   chan struct{} // closed by Close() to signal local xreadLoops
}

type subscriber struct {
	ch     chan string
	cancel context.CancelFunc // cancels the xreadLoop goroutine
}

func newLiveStream(hub *Hub, sessionID, generation string) *LiveStream {
	return &LiveStream{
		client:     hub.client,
		sessionID:  sessionID,
		generation: generation,
		hub:        hub,
		subs:       make(map[uint64]*subscriber),
		closeCh:    make(chan struct{}),
	}
}

// activeTTL is the lease duration for active streams. The producer
// refreshes it via heartbeat + Publish. If the producer crashes, the
// keys expire and new requests can register fresh.
const activeTTL = 600 // seconds (10 min)

// Publish appends a chunk to the Redis Stream. Uses a Lua script to
// check the generation fencing token before writing, so a stale
// producer that lost its lease cannot pollute a newer generation's stream.
func (s *LiveStream) Publish(chunk string) {
	ctx := context.Background()
	publishScript.Exec(ctx, s.client,
		[]string{metaKey(s.sessionID), chunksKey(s.sessionID)},
		[]string{s.generation, chunk, fmt.Sprint(activeTTL)},
	)
}

// Subscribe returns a channel that replays all existing chunks from Redis,
// then delivers live chunks via XREAD BLOCK. The caller must call the
// returned unsubscribe function when done (e.g. HTTP disconnect).
//
// If the stream is already complete, the channel is pre-filled with all
// chunks and immediately closed.
func (s *LiveStream) Subscribe(bufExtra int) (<-chan string, func()) {
	if bufExtra <= 0 {
		bufExtra = 256
	}
	ctx := context.Background()
	key := chunksKey(s.sessionID)

	// Replay existing chunks.
	entries, _ := s.client.Do(ctx,
		s.client.B().Xrange().Key(key).Start("-").End("+").Build(),
	).AsXRange()

	ch := make(chan string, len(entries)+bufExtra)
	lastID := "0-0"
	for _, e := range entries {
		if d, ok := e.FieldValues["d"]; ok {
			ch <- d
		}
		lastID = e.ID
	}

	// Already done — drain any chunks published after the initial XRANGE
	// but before we checked Done, then close.
	if s.Done() {
		s.drainRemaining(key, lastID, ch)
		close(ch)
		return ch, func() {}
	}

	// Start xreadLoop for live delivery.
	subCtx, subCancel := context.WithCancel(context.Background())
	id := s.addSubscriber(ch, subCancel)
	go s.xreadLoop(subCtx, key, lastID, ch)

	return ch, func() {
		subCancel()
		s.removeSubscriber(id)
	}
}

func (s *LiveStream) addSubscriber(ch chan string, cancel context.CancelFunc) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.nextID
	s.nextID++
	s.subs[id] = &subscriber{ch: ch, cancel: cancel}
	return id
}

func (s *LiveStream) removeSubscriber(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subs, id)
}

// xreadLoop polls Redis for new chunks and writes them to ch.
// It exits (and closes ch) when ctx is cancelled or the stream is done.
func (s *LiveStream) xreadLoop(ctx context.Context, key, cursor string, ch chan string) {
	defer close(ch)

	for {
		// Check for cancellation or local close signal.
		select {
		case <-ctx.Done():
			s.drainRemaining(key, cursor, ch)
			return
		case <-s.closeCh:
			s.drainRemaining(key, cursor, ch)
			return
		default:
		}

		var entries map[string][]rueidis.XRangeEntry
		err := s.client.Dedicated(func(dc rueidis.DedicatedClient) error {
			var readErr error
			entries, readErr = dc.Do(ctx,
				dc.B().Xread().Count(64).Block(xreadBlock.Milliseconds()).
					Streams().Key(key).Id(cursor).Build(),
			).AsXRead()
			return readErr
		})

		if err != nil {
			if ctx.Err() != nil {
				s.drainRemaining(key, cursor, ch)
				return
			}
			if rueidis.IsRedisNil(err) {
				// XREAD timeout — check if stream ended.
				if s.Done() {
					s.drainRemaining(key, cursor, ch)
					return
				}
				continue
			}
			// Transient error.
			time.Sleep(100 * time.Millisecond)
			continue
		}

		for _, rangeEntries := range entries {
			for _, e := range rangeEntries {
				if d, ok := e.FieldValues["d"]; ok {
					ch <- d
				}
				cursor = e.ID
			}
		}
	}
}

// drainRemaining reads any chunks after cursor that haven't been
// delivered yet and writes them to ch before the channel is closed.
// Uses non-blocking sends — if the buffer is full (consumer gone),
// chunks are silently dropped.
func (s *LiveStream) drainRemaining(key, cursor string, ch chan string) {
	entries, err := s.client.Do(context.Background(),
		s.client.B().Xrange().Key(key).Start("("+cursor).End("+").Build(),
	).AsXRange()
	if err != nil {
		return
	}
	for _, e := range entries {
		if d, ok := e.FieldValues["d"]; ok {
			select {
			case ch <- d:
			default:
			}
		}
	}
}

// SetMetadata stores caller-defined data in Redis as JSON.
// Must be called before Close. Fenced by generation — a stale producer
// cannot overwrite a newer generation's metadata.
func (s *LiveStream) SetMetadata(v any) {
	if s.generation == "" {
		return
	}
	ctx := context.Background()
	mk := metaKey(s.sessionID)
	// Check generation before writing.
	gen, _ := s.client.Do(ctx, s.client.B().Hget().Key(mk).Field("gen").Build()).ToString()
	if gen != s.generation {
		return
	}
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	s.client.Do(ctx,
		s.client.B().Hset().Key(mk).FieldValue().FieldValue("metadata", string(b)).Build(),
	)
}

// Metadata reads the stored metadata from Redis and unmarshals it into
// the target pointer. Returns true on success.
func (s *LiveStream) Metadata(target any) bool {
	raw, err := s.client.Do(context.Background(),
		s.client.B().Hget().Key(metaKey(s.sessionID)).Field("metadata").Build(),
	).ToString()
	if err != nil || raw == "" {
		return false
	}
	return json.Unmarshal([]byte(raw), target) == nil
}

// Close marks the stream as done, signals all local xreadLoop goroutines
// to exit, and sets a TTL on the Redis keys for auto-cleanup.
// If this LiveStream's generation is stale (no longer matches Redis),
// Close is a local-only no-op — it won't touch Redis or the new
// generation's infrastructure.
func (s *LiveStream) Close() {
	s.closeOnce.Do(func() {
		ctx := context.Background()
		mk := metaKey(s.sessionID)
		ck := chunksKey(s.sessionID)

		// Fencing: only write to Redis if we're still the current owner.
		if s.generation != "" {
			gen, _ := s.client.Do(ctx,
				s.client.B().Hget().Key(mk).Field("gen").Build(),
			).ToString()
			if gen != s.generation {
				// Stale producer — don't touch Redis or the new generation's locals.
				close(s.closeCh)
				return
			}
		}

		// Mark done in Redis.
		s.client.Do(ctx, s.client.B().Hset().Key(mk).
			FieldValue().FieldValue("status", "done").Build())
		// TTL for auto-cleanup — gives remote xreadLoops time to see "done".
		s.client.Do(ctx, s.client.B().Expire().Key(mk).Seconds(int64(keyTTL.Seconds())).Build())
		s.client.Do(ctx, s.client.B().Expire().Key(ck).Seconds(int64(keyTTL.Seconds())).Build())

		// Cancel all local xreadLoop contexts to interrupt XREAD BLOCK
		// immediately, then signal via closeCh as a fallback.
		s.mu.Lock()
		for _, sub := range s.subs {
			sub.cancel()
		}
		s.mu.Unlock()
		close(s.closeCh)

		// Clean up local cancel listener + heartbeat (fenced).
		s.hub.cleanupLocal(s.sessionID, s.generation)
	})
}

// Cancel broadcasts a cancel signal via Redis Pub/Sub and writes a
// durable flag to the Hash as a fallback for the Pub/Sub startup race.
func (s *LiveStream) Cancel() {
	ctx := context.Background()
	mk := metaKey(s.sessionID)
	s.client.DoMulti(ctx,
		s.client.B().Hset().Key(mk).FieldValue().FieldValue("cancel", "1").Build(),
		s.client.B().Publish().Channel(cancelChannel(s.sessionID)).Message("cancel").Build(),
	)
}

// Done reports whether the stream is marked as completed in Redis.
func (s *LiveStream) Done() bool {
	status, err := s.client.Do(context.Background(),
		s.client.B().Hget().Key(metaKey(s.sessionID)).Field("status").Build(),
	).ToString()
	return err == nil && status == "done"
}
