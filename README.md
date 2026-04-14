# streamhub

Resumable, durable LLM/AI streaming for Go — backed by Redis.

`streamhub` is the Go equivalent of [vercel/resumable-stream](https://github.com/vercel/resumable-stream). It lets you build **LLM and AI agent streams that survive disconnects, reconnects, and process restarts** — without coupling the producer and consumer to the same lifetime or server instance.

## The problem

When you stream an LLM response (or any long-running AI task) over SSE/WebSocket, a page refresh, network blip, or load-balancer failover kills the connection. The generation keeps running on the backend, but the client loses all in-flight tokens. Restarting is expensive — both in cost and user experience.

In a distributed setup, it gets worse: the process generating the stream and the process serving the client may be different instances entirely. You need durable, shared state to reconnect them.

## How streamhub solves it

- **Redis Streams** store every chunk, so new or reconnecting subscribers **replay** what they missed and then seamlessly receive live data — no duplicates, no gaps.
- **Redis Pub/Sub** delivers **cancel signals** instantly, so a user can stop generation from any client or service instance.
- **Generation-based fencing** (like a fencing token) prevents stale producers from writing into a newer stream after losing ownership.
- **Single-producer registration** ensures only one producer runs per session — no duplicate LLM calls.

## Use cases

- LLM / AI agent response streaming (ChatGPT-style, Claude-style)
- Server-Sent Events (SSE) or WebSocket streaming with reconnect and replay
- Incremental output from long-running background tasks
- Multi-instance stream sharing with durable state in Redis
- Remote cancel of in-progress AI generation

## Comparison

| Feature | streamhub (Go) | [vercel/resumable-stream](https://github.com/vercel/resumable-stream) (TS) | [ai-resumable-stream](https://github.com/zirkelc/ai-resumable-stream) (TS) |
|---|---|---|---|
| Language | **Go** | TypeScript | TypeScript |
| Redis Stream persistence | ✅ | ✅ | ✅ |
| Pub/Sub cancel signal | ✅ | ❌ | ✅ |
| Reconnect with replay | ✅ | ✅ | ✅ |
| Generation fencing token | ✅ | ✅ | ❌ |
| Single-producer registration | ✅ | ❌ | ❌ |
| Per-stream metadata | ✅ | ❌ | ❌ |
| Framework-agnostic | ✅ | Tied to AI SDK | Tied to AI SDK |

## Requirements

- Go `1.26`
- Redis

## Install

```bash
go get github.com/gtoxlili/streamhub
```

## Quick start

Create a `Hub`:

```go
package main

import (
	"log"

	"github.com/gtoxlili/streamhub"
	"github.com/redis/rueidis"
)

func main() {
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{"127.0.0.1:6379"},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	hub := streamhub.New(client)
	_ = hub
}
```

Register on the producer side:

```go
stream, created, err := hub.Register("chat:123", func() {
	// cancel your runtime task here (e.g. abort LLM generation)
})
if err != nil {
	log.Fatal(err)
}
if !created {
	// another producer already owns this session — do not start a duplicate
	return
}
defer stream.Close()

stream.SetMetadata(map[string]any{
	"model": "claude-sonnet-4-20250514",
})

stream.Publish("hello")
stream.Publish(" world")
```

The key thing here is `created`. If it is `false`, this session already has an active producer, and you should not start another one.

Subscribe from any instance:

```go
stream := hub.Get("chat:123")
if stream == nil {
	return
}

// Replays all existing chunks first, then streams new ones live
chunks, unsubscribe := stream.Subscribe(128)
defer unsubscribe()

for chunk := range chunks {
	// write to SSE / WebSocket / HTTP response
	println(chunk)
}
```

Cancel a running stream remotely:

```go
stream := hub.Get("chat:123")
if stream != nil {
	stream.Cancel()
}
```

## API

### `streamhub.New(client)`

Creates a `Hub`.

### `hub.Register(sessionID, cancelRuntime)`

Tries to register a new stream.

- `sessionID` is your business/session identifier
- `cancelRuntime` is called when a cancel signal is received
- `created` tells you whether this call actually became the producer

### `hub.Get(sessionID)`

Returns a proxy for an existing stream. If Redis has no such stream, it returns `nil`.

### `hub.Active(sessionIDs)`

Checks which session IDs are still active.

### `hub.Remove(sessionID)`

Deletes the Redis keys and local state for a stream.

### `stream.Publish(chunk)`

Publishes a chunk.

### `stream.Subscribe(bufExtra)`

Subscribes to the stream. Existing chunks are replayed first, then new chunks are delivered live.

### `stream.SetMetadata(v)` / `stream.Metadata(&target)`

Stores and loads per-stream metadata.

### `stream.Cancel()`

Sends a cancel signal via Redis Pub/Sub.

### `stream.Close()`

Marks the stream as done and stops local subscriber loops.

### `stream.Done()`

Reports whether the stream is already done.

## Typical flow

1. Call `Register` when a request comes in
2. Start the actual background job only when `created == true`
3. Keep calling `Publish` while the job is running
4. Let readers consume data through `Get(...).Subscribe(...)` — from any instance
5. Call `Close` when the job finishes
6. Call `Cancel` if the user stops the session early

If your service runs on multiple instances, this is much easier to manage than keeping the stream state in process memory.

## Notes

- Do not start another producer when `created == false`
- Call `SetMetadata` before `Close`
- Always call the `unsubscribe` returned by `Subscribe`
- Stream lifetime and expiry are driven by Redis state

## License

GPL-3.0. See [LICENSE](./LICENSE).
