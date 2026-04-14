# streamhub

Resumable LLM streaming for Go, backed by Redis.

`streamhub` is meant for the fairly common case where the code producing a stream and the code consuming it don't share the same lifetime — they might not even be on the same instance. Think LLM responses, SSE endpoints, or anything where you need the stream to survive reconnects.

It uses Redis Streams for chunk persistence (so subscribers can replay what they missed) and Redis Pub/Sub for cancel signals (so you can stop a generation from anywhere). Each producer gets a generation ID as a fencing token, and only one producer can own a session at a time.

## Requirements

- Go `1.26`
- Redis

## Install

```bash
go get github.com/gtoxlili/streamhub
```

## Usage

Create a `Hub`:

```go
client, err := rueidis.NewClient(rueidis.ClientOption{
	InitAddress: []string{"127.0.0.1:6379"},
})
if err != nil {
	log.Fatal(err)
}
defer client.Close()

hub := streamhub.New(client)
```

Register a producer:

```go
stream, created, err := hub.Register("chat:123", func() {
	// called when someone cancels this session
})
if err != nil {
	log.Fatal(err)
}
if !created {
	return // another instance already owns this session
}
defer stream.Close()

stream.SetMetadata(map[string]any{"model": "claude-sonnet-4-20250514"})

stream.Publish("hello")
stream.Publish(" world")
```

`created` is the important bit — if it's `false`, a producer is already running for this session.

Subscribe (from any instance):

```go
stream := hub.Get("chat:123")
if stream == nil {
	return
}

chunks, unsubscribe := stream.Subscribe(128)
defer unsubscribe()

for chunk := range chunks {
	// replays existing chunks first, then streams live
	println(chunk)
}
```

Cancel:

```go
if stream := hub.Get("chat:123"); stream != nil {
	stream.Cancel()
}
```

## API

### `streamhub.New(client)`

Creates a `Hub`.

### `hub.Register(sessionID, cancelRuntime)`

Tries to claim a session as producer. Returns `(stream, created, err)` — check `created` before writing.

### `hub.Get(sessionID)`

Returns a handle for an existing stream, or `nil`.

### `hub.Active(sessionIDs)`

Checks which sessions are still active.

### `hub.Remove(sessionID)`

Deletes Redis keys and local state for a stream.

### `stream.Publish(chunk)`

Publishes a chunk.

### `stream.Subscribe(bufExtra)`

Subscribes. Replays existing chunks, then delivers new ones live.

### `stream.SetMetadata(v)` / `stream.Metadata(&target)`

Stores / loads per-stream JSON metadata.

### `stream.Cancel()`

Sends a cancel signal via Pub/Sub.

### `stream.Close()`

Marks the stream as done.

### `stream.Done()`

Reports whether the stream has finished.

## Typical flow

1. `Register` when a request comes in
2. Only start the job if `created == true`
3. `Publish` chunks as they're generated
4. Consumers call `Get` + `Subscribe` from any instance
5. `Close` when done, `Cancel` if the user aborts

## Notes

- Don't start a second producer when `created == false`
- Call `SetMetadata` before `Close`
- Always call `unsubscribe`

## See also

- [vercel/resumable-stream](https://github.com/vercel/resumable-stream) — same idea, TypeScript, tied to the Vercel AI SDK
- [ai-resumable-stream](https://github.com/zirkelc/ai-resumable-stream) — TypeScript, also Redis-backed

## License

GPL-3.0. See [LICENSE](./LICENSE).
