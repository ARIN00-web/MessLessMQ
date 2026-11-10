# MessLessMQ — Phase 9 complete implementation overlay

This implementation covers exactly the seven locked Phase 9 subchapters:

- 9.1 Consumer Model
- 9.2 Fetching From Logs
- 9.3 Consumer Response
- 9.4 Consumer Position
- 9.5 Replay
- 9.6 Consumer Errors
- 9.7 Consume Testing

The ZIP contains all new files and complete replacement files. Extract it at the MessLessMQ repository root. Two small API additions are required in existing files and are included below. Do not replace those existing files wholesale.

## 1. Add to `internal/storage/log.go`

Place these methods near the existing `NextOffset()` method. Do not replace `log.go`.

```go
// StartOffset returns the earliest offset still present in the log.
// Retention may advance this value while NextOffset remains monotonic.
func (l *Log) StartOffset() (uint64, error) {
    l.mu.RLock()
    defer l.mu.RUnlock()

    if l.closed {
        return 0, os.ErrClosed
    }
    if len(l.segments) == 0 {
        return l.nextOffset, nil
    }
    return l.segments[0].baseOffset, nil
}

// OffsetRange returns the earliest retained offset and the next offset after
// the latest record under one read lock. This gives consumers a consistent
// range snapshot when appends or retention may be happening concurrently.
func (l *Log) OffsetRange() (startOffset, nextOffset uint64, err error) {
    l.mu.RLock()
    defer l.mu.RUnlock()

    if l.closed {
        return 0, 0, os.ErrClosed
    }
    if len(l.segments) == 0 {
        return l.nextOffset, l.nextOffset, nil
    }
    return l.segments[0].baseOffset, l.nextOffset, nil
}
```

The existing file already imports `os` and `sync`; no import changes are required.

## 2. Add to `internal/topic/partition_log.go`

Place these methods near the existing `NextOffset()` method. The file already imports `internal/storage`.

```go
// StartOffset returns the earliest offset still retained by this partition.
func (p *Partition) StartOffset() (uint64, error) {
    log, err := p.storageLog()
    if err != nil {
        return 0, err
    }
    return log.StartOffset()
}

// OffsetRange returns a consistent snapshot of the retained start offset
// and current log end offset.
func (p *Partition) OffsetRange() (startOffset, nextOffset uint64, err error) {
    log, err := p.storageLog()
    if err != nil {
        return 0, 0, err
    }
    return log.OffsetRange()
}

// ApplyRetention applies the storage engine's retention policy to this
// partition's log. This is useful for testing how consumers handle retention.
func (p *Partition) ApplyRetention(cfg storage.RetentionConfig) error {
    log, err := p.storageLog()
    if err != nil {
        return err
    }
    return log.ApplyRetention(cfg)
}
```

## 3. What the implementation does

### 9.1 — Consumer Model

`consumer.Consumer` holds explicit topic-partition assignments. It rejects nil and duplicate logical partitions, supports multiple topics, and stores one independent in-memory position for each assigned partition.

### 9.2 — Fetching From Logs

`Consumer.Fetch` reads up to a caller-supplied record limit from the partition's existing storage log. It returns payload, timestamp, topic, partition ID, and offset. The maximum fetch size is bounded at 500 records.

### 9.3 — Consumer Response

The protocol package now has a binary `ConsumeRequest` and `ConsumeResponse` codec. The broker's `ConsumeHandler` resolves the topic and partition from the shared registry and returns the encoded records.

### 9.4 — Consumer Position

The consumer tracks the next unread offset independently for every partition. Successful fetches advance this in-memory cursor. `SetPosition`, `ResetToEarliest`, and `ResetToLatest` validate against the currently retained offset range.

### 9.5 — Replay

`Consumer.Replay` reads again from a specified retained offset and advances the cursor after the replayed page. It cannot recover records already removed by retention.

### 9.6 — Consumer Errors

The implementation distinguishes invalid fetch limits, unassigned partitions, offsets before the retained start, offsets beyond the log end, unknown topics, unknown partitions, malformed payloads, and unsupported API versions. The network layer maps these to protocol response error codes.

### 9.7 — Consume Testing

Tests cover cursor advancement, independent per-partition positions, retention boundaries, replay, malformed codecs, handler integration, TCP request/response correlation, full repository tests, and race detection.

## 4. Important semantics and limitations

- Consumer positions are in-memory read cursors, **not durable commits**. Durable offset commits are Phase 11.
- Fetch advances the local read cursor after successfully reading a response. It does not mean application processing has been acknowledged; delivery semantics belong to Phase 12.
- Each network consume request carries an explicit offset. The consume handler creates a request-scoped reader; group membership and persistent consumer identity are later phases.
- The current storage record format stores payload and timestamp. It does not expose the producer key as a separate consumed field. This implementation does not invent one.
- The dispatcher accepts an optional shared `*topic.Registry`. To consume registered topics, pass the same registry where the topics were created and their partition logs opened.
- `NewDispatcher()` with no argument remains compatible but creates an empty registry. The current broker executable has no topic-admin/bootstrap API, so a running broker must be wired to a populated registry by its application bootstrap before network consume requests can read a topic.
- The current `ProduceHandler` remains a placeholder. Phase 9 implements consuming records already present in partition logs; it does not claim network PRODUCE is complete.

## 5. Files included in the overlay

**New files:**
- `internal/protocol/consume.go`
- `internal/protocol/consume_codec_test.go`
- `internal/storage/start_offset_test.go`
- `internal/broker/consume_handler_test.go`

**Complete replacements:**
- `internal/consumer/consumer.go`
- `internal/consumer/consumer_test.go`
- `internal/protocol/errors.go`
- `internal/protocol/response.go`
- `internal/broker/consume_handler.go`
- `internal/broker/dispatcher.go`
- `internal/broker/dispatcher_test.go`
- `internal/broker/handler_test.go`
- `internal/network/connection.go`
- `internal/network/connection_test.go`
- `internal/network/integration_test.go`

## 6. Validation

After extracting and making the two API additions, run from the repository root:

```bash
gofmt -w internal/consumer internal/protocol/consume.go internal/protocol/consume_codec_test.go internal/storage/start_offset_test.go internal/broker/consume_handler.go internal/broker/consume_handler_test.go internal/broker/dispatcher.go internal/broker/dispatcher_test.go internal/broker/handler_test.go internal/network/connection.go internal/network/connection_test.go internal/network/integration_test.go
```

Then run separately:

```bash
go test -count=1 ./...
go test -race -count=1 ./...
go test -cover ./internal/storage ./internal/consumer ./internal/protocol ./internal/broker ./internal/network
go vet ./...
```

Do not mark Phase 9 complete until these commands pass on your local repository.
