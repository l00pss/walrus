# Walrus - Write-Ahead Log (WAL) Implementation

<br>
<div align="center">
  <img src="logo.png" alt="Walrus Logo" width="800"/>
  <br><br>
  <a href="https://golang.org/"><img src="https://img.shields.io/badge/go-1.21+-00ADD8?style=flat&logo=go&logoColor=white" alt="Go Version"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue?style=flat" alt="License"></a>
  <a href="https://goreportcard.com/report/github.com/l00pss/walrus"><img src="https://goreportcard.com/badge/github.com/l00pss/walrus" alt="Go Report Card"></a>
  <a href="https://github.com/l00pss/walrus/stargazers"><img src="https://img.shields.io/github/stars/l00pss/walrus?style=flat&logo=github" alt="GitHub Stars"></a>
</div>

<br>
<div align="center">
  <a href="https://www.buymeacoffee.com/l00pss" target="_blank"><img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" alt="Buy Me A Coffee" style="height: 60px !important;width: 217px !important;" ></a>
</div>

Walrus is a high-performance Write-Ahead Log (WAL) implementation in Go with ACID transactions, zero-copy reads, and segment-based architecture.

## Features

- **ACID Transactions** - Atomic commits with timeout and rollback
- **Zero-Copy Reads** - Memory-mapped files for high performance
- **Segment-Based** - Automatic rotation and cleanup
- **Thread-Safe** - Safe for concurrent access
- **Batch Operations** - High throughput batch writing
- **Context Support** - Timeout and cancellation support
- **Custom Logging** - Pluggable logger interface

## Installation

```bash
go get github.com/l00pss/walrus
```

## Quick Start

```go
config := walrus.DefaultConfig()
wal := walrus.NewWAL("./wal_data", config).Unwrap()
defer wal.Close()

entry := walrus.Entry{
    Data:      []byte("Hello World!"),
    Term:      1,
    Timestamp: time.Now(),
}

index := wal.Append(entry).Unwrap()
readEntry := wal.Get(index).Unwrap()
```

## Transactions

```go
txID := wal.BeginTransaction(30 * time.Second).Unwrap()

wal.AddToTransaction(txID, entry1)
wal.AddToTransaction(txID, entry2)

indices := wal.CommitTransaction(txID).Unwrap()
// Or rollback: wal.RollbackTransaction(txID)
```

## Batch Operations

```go
entries := []walrus.Entry{
    {Data: []byte("Entry 1"), Term: 1, Timestamp: time.Now()},
    {Data: []byte("Entry 2"), Term: 1, Timestamp: time.Now()},
}

indices := wal.WriteBatch(entries).Unwrap()
```

## Context Support

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

index := wal.AppendWithContext(ctx, entry).Unwrap()
```

## Configuration

```go
config := walrus.Config{
    SegmentSize:     64 * 1024 * 1024,  // 64MB per segment
    MaxSegments:     100,
    SyncAfterWrite:  true,
    BufferSize:      4096,
    ZeroCopy:        true,
    Format:          walrus.BINARY,
}
```

## Benchmarks

Tested on Apple M1 Pro:

| Operation | Throughput | Latency | Allocations |
|-----------|------------|---------|-------------|
| Append | 290,697 ops/sec | 3.44 µs/op | 14 allocs/op |
| Get | 741,289 ops/sec | 1.35 µs/op | 9 allocs/op |
| Get (Zero-Copy) | 813,008 ops/sec | 1.23 µs/op | 0 allocs/op |
| Get (Regular) | 709,220 ops/sec | 1.41 µs/op | 7 allocs/op |

Zero-copy mode provides **~15% faster reads** with **zero memory allocations**.

```bash
go test -bench=. -benchmem
```

## License

MIT
