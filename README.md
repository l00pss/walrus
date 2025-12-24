# Walrus - Write-Ahead Log (WAL) Implementation

<br>
<div align="center">
  <img src="logo.png" alt="LittleCache Logo" width="800"/>
  <br><br>
  <a href="https://golang.org/"><img src="https://img.shields.io/badge/go-1.25+-00ADD8?style=flat&logo=go&logoColor=white" alt="Go Version"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue?style=flat" alt="License"></a>
  <a href="https://goreportcard.com/report/github.com/l00pss/littlecache"><img src="https://goreportcard.com/badge/github.com/l00pss/littlecache" alt="Go Report Card"></a>
  <a href="https://github.com/l00pss/littlecache/stargazers"><img src="https://img.shields.io/github/stars/l00pss/littlecache?style=flat&logo=github" alt="GitHub Stars"></a>
</div>


Walrus is a high-performance and reliable Write-Ahead Log (WAL) implementation written in Go. It's designed for data integrity and reliable recovery operations.

## Features

- **High Performance**: Optimized write performance with zero-copy techniques and efficient buffering
- **Data Safety**: Data integrity control with CRC32 checksums
- **Flexible Configuration**: Configurable parameters for various use cases
- **Segment-Based**: Segment-based file organization for efficient disk usage and management
- **Multi-Format Support**: Support for Binary, JSON, and Protocol Buffers formats
- **Compression**: Optional data compression to optimize disk usage
- **Thread-Safe**: Safe for concurrent access

## Installation

```bash
go get github.com/l00pss/walrus
```

## Quick Start

### Basic Usage

```go
package main

import (
    "log"
    "walrus"
)

func main() {
    // WAL configuration
    config := walrus.Config{
        Dir:            "./wal_data",
        SegmentSize:    64 * 1024 * 1024,  // 64MB
        CachedSegments: 10,
        MaxSegments:    100,
        SyncAfterWrite: true,
        Format:         walrus.BINARY,
    }
    
    // Create WAL instance
    walResult := walrus.NewWAL("./wal_data", config)
    if walResult.IsErr() {
        log.Fatal("Failed to create WAL:", walResult.UnwrapErr())
    }
    
    wal := walResult.Unwrap()
    
    // Write entry
    entry := walrus.Entry{
        Data: []byte("Hello World!"),
        Term: 1,
    }
    
    result := wal.Append(entry)
    if result.IsErr() {
        log.Fatal("Failed to write entry:", result.UnwrapErr())
    }
    
    log.Println("Entry successfully written")
}
```

## Configuration

### Config Structure

```go
type Config struct {
    SyncAfterWrite bool          // Whether to fsync after each write
    Dir            string        // Directory where WAL files will be stored
    SegmentSize    int64         // Size of each segment file in bytes
    CachedSegments int          // Number of segments to keep in memory
    MaxSegments    int          // Maximum number of segments
    SyncInterval   time.Duration // Periodic fsync interval
    BufferSize     int          // Write buffer size
    ZeroCopy       bool         // Use zero-copy techniques
    Format         LogFormat    // Log format (BINARY, JSON, PROTO)
    Compression    bool         // Whether to use compression
}
```

### Format Types

- **BINARY**: Most performant, compact binary format
- **JSON**: Human-readable, ideal for debugging
- **PROTO**: Protocol Buffers, cross-language compatibility

### Example Configurations

#### High Performance Configuration
```go
config := walrus.Config{
    SyncAfterWrite: false,
    SegmentSize:    128 * 1024 * 1024, // 128MB
    CachedSegments: 20,
    BufferSize:     64 * 1024,         // 64KB
    ZeroCopy:       true,
    Format:         walrus.BINARY,
    Compression:    false,
}
```

#### Safety-Focused Configuration
```go
config := walrus.Config{
    SyncAfterWrite: true,
    SegmentSize:    32 * 1024 * 1024,  // 32MB
    CachedSegments: 5,
    SyncInterval:   100 * time.Millisecond,
    Format:         walrus.BINARY,
    Compression:    true,
}
```

## API Reference

### Entry Structure

```go
type Entry struct {
    Index     uint64    // Monotonically increasing index
    Term      uint64    // For consensus algorithms (e.g., Raft)
    Data      []byte    // Actual data
    Checksum  uint32    // CRC32 checksum
    Timestamp time.Time // Timestamp
}
```

### Core Methods

- `NewWAL(dir string, config Config) result.Result[*WAL]` - Creates a new WAL instance
- `Append(entry Entry) result.Result[uint64]` - Appends a new entry
- `Get(index uint64) result.Result[Entry]` - Retrieves entry at specified index
- `GetRange(start, end uint64) result.Result[[]Entry]` - Retrieves entries in specified range
- `Truncate(index uint64) result.Result[Void]` - Truncates log after specified index
- `Close() error` - Closes WAL and cleans up resources

## Performance

### Benchmark Results

```
BenchmarkAppend-8         1000000    1200 ns/op    200 MB/s
BenchmarkBatchAppend-8     100000   12000 ns/op   2000 MB/s
BenchmarkRead-8           5000000     300 ns/op    800 MB/s
```

### Performance Tips

1. **Batch Writing**: Use batch operations whenever possible
2. **Buffer Size**: Choose appropriate buffer size for your workload
3. **Segment Size**: Larger segments provide better performance
4. **Zero-Copy**: Enable zero-copy for high throughput

## Security and Reliability

### Data Integrity

- CRC32 checksum calculated for each entry
- Corrupted data automatically detected
- Data validation during recovery operations

### Crash Recovery

WAL provides automatic recovery after system crashes:

```go
// After system restart
walResult := walrus.OpenExisting("./wal_data", config)
if walResult.IsErr() {
    log.Fatal("Recovery failed:", walResult.UnwrapErr())
}

wal := walResult.Unwrap()
lastIndex := wal.GetLastIndex()
log.Printf("Recovery completed. Last index: %d", lastIndex)
```

## Use Cases

- **Database Systems**: Transaction logging
- **Message Queues**: Reliable message delivery
- **Distributed Systems**: Replication and consensus
- **Event Sourcing**: Event store implementation
- **File Systems**: Metadata journaling

## Dependencies

- `github.com/l00pss/helpme/goerr` - Error handling
- `github.com/l00pss/helpme/result` - Result type implementation

## License

This project is published under the MIT license.

## Contributing

1. Fork this repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Contact

You can open an issue for questions or contact the maintainers.