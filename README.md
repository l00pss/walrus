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


Walrus is a high-performance and reliable Write-Ahead Log (WAL) implementation written in Go. It's designed for data integrity and reliable recovery operations.

## Features

- **Data Safety**: CRC32 checksum for each entry
- **Segment-Based**: Automatic segment rotation and cleanup  
- **Multi-Format**: Binary and JSON encoding support
- **Crash Recovery**: Automatic recovery after system crashes
- **Thread-Safe**: Safe for concurrent access
- **Transactions**: ACID transaction support with timeout and rollback capabilities
- **Batch Operations**: Efficient batch writing for high throughput
- **Write Buffering**: Configurable I/O buffering for improved write performance
- **Zero Copy**: Memory-mapped files for high-performance read operations

## Installation

```bash
go get github.com/l00pss/walrus
```

## Quick Start

```go
package main

import (
    "log"
    "time"
    "github.com/l00pss/walrus"
)

func main() {
    config := walrus.DefaultConfig()
    
    wal := walrus.NewWAL("./wal_data", config).Unwrap()
    defer wal.Close()
    
    entry := walrus.Entry{
        Data:      []byte("Hello World!"),
        Term:      1,
        Timestamp: time.Now(),
    }
    
    index := wal.Append(entry).Unwrap()
    log.Printf("Entry written at index: %d", index)
    
    readEntry := wal.Get(index).Unwrap()
    log.Printf("Read data: %s", readEntry.Data)
}
```

## Transaction Support

Walrus now supports ACID transactions, allowing you to group multiple entries and commit them atomically:

```go
// Begin a transaction with timeout
txID := wal.BeginTransaction(30 * time.Second).Unwrap()

// Add entries to transaction
entry1 := walrus.Entry{
    Data:      []byte("Entry 1"),
    Term:      1,
    Timestamp: time.Now(),
}

entry2 := walrus.Entry{
    Data:      []byte("Entry 2"), 
    Term:      1,
    Timestamp: time.Now(),
}

wal.AddToTransaction(txID, entry1)
wal.AddToTransaction(txID, entry2)

// Commit all entries atomically
indices := wal.CommitTransaction(txID).Unwrap()
log.Printf("Transaction committed with indices: %v", indices)

// Or rollback if needed
// wal.RollbackTransaction(txID)
```

### Transaction Features

- **Atomic Commits**: All entries in a transaction are written together or not at all
- **Timeout Management**: Transactions automatically expire after specified timeout
- **State Tracking**: Monitor transaction state (Pending, Committed, Aborted)
- **Resource Limits**: Configurable maximum entries per transaction
- **Automatic Cleanup**: Expired transactions are automatically cleaned up

### Batch Operations

For high-throughput scenarios, use batch operations:

```go
entries := []walrus.Entry{
    {Data: []byte("Batch Entry 1"), Term: 1, Timestamp: time.Now()},
    {Data: []byte("Batch Entry 2"), Term: 1, Timestamp: time.Now()},
    {Data: []byte("Batch Entry 3"), Term: 1, Timestamp: time.Now()},
}

indices := wal.WriteBatch(entries).Unwrap()
log.Printf("Batch written with indices: %v", indices)
```

## Segmentation Architecture

Walrus uses a sophisticated segmented log architecture that divides the write-ahead log into multiple segments, each with a bounded size. This design provides significant advantages over traditional monolithic log approaches.

### Segment Features

- **Automatic Rotation**: When a segment reaches its maximum size, a new segment is automatically created
- **Bounded Memory Usage**: Each segment has a predictable memory footprint, preventing memory exhaustion
- **Parallel Processing**: Different segments can be processed concurrently for better performance
- **Efficient Cleanup**: Old segments can be deleted independently without affecting active segments
- **Fast Recovery**: Smaller segments enable faster crash recovery and reduced startup times

### Segment File Structure

Segments are stored as individual files with the naming convention:
```
segment_00000000000000000001.wal
segment_00000000000000000002.wal
segment_00000000000000000003.wal
```

Each segment maintains:
- **Entry Offsets**: Fast random access to any entry within the segment
- **Metadata**: First and last entry indices for efficient range queries  
- **Size Tracking**: Current segment size for rotation decisions

### Segment Operations

```go
// Access segment information
stats := wal.Stats()
log.Printf("Active segments: %d", stats.SegmentCount)
log.Printf("Current segment size: %d bytes", stats.CurrentSegmentSize)

// Manual segment rotation (optional)
err := wal.RotateSegment()
if err != nil {
    log.Printf("Segment rotation failed: %v", err)
}

// Read from specific segment
entries, err := wal.ReadSegment(segmentIndex)
if err != nil {
    log.Printf("Failed to read segment: %v", err)
}
```

### Performance Benefits

The segmented architecture provides:
- **Improved Write Performance**: Sequential writes within segments maximize disk throughput
- **Reduced Fragmentation**: Smaller files reduce file system fragmentation
- **Better Cache Locality**: Operating system caches work more effectively with smaller files
- **Scalable Storage**: No single-file size limitations, enabling virtually unlimited log growth
- **Write Buffering**: Reduces system calls and improves throughput for small writes
- **Zero Copy Reads**: Memory-mapped files eliminate data copying for faster access

## Configuration

```go
config := walrus.Config{
    SegmentSize:     1024 * 1024,  // 1MB per segment
    MaxSegments:     100,           // Keep max 100 segments
    CachedSegments:  10,            // Cache 10 segments in memory
    SyncAfterWrite:  true,          // Sync after each write
    BufferSize:      4096,          // I/O buffer size
    ZeroCopy:        true,          // Enable zero-copy reads
    Format:          walrus.BINARY, // Binary or JSON encoding
}
```

## Installation

```bash
go get github.com/l00pss/walrus
```

## Repository

- **GitHub**: [https://github.com/l00pss/walrus](https://github.com/l00pss/walrus)
- **Issues**: [https://github.com/l00pss/walrus/issues](https://github.com/l00pss/walrus/issues)
- **Releases**: [https://github.com/l00pss/walrus/releases](https://github.com/l00pss/walrus/releases)

## License

MIT
