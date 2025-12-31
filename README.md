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

## API

### Entry

```go
type Entry struct {
    Index     uint64
    Term      uint64
    Data      []byte
    Checksum  uint32
    Timestamp time.Time
}
```


## License

MIT
