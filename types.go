package walrus

import (
	"os"
	"time"
)

type Entry struct {
	Index     uint64 // Monotonically increasing index
	Term      uint64 // For consensus algorithms like Raft
	Data      []byte
	Checksum  uint32 // CRC32 or similar
	Timestamp time.Time
}

type Segment struct {
	id         uint64
	startIndex uint64
	endIndex   uint64
	size       int64
	filePath   string
	file       *os.File
}
