package walrus

import (
	"os"
	"time"
)

type State int

const (
	Initializing State = iota
	Ready
	Closed
)

type Status int

const (
	OK Status = iota
	Corrupted
)

type Cursor struct {
	FirstIndex uint64
	LastIndex  uint64
}

func (c *Cursor) IsValid() bool {
	return c.FirstIndex <= c.LastIndex
}

func StartCursor() Cursor {
	return Cursor{
		FirstIndex: 0,
		LastIndex:  0,
	}
}

type Batch struct {
	entries []batchEntry
	datas   []byte
}

type batchEntry struct {
	index uint64
	size  int
}

func (b *Batch) Reset() {
	b.entries = b.entries[:0]
	b.datas = b.datas[:0]
}

func (b *Batch) Len() int {
	return len(b.entries)
}

func (b *Batch) Write(index uint64, data []byte) {
	b.entries = append(b.entries, batchEntry{
		index: index,
		size:  len(data),
	})
	b.datas = append(b.datas, data...)
}

type Entry struct {
	Index     uint64 // Monotonically increasing index
	Term      uint64 // For consensus algorithms like Raft
	Data      []byte
	Checksum  uint32 // CRC32 or similar
	Timestamp time.Time
}

type Segment struct {
	path     string
	index    uint64
	filePath string
	file     *os.File
}

type Permission os.FileMode

const (
	DefaultFilePermission Permission = 0644
	DirectoryPermission   Permission = 0750
	FilePermission        Permission = 0640
)
