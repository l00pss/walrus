package walrus

import (
	"os"
	"time"
)

type State int

const (
	// StateInitializing indicates that the WAL is initializing.
	Initializing State = iota
	// StateReady indicates that the WAL is ready for operations.
	Ready
	// StateClosed indicates that the WAL has been closed.
	Closed
)

type Status int

const (
	// OK indicates that the operation was successful.
	OK Status = iota
	// Corrupted indicates that the WAL is corrupted.
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
	// DefaultFilePermission is the default file permission for WAL files.
	DefaultFilePermission Permission = 0644
	// DirectoryPermission is the permission for WAL directories.
	DirectoryPermission = 0750
	// Segment
	FilePermission = 0640
)
