package walrus

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/l00pss/helpme/result"
	"github.com/l00pss/littlecache"
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
	FirstIndex  uint64
	LastIndex   uint64
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

type WAL struct {
	mu             sync.RWMutex
	state 		   State
	status 	       Status
	cusror         Cursor
	dir            string
	tailSFH        os.File
	segments       []*Segment
	currentSegment *Segment
	cache          *littlecache.LittleCache
	config         Config
}

func NewWAL(dir string, config Config) result.Result[*WAL] {
	configResult := config.Validate()
	if configResult.IsErr() {
		return result.Err[*WAL](configResult.UnwrapErr())
	}

	dir, err := filepath.Abs(dir)
	if err != nil {
		return result.Err[*WAL](err)
	}

	cacheConfig := littlecache.DefaultConfig()
	cache, err := littlecache.NewLittleCache(cacheConfig)

	if err != nil {
		return result.Err[*WAL](err)
	}

	w := &WAL{
		mu:     sync.RWMutex{},
		state:  Initializing,
		status: OK,
		cusror: StartCursor(),
		dir:    dir,
		config: configResult.Unwrap(),
		cache:  &cache,
	}

	return result.Ok(w)
}
