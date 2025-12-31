package walrus

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/l00pss/helpme/result"
	"github.com/l00pss/littlecache"
)

type WAL struct {
	config         Config
	mu             sync.RWMutex
	encoder        Encoder
	state          State
	status         Status
	cusror         Cursor
	dir            string
	tailSFH        os.File
	segments       []*Segment
	currentSegment *Segment
	cache          *littlecache.LittleCache
	batch          Batch
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
	cache.Resize(config.cachedSegments * 1024)
	MkDirIfNotExist(dir)

	var encoder Encoder
	switch config.format {
	case BINARY:
		encoder = BinaryEncoder{}
	case JSON:
		encoder = JSONEncoder{}
	}

	w := &WAL{
		mu:      sync.RWMutex{},
		encoder: encoder,
		state:   Initializing,
		status:  OK,
		cusror:  StartCursor(),
		dir:     dir,
		config:  configResult.Unwrap(),
		cache:   &cache,
	}

	return result.Ok(w)
}

func MkDirIfNotExist(dir string) result.Result[struct{}] {
	err := os.MkdirAll(dir, os.FileMode(DirectoryPermission))
	if err != nil {
		return result.Err[struct{}](err)
	}
	return result.Ok(struct{}{})
}

func (w *WAL) Append(entry Entry) result.Result[uint64] {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.state == Closed {
		return result.Err[uint64](ErrWALClosed)
	}

	if w.currentSegment == nil {
		if err := w.ensureSegment(); err != nil {
			return result.Err[uint64](err)
		}
	}

	size, err := w.currentSegment.Size()
	if err != nil {
		return result.Err[uint64](err)
	}
	if size >= w.config.segmentSize {
		if err := w.rotateSegment(); err != nil {
			return result.Err[uint64](err)
		}
	}

	entry.Index = w.cusror.LastIndex + 1

	seralizedEntryResult := w.encoder.Encode(entry)
	if seralizedEntryResult.IsErr() {
		return result.Err[uint64](seralizedEntryResult.UnwrapErr())
	}
	data := seralizedEntryResult.Unwrap()

	_, err = w.currentSegment.Write(data)
	if err != nil {
		return result.Err[uint64](err)
	}

	if w.config.syncAfterWrite {
		if err := w.currentSegment.Sync(); err != nil {
			return result.Err[uint64](err)
		}
	}

	w.cusror.LastIndex = entry.Index
	if w.cusror.FirstIndex == 0 {
		w.cusror.FirstIndex = entry.Index
	}

	return result.Ok(entry.Index)
}

func (w *WAL) ensureSegment() error {
	if w.currentSegment != nil {
		return nil
	}

	segments, err := loadSegments(w.dir)
	if err != nil {
		return err
	}

	if len(segments) > 0 {
		w.segments = segments
		w.currentSegment = segments[len(segments)-1]
		return nil
	}

	return w.rotateSegment()
}

func (w *WAL) rotateSegment() error {
	if w.currentSegment != nil {
		if err := w.currentSegment.Sync(); err != nil {
			return err
		}
	}

	var newIndex uint64 = 1
	if len(w.segments) > 0 {
		newIndex = w.segments[len(w.segments)-1].index + 1
	}

	segment, err := createSegment(w.dir, newIndex)
	if err != nil {
		return err
	}

	w.segments = append(w.segments, segment)
	w.currentSegment = segment

	if len(w.segments) > w.config.maxSegments {
		w.cleanupOldSegments()
	}

	return nil
}

func (w *WAL) cleanupOldSegments() {
	for len(w.segments) > w.config.maxSegments {
		oldest := w.segments[0]
		oldest.Close()
		os.Remove(oldest.filePath)
		w.segments = w.segments[1:]
	}
}
