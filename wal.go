package walrus

import (
	"io"
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

	// Get current offset before writing
	currentSize, _ := w.currentSegment.Size()

	_, err = w.currentSegment.Write(data)
	if err != nil {
		return result.Err[uint64](err)
	}

	// Track entry offset for index-based lookup
	w.currentSegment.TrackEntry(entry.Index, currentSize)

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

func (w *WAL) Get(index uint64) result.Result[Entry] {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.state == Closed {
		return result.Err[Entry](ErrWALClosed)
	}

	if index < w.cusror.FirstIndex || index > w.cusror.LastIndex {
		return result.Err[Entry](ErrIndexOutOfRange)
	}

	segment := w.findSegmentByIndex(index)
	if segment == nil {
		return result.Err[Entry](ErrSegmentNotFound)
	}

	data, err := segment.GetEntryByIndex(index)
	if err != nil {
		return result.Err[Entry](err)
	}

	entryResult := w.encoder.Decode(data)
	if entryResult.IsErr() {
		return result.Err[Entry](entryResult.UnwrapErr())
	}

	return result.Ok(entryResult.Unwrap())
}

func (w *WAL) GetRange(start, end uint64) result.Result[[]Entry] {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.state == Closed {
		return result.Err[[]Entry](ErrWALClosed)
	}

	if start > end {
		return result.Err[[]Entry](ErrIndexOutOfRange)
	}

	if start < w.cusror.FirstIndex {
		start = w.cusror.FirstIndex
	}
	if end > w.cusror.LastIndex {
		end = w.cusror.LastIndex
	}

	entries := make([]Entry, 0, end-start+1)

	for i := start; i <= end; i++ {
		segment := w.findSegmentByIndex(i)
		if segment == nil {
			continue
		}

		data, err := segment.GetEntryByIndex(i)
		if err != nil {
			return result.Err[[]Entry](err)
		}

		entryResult := w.encoder.Decode(data)
		if entryResult.IsErr() {
			return result.Err[[]Entry](entryResult.UnwrapErr())
		}

		entries = append(entries, entryResult.Unwrap())
	}

	return result.Ok(entries)
}

func (w *WAL) findSegmentByIndex(index uint64) *Segment {
	for _, seg := range w.segments {
		if seg.ContainsIndex(index) {
			return seg
		}
	}
	return nil
}

func (w *WAL) GetFirstIndex() uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.cusror.FirstIndex
}

func (w *WAL) GetLastIndex() uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.cusror.LastIndex
}

func Open(dir string, config Config) result.Result[*WAL] {
	walResult := NewWAL(dir, config)
	if walResult.IsErr() {
		return walResult
	}

	w := walResult.Unwrap()
	if err := w.recover(); err != nil {
		return result.Err[*WAL](err)
	}

	w.state = Ready
	return result.Ok(w)
}

func (w *WAL) recover() error {
	segments, err := loadSegments(w.dir)
	if err != nil {
		return err
	}

	if len(segments) == 0 {
		return nil
	}

	w.segments = segments
	w.currentSegment = segments[len(segments)-1]

	for _, seg := range segments {
		if err := w.recoverSegment(seg); err != nil {
			w.status = Corrupted
			return err
		}
	}

	return nil
}

func (w *WAL) recoverSegment(seg *Segment) error {
	var offset int64 = 0

	for {
		data, nextOffset, err := seg.ReadAt(offset)
		if err == io.EOF {
			break
		}
		if err != nil {
			return w.handleCorruptedEntry(seg, offset)
		}

		entryResult := w.encoder.Decode(data)
		if entryResult.IsErr() {
			return w.handleCorruptedEntry(seg, offset)
		}

		entry := entryResult.Unwrap()
		seg.TrackEntry(entry.Index, offset)

		if w.cusror.FirstIndex == 0 {
			w.cusror.FirstIndex = entry.Index
		}
		w.cusror.LastIndex = entry.Index

		offset = nextOffset
	}

	return nil
}

func (w *WAL) handleCorruptedEntry(seg *Segment, offset int64) error {
	if err := seg.file.Truncate(offset); err != nil {
		return err
	}
	return nil
}

func (w *WAL) Truncate(index uint64) result.Result[struct{}] {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.state == Closed {
		return result.Err[struct{}](ErrWALClosed)
	}

	if index < w.cusror.FirstIndex || index > w.cusror.LastIndex {
		return result.Err[struct{}](ErrIndexOutOfRange)
	}

	for i := len(w.segments) - 1; i >= 0; i-- {
		seg := w.segments[i]

		if seg.firstEntry > index {
			seg.Close()
			os.Remove(seg.filePath)
			w.segments = w.segments[:i]
			continue
		}

		if seg.ContainsIndex(index) {
			localIdx := index - seg.firstEntry
			if localIdx < uint64(len(seg.entryOffsets)-1) {
				nextOffset := seg.entryOffsets[localIdx+1]
				seg.file.Truncate(nextOffset)
				seg.lastEntry = index
				seg.entryOffsets = seg.entryOffsets[:localIdx+1]
			}
			break
		}
	}

	w.cusror.LastIndex = index
	if len(w.segments) > 0 {
		w.currentSegment = w.segments[len(w.segments)-1]
	}

	return result.Ok(struct{}{})
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.state == Closed {
		return nil
	}

	for _, seg := range w.segments {
		seg.Sync()
		seg.Close()
	}

	w.segments = nil
	w.currentSegment = nil
	w.state = Closed

	return nil
}
