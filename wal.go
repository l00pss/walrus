package walrus

import (
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

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
	transactions   map[TransactionID]*Transaction
	transactionsMu sync.RWMutex
	writeBuffer    []byte
	bufferPos      int
	bufferMu       sync.Mutex
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
		mu:             sync.RWMutex{},
		encoder:        encoder,
		state:          Initializing,
		status:         OK,
		cusror:         StartCursor(),
		dir:            dir,
		config:         configResult.Unwrap(),
		cache:          &cache,
		transactions:   make(map[TransactionID]*Transaction),
		transactionsMu: sync.RWMutex{},
		writeBuffer:    make([]byte, config.bufferSize),
		bufferPos:      0,
		bufferMu:       sync.Mutex{},
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

	currentSize, _ := w.currentSegment.Size()
	_, err = w.currentSegment.Write(data)
	if err != nil {
		return result.Err[uint64](err)
	}

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

func (w *WAL) WriteBatch(entries []Entry) result.Result[[]uint64] {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.state == Closed {
		return result.Err[[]uint64](ErrWALClosed)
	}

	if len(entries) == 0 {
		return result.Ok([]uint64{})
	}

	if w.currentSegment == nil {
		if err := w.ensureSegment(); err != nil {
			return result.Err[[]uint64](err)
		}
	}

	indices := make([]uint64, len(entries))
	totalSize := 0

	for i := range entries {
		indices[i] = w.cusror.LastIndex + uint64(i) + 1
		entries[i].Index = indices[i]
		if entries[i].Timestamp.IsZero() {
			entries[i].Timestamp = time.Now()
		}

		seralizedEntryResult := w.encoder.Encode(entries[i])
		if seralizedEntryResult.IsErr() {
			return result.Err[[]uint64](seralizedEntryResult.UnwrapErr())
		}
		totalSize += len(seralizedEntryResult.Unwrap())
	}

	currentSize, err := w.currentSegment.Size()
	if err != nil {
		return result.Err[[]uint64](err)
	}
	if currentSize+int64(totalSize) >= w.config.segmentSize {
		if err := w.rotateSegment(); err != nil {
			return result.Err[[]uint64](err)
		}
	}

	startOffset, _ := w.currentSegment.Size()
	for _, entry := range entries {
		seralizedEntryResult := w.encoder.Encode(entry)
		data := seralizedEntryResult.Unwrap()

		// Get current offset before writing (includes buffer position)
		currentOffset, _ := w.currentSegment.Size()
		_, err := w.currentSegment.Write(data)
		if err != nil {
			w.currentSegment.file.Truncate(startOffset)
			return result.Err[[]uint64](err)
		}

		w.currentSegment.TrackEntry(entry.Index, currentOffset)
	}

	if w.config.syncAfterWrite {
		if err := w.currentSegment.Sync(); err != nil {
			w.currentSegment.file.Truncate(startOffset)
			return result.Err[[]uint64](err)
		}
	}
	w.cusror.LastIndex = indices[len(indices)-1]
	if w.cusror.FirstIndex == 0 {
		w.cusror.FirstIndex = indices[0]
	}

	return result.Ok(indices)
}

func generateTransactionID() TransactionID {
	b := make([]byte, 16)
	rand.Read(b)
	return TransactionID(fmt.Sprintf("tx_%x", b))
}

func (w *WAL) BeginTransaction(timeout time.Duration) result.Result[TransactionID] {
	w.transactionsMu.Lock()
	defer w.transactionsMu.Unlock()

	if w.state == Closed {
		return result.Err[TransactionID](ErrWALClosed)
	}

	if timeout == 0 {
		timeout = w.config.transactionTimeout
	}

	txID := generateTransactionID()
	tx := &Transaction{
		ID:        txID,
		State:     TransactionPending,
		Entries:   make([]Entry, 0),
		StartTime: time.Now(),
		Timeout:   timeout,
		Batch:     Batch{},
	}

	w.transactions[txID] = tx
	return result.Ok(txID)
}

func (w *WAL) AddToTransaction(txID TransactionID, entry Entry) result.Result[struct{}] {
	w.transactionsMu.Lock()
	defer w.transactionsMu.Unlock()

	tx, exists := w.transactions[txID]
	if !exists {
		return result.Err[struct{}](ErrTransactionNotFound)
	}

	if tx.State != TransactionPending {
		return result.Err[struct{}](ErrTransactionNotPending)
	}

	if tx.IsExpired() {
		tx.State = TransactionAborted
		return result.Err[struct{}](ErrTransactionExpired)
	}

	if len(tx.Entries) >= w.config.maxTransactionEntries {
		return result.Err[struct{}](ErrTransactionTooLarge)
	}

	entry.TransactionID = txID
	tx.Entries = append(tx.Entries, entry)
	return result.Ok(struct{}{})
}

func (w *WAL) CommitTransaction(txID TransactionID) result.Result[[]uint64] {
	w.transactionsMu.Lock()
	tx, exists := w.transactions[txID]
	if !exists {
		w.transactionsMu.Unlock()
		return result.Err[[]uint64](ErrTransactionNotFound)
	}

	if tx.State != TransactionPending {
		w.transactionsMu.Unlock()
		return result.Err[[]uint64](ErrTransactionNotPending)
	}

	if tx.IsExpired() {
		tx.State = TransactionAborted
		w.transactionsMu.Unlock()
		return result.Err[[]uint64](ErrTransactionExpired)
	}

	tx.State = TransactionCommitted
	entries := make([]Entry, len(tx.Entries))
	copy(entries, tx.Entries)
	w.transactionsMu.Unlock()

	result := w.WriteBatch(entries)

	w.transactionsMu.Lock()
	delete(w.transactions, txID)
	w.transactionsMu.Unlock()

	return result
}

func (w *WAL) RollbackTransaction(txID TransactionID) result.Result[struct{}] {
	w.transactionsMu.Lock()
	defer w.transactionsMu.Unlock()

	tx, exists := w.transactions[txID]
	if !exists {
		return result.Err[struct{}](ErrTransactionNotFound)
	}

	if tx.State != TransactionPending {
		return result.Err[struct{}](ErrTransactionNotPending)
	}

	tx.State = TransactionAborted
	delete(w.transactions, txID)
	return result.Ok(struct{}{})
}

func (w *WAL) GetTransactionState(txID TransactionID) result.Result[TransactionState] {
	w.transactionsMu.RLock()
	defer w.transactionsMu.RUnlock()

	tx, exists := w.transactions[txID]
	if !exists {
		return result.Err[TransactionState](ErrTransactionNotFound)
	}

	if tx.IsExpired() && tx.State == TransactionPending {
		return result.Ok(TransactionAborted)
	}

	return result.Ok(tx.State)
}

func (w *WAL) cleanupUncommittedTransactions() {
	w.transactionsMu.Lock()
	defer w.transactionsMu.Unlock()

	for txID := range w.transactions {
		delete(w.transactions, txID)
	}
}

func (w *WAL) CleanupExpiredTransactions() int {
	w.transactionsMu.Lock()
	defer w.transactionsMu.Unlock()

	cleaned := 0
	for txID, tx := range w.transactions {
		if tx.IsExpired() && tx.State == TransactionPending {
			tx.State = TransactionAborted
			delete(w.transactions, txID)
			cleaned++
		}
	}
	return cleaned
}

func (w *WAL) GetActiveTransactionCount() int {
	w.transactionsMu.RLock()
	defer w.transactionsMu.RUnlock()
	return len(w.transactions)
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
		for _, seg := range w.segments {
			seg.wal = w
		}
		w.currentSegment = segments[len(segments)-1]
		return nil
	}

	return w.rotateSegment()
}

func (w *WAL) rotateSegment() error {
	if w.currentSegment != nil {
		if err := w.flushBuffer(); err != nil {
			return err
		}
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

	// Set WAL reference for buffered writes
	segment.wal = w

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

	w.cleanupUncommittedTransactions()

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
	for _, seg := range w.segments {
		seg.wal = w
	}
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

	// Flush any remaining buffer data before closing
	if err := w.flushBuffer(); err != nil {
		return err
	}

	w.rollbackAllPendingTransactions()

	for _, seg := range w.segments {
		seg.Sync()
		seg.Close()
	}

	w.segments = nil
	w.currentSegment = nil
	w.state = Closed

	return nil
}

func (w *WAL) rollbackAllPendingTransactions() {
	w.transactionsMu.Lock()
	defer w.transactionsMu.Unlock()

	for txID, tx := range w.transactions {
		if tx.State == TransactionPending {
			tx.State = TransactionAborted
		}
		delete(w.transactions, txID)
	}
}

func (w *WAL) StartTransactionCleanup() {
	go func() {
		ticker := time.NewTicker(w.config.transactionCleanupInterval)
		defer ticker.Stop()

		for range ticker.C {
			if w.state == Closed {
				return
			}
			w.CleanupExpiredTransactions()
		}
	}()
}

func (w *WAL) flushBuffer() error {
	w.bufferMu.Lock()
	defer w.bufferMu.Unlock()

	if w.bufferPos == 0 {
		return nil
	}

	if w.currentSegment == nil {
		if err := w.ensureSegment(); err != nil {
			return err
		}
	}

	_, err := w.currentSegment.file.Write(w.writeBuffer[:w.bufferPos])
	if err != nil {
		return err
	}

	w.bufferPos = 0

	return nil
}

func (w *WAL) bufferedWrite(data []byte) error {
	w.bufferMu.Lock()
	defer w.bufferMu.Unlock()

	if len(data) > len(w.writeBuffer) {
		if w.bufferPos > 0 {
			if err := w.flushBufferUnsafe(); err != nil {
				return err
			}
		}
		_, err := w.currentSegment.file.Write(data)
		return err
	}

	if w.bufferPos+len(data) > len(w.writeBuffer) {
		if err := w.flushBufferUnsafe(); err != nil {
			return err
		}
	}

	copy(w.writeBuffer[w.bufferPos:], data)
	w.bufferPos += len(data)

	return nil
}

func (w *WAL) flushBufferUnsafe() error {
	if w.bufferPos == 0 {
		return nil
	}

	if w.currentSegment == nil {
		if err := w.ensureSegment(); err != nil {
			return err
		}
	}

	_, err := w.currentSegment.file.Write(w.writeBuffer[:w.bufferPos])
	if err != nil {
		return err
	}

	w.bufferPos = 0
	return nil
}
