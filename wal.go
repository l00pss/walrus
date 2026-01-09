package walrus

import (
	"context"
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
	cursor         Cursor
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
	cleanupDone    chan struct{}
	logger         Logger
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

	cacheConfig := littlecache.Config{
		MaxSize:        config.cacheMaxItemSize,
		EvictionPolicy: config.evictionPolicy,
	}

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

	logger := config.logger
	if logger == nil {
		logger = NoOpLogger{}
	}

	w := &WAL{
		mu:             sync.RWMutex{},
		encoder:        encoder,
		state:          Initializing,
		status:         OK,
		cursor:         StartCursor(),
		dir:            dir,
		config:         configResult.Unwrap(),
		cache:          &cache,
		transactions:   make(map[TransactionID]*Transaction),
		transactionsMu: sync.RWMutex{},
		writeBuffer:    make([]byte, config.bufferSize),
		bufferPos:      0,
		bufferMu:       sync.Mutex{},
		cleanupDone:    make(chan struct{}),
		logger:         logger,
	}

	w.logger.Info("WAL initialized", "dir", dir)
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
	return w.AppendWithContext(context.Background(), entry)
}

func (w *WAL) AppendWithContext(ctx context.Context, entry Entry) result.Result[uint64] {
	select {
	case <-ctx.Done():
		w.logger.Warn("Append cancelled", "error", ctx.Err())
		return result.Err[uint64](ctx.Err())
	default:
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.state == Closed {
		w.logger.Warn("Append called on closed WAL")
		return result.Err[uint64](ErrWALClosed)
	}

	if w.currentSegment == nil {
		if err := w.ensureSegment(); err != nil {
			w.logger.Error("failed to ensure segment", "error", err)
			return result.Err[uint64](err)
		}
	}

	size, err := w.currentSegment.Size()
	if err != nil {
		w.logger.Error("failed to get segment size", "error", err)
		return result.Err[uint64](err)
	}
	if size >= w.config.segmentSize {
		w.logger.Debug("rotating segment", "size", size)
		if err := w.rotateSegment(); err != nil {
			w.logger.Error("failed to rotate segment", "error", err)
			return result.Err[uint64](err)
		}
	}

	entry.Index = w.cursor.LastIndex + 1

	seralizedEntryResult := w.encoder.Encode(entry)
	if seralizedEntryResult.IsErr() {
		w.logger.Error("failed to encode entry", "error", seralizedEntryResult.UnwrapErr())
		return result.Err[uint64](seralizedEntryResult.UnwrapErr())
	}
	data := seralizedEntryResult.Unwrap()

	currentSize, _ := w.currentSegment.Size()
	_, err = w.currentSegment.Write(data)
	if err != nil {
		w.logger.Error("failed to write entry", "error", err)
		return result.Err[uint64](err)
	}

	w.currentSegment.TrackEntry(entry.Index, currentSize)

	if w.config.syncAfterWrite {
		if err := w.currentSegment.Sync(); err != nil {
			w.logger.Error("failed to sync segment", "error", err)
			return result.Err[uint64](err)
		}
	}

	w.cursor.LastIndex = entry.Index
	if w.cursor.FirstIndex == 0 {
		w.cursor.FirstIndex = entry.Index
	}

	w.logger.Debug("entry appended", "index", entry.Index)
	return result.Ok(entry.Index)
}

func (w *WAL) WriteBatch(entries []Entry) result.Result[[]uint64] {
	return w.WriteBatchWithContext(context.Background(), entries)
}

func (w *WAL) WriteBatchWithContext(ctx context.Context, entries []Entry) result.Result[[]uint64] {
	select {
	case <-ctx.Done():
		w.logger.Warn("WriteBatch cancelled", "error", ctx.Err())
		return result.Err[[]uint64](ctx.Err())
	default:
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.state == Closed {
		w.logger.Warn("WriteBatch called on closed WAL")
		return result.Err[[]uint64](ErrWALClosed)
	}

	if len(entries) == 0 {
		return result.Ok([]uint64{})
	}

	if w.currentSegment == nil {
		if err := w.ensureSegment(); err != nil {
			w.logger.Error("failed to ensure segment", "error", err)
			return result.Err[[]uint64](err)
		}
	}

	indices := make([]uint64, len(entries))
	encodedEntries := make([][]byte, len(entries))
	totalSize := 0

	for i := range entries {
		indices[i] = w.cursor.LastIndex + uint64(i) + 1
		entries[i].Index = indices[i]
		if entries[i].Timestamp.IsZero() {
			entries[i].Timestamp = time.Now()
		}

		seralizedEntryResult := w.encoder.Encode(entries[i])
		if seralizedEntryResult.IsErr() {
			w.logger.Error("failed to encode entry", "index", entries[i].Index, "error", seralizedEntryResult.UnwrapErr())
			return result.Err[[]uint64](seralizedEntryResult.UnwrapErr())
		}
		encodedEntries[i] = seralizedEntryResult.Unwrap()
		totalSize += len(encodedEntries[i])
	}

	currentSize, err := w.currentSegment.Size()
	if err != nil {
		w.logger.Error("failed to get segment size", "error", err)
		return result.Err[[]uint64](err)
	}
	if currentSize+int64(totalSize) >= w.config.segmentSize {
		w.logger.Debug("rotating segment", "currentSize", currentSize, "totalSize", totalSize)
		if err := w.rotateSegment(); err != nil {
			w.logger.Error("failed to rotate segment", "error", err)
			return result.Err[[]uint64](err)
		}
	}

	startOffset, _ := w.currentSegment.Size()
	currentOffset := startOffset
	for i, entry := range entries {
		data := encodedEntries[i]

		w.currentSegment.TrackEntry(entry.Index, currentOffset)

		_, err := w.currentSegment.Write(data)
		if err != nil {
			w.logger.Error("failed to write entry", "index", entry.Index, "error", err)
			w.currentSegment.file.Truncate(startOffset)
			return result.Err[[]uint64](err)
		}

		currentOffset += int64(EntryLengthSize + len(data))
	}

	if w.config.syncAfterWrite {
		if err := w.currentSegment.Sync(); err != nil {
			w.logger.Error("failed to sync segment", "error", err)
			w.currentSegment.file.Truncate(startOffset)
			return result.Err[[]uint64](err)
		}
	}
	w.cursor.LastIndex = indices[len(indices)-1]
	if w.cursor.FirstIndex == 0 {
		w.cursor.FirstIndex = indices[0]
	}

	w.logger.Debug("batch written", "count", len(entries), "firstIndex", indices[0], "lastIndex", indices[len(indices)-1])
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
		delete(w.transactions, txID)
		w.transactionsMu.Unlock()
		return result.Err[[]uint64](ErrTransactionExpired)
	}

	tx.State = TransactionCommitted
	entries := make([]Entry, len(tx.Entries))
	copy(entries, tx.Entries)
	delete(w.transactions, txID)
	w.transactionsMu.Unlock()

	return w.WriteBatch(entries)
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
			if w.config.zeroCopy {
				if err := seg.enableZeroCopy(); err != nil {
					continue
				}
			}
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

	segment.wal = w

	if w.config.zeroCopy {
		if err := segment.enableZeroCopy(); err != nil {
		}
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
	if w.config.zeroCopy {
		return w.GetZeroCopy(index)
	}

	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.state == Closed {
		return result.Err[Entry](ErrWALClosed)
	}

	if index < w.cursor.FirstIndex || index > w.cursor.LastIndex {
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

func (w *WAL) GetZeroCopy(index uint64) result.Result[Entry] {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.state == Closed {
		return result.Err[Entry](ErrWALClosed)
	}

	if index < w.cursor.FirstIndex || index > w.cursor.LastIndex {
		return result.Err[Entry](ErrIndexOutOfRange)
	}

	segment := w.findSegmentByIndex(index)
	if segment == nil {
		return result.Err[Entry](ErrSegmentNotFound)
	}

	if w.config.zeroCopy && segment.zeroCopyMode {
		data, err := segment.GetEntryByIndexZeroCopy(index)
		if err != nil {
			return result.Err[Entry](err)
		}

		if zeroCopyEncoder, ok := w.encoder.(ZeroCopyEncoder); ok {
			entryResult := zeroCopyEncoder.DecodeZeroCopy(data)
			if entryResult.IsErr() {
				return result.Err[Entry](entryResult.UnwrapErr())
			}
			return result.Ok(entryResult.Unwrap())
		}
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

	if start < w.cursor.FirstIndex {
		start = w.cursor.FirstIndex
	}
	if end > w.cursor.LastIndex {
		end = w.cursor.LastIndex
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

func (w *WAL) GetRangeZeroCopy(start, end uint64) result.Result[[]Entry] {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.state == Closed {
		return result.Err[[]Entry](ErrWALClosed)
	}

	if start > end {
		return result.Err[[]Entry](ErrIndexOutOfRange)
	}

	if start < w.cursor.FirstIndex {
		start = w.cursor.FirstIndex
	}
	if end > w.cursor.LastIndex {
		end = w.cursor.LastIndex
	}

	entries := make([]Entry, 0, end-start+1)
	zeroCopyEncoder, hasZeroCopy := w.encoder.(ZeroCopyEncoder)

	for i := start; i <= end; i++ {
		segment := w.findSegmentByIndex(i)
		if segment == nil {
			continue
		}

		if w.config.zeroCopy && segment.zeroCopyMode && hasZeroCopy {
			data, err := segment.GetEntryByIndexZeroCopy(i)
			if err != nil {
				return result.Err[[]Entry](err)
			}

			entryResult := zeroCopyEncoder.DecodeZeroCopy(data)
			if entryResult.IsErr() {
				return result.Err[[]Entry](entryResult.UnwrapErr())
			}

			entries = append(entries, entryResult.Unwrap())
		} else {
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
	return w.cursor.FirstIndex
}

func (w *WAL) GetLastIndex() uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.cursor.LastIndex
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
		if w.config.zeroCopy {
			if err := seg.enableZeroCopy(); err != nil {
				continue
			}
		}
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

		if w.cursor.FirstIndex == 0 {
			w.cursor.FirstIndex = entry.Index
		}
		w.cursor.LastIndex = entry.Index

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

	if index < w.cursor.FirstIndex || index > w.cursor.LastIndex {
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

	w.cursor.LastIndex = index
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

	w.logger.Info("closing WAL", "dir", w.dir)

	// Signal cleanup goroutine to stop
	if w.cleanupDone != nil {
		close(w.cleanupDone)
	}

	// Flush any remaining buffer data before closing
	if err := w.flushBuffer(); err != nil {
		w.logger.Error("failed to flush buffer on close", "error", err)
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

	w.logger.Info("WAL closed successfully")
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

		for {
			select {
			case <-ticker.C:
				w.mu.RLock()
				state := w.state
				w.mu.RUnlock()
				if state == Closed {
					return
				}
				w.CleanupExpiredTransactions()
			case <-w.cleanupDone:
				return
			}
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
		return nil
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
		return nil
	}

	_, err := w.currentSegment.file.Write(w.writeBuffer[:w.bufferPos])
	if err != nil {
		return err
	}

	w.bufferPos = 0
	return nil
}
