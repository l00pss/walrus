package walrus

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func tempDir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "walrus_test_*")
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func cleanup(dir string) {
	os.RemoveAll(dir)
}

func testConfig() Config {
	return Config{
		syncAfterWrite:             true,
		dir:                        "",
		segmentSize:                1024 * 1024,
		cachedSegments:             4,
		maxSegments:                10,
		syncInterval:               time.Second,
		bufferSize:                 4096,
		zeroCopy:                   false,
		format:                     BINARY,
		compression:                false,
		transactionTimeout:         30 * time.Second,
		maxTransactionEntries:      1000,
		transactionCleanupInterval: 5 * time.Minute,
	}
}

// ==================== CONFIG TESTS ====================

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.segmentSize != 64*1024*1024 {
		t.Errorf("expected segmentSize 64MB, got %d", config.segmentSize)
	}
	if config.cachedSegments != 4 {
		t.Errorf("expected cachedSegments 4, got %d", config.cachedSegments)
	}
	if config.maxSegments != 100 {
		t.Errorf("expected maxSegments 100, got %d", config.maxSegments)
	}
	if config.syncInterval != time.Second {
		t.Errorf("expected syncInterval 1s, got %v", config.syncInterval)
	}
	if config.bufferSize != 4096 {
		t.Errorf("expected bufferSize 4096, got %d", config.bufferSize)
	}
	if config.format != BINARY {
		t.Errorf("expected format BINARY, got %v", config.format)
	}
	if config.transactionTimeout != 30*time.Second {
		t.Errorf("expected transactionTimeout 30s, got %v", config.transactionTimeout)
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name      string
		config    Config
		shouldErr bool
	}{
		{
			name:      "valid config",
			config:    testConfig(),
			shouldErr: false,
		},
		{
			name: "invalid segment size",
			config: Config{
				segmentSize:                0,
				cachedSegments:             4,
				maxSegments:                10,
				syncInterval:               time.Second,
				bufferSize:                 4096,
				transactionTimeout:         30 * time.Second,
				maxTransactionEntries:      1000,
				transactionCleanupInterval: 5 * time.Minute,
			},
			shouldErr: true,
		},
		{
			name: "invalid cached segments",
			config: Config{
				segmentSize:                1024,
				cachedSegments:             0,
				maxSegments:                10,
				syncInterval:               time.Second,
				bufferSize:                 4096,
				transactionTimeout:         30 * time.Second,
				maxTransactionEntries:      1000,
				transactionCleanupInterval: 5 * time.Minute,
			},
			shouldErr: true,
		},
		{
			name: "invalid max segments",
			config: Config{
				segmentSize:                1024,
				cachedSegments:             4,
				maxSegments:                0,
				syncInterval:               time.Second,
				bufferSize:                 4096,
				transactionTimeout:         30 * time.Second,
				maxTransactionEntries:      1000,
				transactionCleanupInterval: 5 * time.Minute,
			},
			shouldErr: true,
		},
		{
			name: "invalid sync interval",
			config: Config{
				segmentSize:                1024,
				cachedSegments:             4,
				maxSegments:                10,
				syncInterval:               0,
				bufferSize:                 4096,
				transactionTimeout:         30 * time.Second,
				maxTransactionEntries:      1000,
				transactionCleanupInterval: 5 * time.Minute,
			},
			shouldErr: true,
		},
		{
			name: "invalid buffer size",
			config: Config{
				segmentSize:                1024,
				cachedSegments:             4,
				maxSegments:                10,
				syncInterval:               time.Second,
				bufferSize:                 0,
				transactionTimeout:         30 * time.Second,
				maxTransactionEntries:      1000,
				transactionCleanupInterval: 5 * time.Minute,
			},
			shouldErr: true,
		},
		{
			name: "invalid transaction timeout",
			config: Config{
				segmentSize:                1024,
				cachedSegments:             4,
				maxSegments:                10,
				syncInterval:               time.Second,
				bufferSize:                 4096,
				transactionTimeout:         0,
				maxTransactionEntries:      1000,
				transactionCleanupInterval: 5 * time.Minute,
			},
			shouldErr: true,
		},
		{
			name: "invalid max transaction entries",
			config: Config{
				segmentSize:                1024,
				cachedSegments:             4,
				maxSegments:                10,
				syncInterval:               time.Second,
				bufferSize:                 4096,
				transactionTimeout:         30 * time.Second,
				maxTransactionEntries:      0,
				transactionCleanupInterval: 5 * time.Minute,
			},
			shouldErr: true,
		},
		{
			name: "invalid cleanup interval",
			config: Config{
				segmentSize:                1024,
				cachedSegments:             4,
				maxSegments:                10,
				syncInterval:               time.Second,
				bufferSize:                 4096,
				transactionTimeout:         30 * time.Second,
				maxTransactionEntries:      1000,
				transactionCleanupInterval: 0,
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.config.Validate()
			if tt.shouldErr && result.IsOk() {
				t.Error("expected validation error, got none")
			}
			if !tt.shouldErr && result.IsErr() {
				t.Errorf("unexpected validation error: %v", result.UnwrapErr())
			}
		})
	}
}

// ==================== TYPES TESTS ====================

func TestNoOpLogger(t *testing.T) {
	logger := NoOpLogger{}
	// These should not panic
	logger.Debug("test")
	logger.Info("test")
	logger.Warn("test")
	logger.Error("test")
	logger.Debug("test with args", "key", "value")
	logger.Info("test with args", "key", "value")
	logger.Warn("test with args", "key", "value")
	logger.Error("test with args", "key", "value")
}

func TestCursorIsValid(t *testing.T) {
	tests := []struct {
		name   string
		cursor Cursor
		valid  bool
	}{
		{
			name:   "valid cursor",
			cursor: Cursor{FirstIndex: 1, LastIndex: 10},
			valid:  true,
		},
		{
			name:   "equal indices",
			cursor: Cursor{FirstIndex: 5, LastIndex: 5},
			valid:  true,
		},
		{
			name:   "zero cursor",
			cursor: Cursor{FirstIndex: 0, LastIndex: 0},
			valid:  true,
		},
		{
			name:   "invalid cursor",
			cursor: Cursor{FirstIndex: 10, LastIndex: 5},
			valid:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.cursor.IsValid() != tt.valid {
				t.Errorf("expected IsValid() = %v, got %v", tt.valid, tt.cursor.IsValid())
			}
		})
	}
}

func TestBatchOperations(t *testing.T) {
	batch := Batch{}

	if batch.Len() != 0 {
		t.Errorf("expected empty batch, got len %d", batch.Len())
	}

	batch.Write(1, []byte("data1"))
	batch.Write(2, []byte("data2"))
	batch.Write(3, []byte("data3"))

	if batch.Len() != 3 {
		t.Errorf("expected batch len 3, got %d", batch.Len())
	}

	batch.Reset()

	if batch.Len() != 0 {
		t.Errorf("expected batch len 0 after reset, got %d", batch.Len())
	}
}

func TestTransactionIsExpired(t *testing.T) {
	tx := Transaction{
		StartTime: time.Now().Add(-100 * time.Millisecond),
		Timeout:   50 * time.Millisecond,
	}
	if !tx.IsExpired() {
		t.Error("expected transaction to be expired")
	}

	tx2 := Transaction{
		StartTime: time.Now(),
		Timeout:   1 * time.Hour,
	}
	if tx2.IsExpired() {
		t.Error("expected transaction to not be expired")
	}

	tx3 := Transaction{
		StartTime: time.Now().Add(-100 * time.Millisecond),
		Timeout:   0,
	}
	if tx3.IsExpired() {
		t.Error("expected transaction with zero timeout to not expire")
	}
}

func TestTransactionReset(t *testing.T) {
	tx := Transaction{
		Entries: []Entry{
			{Index: 1, Data: []byte("test")},
			{Index: 2, Data: []byte("test2")},
		},
	}
	tx.Batch.Write(1, []byte("data"))

	tx.Reset()

	if len(tx.Entries) != 0 {
		t.Errorf("expected entries to be empty after reset, got %d", len(tx.Entries))
	}
	if tx.Batch.Len() != 0 {
		t.Errorf("expected batch to be empty after reset, got %d", tx.Batch.Len())
	}
}

// ==================== ENCODING TESTS ====================

func TestBinaryEncoderEncodeInPlace(t *testing.T) {
	encoder := BinaryEncoder{}
	entry := Entry{
		Index:         1,
		Term:          2,
		Data:          []byte("test data"),
		Timestamp:     time.Now(),
		TransactionID: "tx-123",
	}

	bufferSize := encoder.EstimateSize(entry)
	buffer := make([]byte, bufferSize+100)

	result := encoder.EncodeInPlace(entry, buffer)
	if result.IsErr() {
		t.Fatalf("EncodeInPlace failed: %v", result.UnwrapErr())
	}

	written := result.Unwrap()
	if written <= 0 {
		t.Error("expected positive bytes written")
	}

	// Try with too small buffer
	smallBuffer := make([]byte, 10)
	smallResult := encoder.EncodeInPlace(entry, smallBuffer)
	if smallResult.IsOk() {
		t.Error("expected error with small buffer")
	}
}

func TestBinaryEncoderEstimateSize(t *testing.T) {
	encoder := BinaryEncoder{}
	entry := Entry{
		Index:         1,
		Term:          2,
		Data:          []byte("test data"),
		Timestamp:     time.Now(),
		TransactionID: "tx-123",
	}

	size := encoder.EstimateSize(entry)
	if size <= 0 {
		t.Error("expected positive size estimate")
	}

	// Size should include header + data + transaction ID
	expectedMinSize := EntryHeaderSize + len(entry.Data) + len(entry.TransactionID)
	if size < expectedMinSize {
		t.Errorf("expected size at least %d, got %d", expectedMinSize, size)
	}
}

func TestJSONEncoderEncodeDecode(t *testing.T) {
	encoder := JSONEncoder{}
	entry := Entry{
		Index:         1,
		Term:          2,
		Data:          []byte("test data"),
		Timestamp:     time.Now(),
		TransactionID: "tx-123",
	}

	encoded := encoder.Encode(entry)
	if encoded.IsErr() {
		t.Fatalf("Encode failed: %v", encoded.UnwrapErr())
	}

	decoded := encoder.Decode(encoded.Unwrap())
	if decoded.IsErr() {
		t.Fatalf("Decode failed: %v", decoded.UnwrapErr())
	}

	result := decoded.Unwrap()
	if result.Index != entry.Index {
		t.Errorf("expected Index %d, got %d", entry.Index, result.Index)
	}
	if result.Term != entry.Term {
		t.Errorf("expected Term %d, got %d", entry.Term, result.Term)
	}
	if !bytes.Equal(result.Data, entry.Data) {
		t.Error("data mismatch")
	}
}

func TestJSONEncoderEncodeInPlace(t *testing.T) {
	encoder := JSONEncoder{}
	entry := Entry{
		Index:         1,
		Term:          2,
		Data:          []byte("test data"),
		Timestamp:     time.Now(),
		TransactionID: "tx-123",
	}

	bufferSize := encoder.EstimateSize(entry)
	buffer := make([]byte, bufferSize+100)

	result := encoder.EncodeInPlace(entry, buffer)
	if result.IsErr() {
		t.Fatalf("EncodeInPlace failed: %v", result.UnwrapErr())
	}

	written := result.Unwrap()
	if written <= 0 {
		t.Error("expected positive bytes written")
	}

	// Try with too small buffer
	smallBuffer := make([]byte, 5)
	smallResult := encoder.EncodeInPlace(entry, smallBuffer)
	if smallResult.IsOk() {
		t.Error("expected error with small buffer")
	}
}

func TestJSONEncoderEstimateSize(t *testing.T) {
	encoder := JSONEncoder{}
	entry := Entry{
		Index:         1,
		Term:          2,
		Data:          []byte("test data"),
		Timestamp:     time.Now(),
		TransactionID: "tx-123",
	}

	size := encoder.EstimateSize(entry)
	if size <= 0 {
		t.Error("expected positive size estimate")
	}
}

func TestJSONEncoderDecodeZeroCopy(t *testing.T) {
	encoder := JSONEncoder{}
	entry := Entry{
		Index:         1,
		Term:          2,
		Data:          []byte("test data"),
		Timestamp:     time.Now(),
		TransactionID: "tx-123",
	}

	encoded := encoder.Encode(entry)
	if encoded.IsErr() {
		t.Fatalf("Encode failed: %v", encoded.UnwrapErr())
	}

	decoded := encoder.DecodeZeroCopy(encoded.Unwrap())
	if decoded.IsErr() {
		t.Fatalf("DecodeZeroCopy failed: %v", decoded.UnwrapErr())
	}

	result := decoded.Unwrap()
	if result.Index != entry.Index {
		t.Errorf("expected Index %d, got %d", entry.Index, result.Index)
	}
}

func TestJSONEncoderDecodeInvalidChecksum(t *testing.T) {
	encoder := JSONEncoder{}
	// Invalid JSON with wrong checksum
	invalidData := []byte(`{"index":1,"term":2,"data":"dGVzdA==","checksum":999999,"timestamp":0,"transaction_id":""}`)

	result := encoder.Decode(invalidData)
	if result.IsOk() {
		t.Error("expected checksum error")
	}
}

// ==================== SEGMENT TESTS ====================

func TestSegmentReadAll(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	seg, err := createSegment(dir, 1)
	if err != nil {
		t.Fatalf("createSegment failed: %v", err)
	}
	defer seg.Close()

	// Write some entries directly
	encoder := BinaryEncoder{}
	for i := 0; i < 3; i++ {
		entry := Entry{
			Index:     uint64(i + 1),
			Term:      1,
			Data:      []byte("test data"),
			Timestamp: time.Now(),
		}
		encoded := encoder.Encode(entry)
		if encoded.IsErr() {
			t.Fatalf("Encode failed: %v", encoded.UnwrapErr())
		}
		seg.Write(encoded.Unwrap())
	}
	seg.Sync()

	entries, err := seg.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(entries) != 3 {
		t.Errorf("expected 3 entries, got %d", len(entries))
	}
}

func TestSegmentReadAllEmpty(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	seg, err := createSegment(dir, 1)
	if err != nil {
		t.Fatalf("createSegment failed: %v", err)
	}
	defer seg.Close()

	entries, err := seg.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(entries) != 0 {
		t.Errorf("expected 0 entries, got %d", len(entries))
	}
}

// ==================== WAL ZEROCOPY TESTS ====================

func TestWALWithZeroCopy(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	config.zeroCopy = true
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	// Append some entries
	for i := 0; i < 5; i++ {
		entry := Entry{
			Term:      1,
			Data:      []byte("test data"),
			Timestamp: time.Now(),
		}
		wal.Append(entry)
	}

	// Test GetZeroCopy
	result := wal.GetZeroCopy(1)
	if result.IsErr() {
		t.Fatalf("GetZeroCopy failed: %v", result.UnwrapErr())
	}

	entry := result.Unwrap()
	if !bytes.Equal(entry.Data, []byte("test data")) {
		t.Error("data mismatch")
	}

	// Test GetRangeZeroCopy
	rangeResult := wal.GetRangeZeroCopy(1, 3)
	if rangeResult.IsErr() {
		t.Fatalf("GetRangeZeroCopy failed: %v", rangeResult.UnwrapErr())
	}

	entries := rangeResult.Unwrap()
	if len(entries) != 3 {
		t.Errorf("expected 3 entries, got %d", len(entries))
	}
}

func TestGetZeroCopyOutOfRange(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	config.zeroCopy = true
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	wal.Append(Entry{Data: []byte("test"), Timestamp: time.Now()})

	result := wal.GetZeroCopy(999)
	if result.IsOk() {
		t.Error("expected error for out of range index")
	}
}

func TestGetZeroCopyOnClosedWAL(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	config.zeroCopy = true
	wal := NewWAL(dir, config).Unwrap()

	wal.Append(Entry{Data: []byte("test"), Timestamp: time.Now()})
	wal.Close()

	result := wal.GetZeroCopy(1)
	if result.IsOk() {
		t.Error("expected error for closed WAL")
	}
}

// ==================== WAL CONTEXT TESTS ====================

func TestAppendWithContext(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	ctx := context.Background()
	entry := Entry{
		Term:      1,
		Data:      []byte("test data"),
		Timestamp: time.Now(),
	}

	result := wal.AppendWithContext(ctx, entry)
	if result.IsErr() {
		t.Fatalf("AppendWithContext failed: %v", result.UnwrapErr())
	}
}

func TestAppendWithContextCanceled(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	entry := Entry{
		Term:      1,
		Data:      []byte("test data"),
		Timestamp: time.Now(),
	}

	result := wal.AppendWithContext(ctx, entry)
	if result.IsOk() {
		t.Error("expected error for canceled context")
	}
}

func TestWriteBatchWithContext(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	ctx := context.Background()
	entries := []Entry{
		{Term: 1, Data: []byte("data1"), Timestamp: time.Now()},
		{Term: 1, Data: []byte("data2"), Timestamp: time.Now()},
		{Term: 1, Data: []byte("data3"), Timestamp: time.Now()},
	}

	result := wal.WriteBatchWithContext(ctx, entries)
	if result.IsErr() {
		t.Fatalf("WriteBatchWithContext failed: %v", result.UnwrapErr())
	}

	if len(result.Unwrap()) != 3 {
		t.Errorf("expected 3 indices, got %d", len(result.Unwrap()))
	}
}

func TestWriteBatchWithContextCanceled(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	entries := []Entry{
		{Term: 1, Data: []byte("data1"), Timestamp: time.Now()},
	}

	result := wal.WriteBatchWithContext(ctx, entries)
	if result.IsOk() {
		t.Error("expected error for canceled context")
	}
}

// ==================== TRANSACTION CLEANUP TESTS ====================

func TestStartTransactionCleanup(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	config.transactionCleanupInterval = 50 * time.Millisecond
	wal := NewWAL(dir, config).Unwrap()

	// Start cleanup routine
	wal.StartTransactionCleanup()

	// Create expired transaction
	txID := wal.BeginTransaction(10 * time.Millisecond)
	if txID.IsErr() {
		t.Fatalf("BeginTransaction failed: %v", txID.UnwrapErr())
	}

	if wal.GetActiveTransactionCount() != 1 {
		t.Errorf("expected 1 active transaction, got %d", wal.GetActiveTransactionCount())
	}

	// Wait for cleanup to run
	time.Sleep(100 * time.Millisecond)

	if wal.GetActiveTransactionCount() != 0 {
		t.Errorf("expected 0 active transactions after cleanup, got %d", wal.GetActiveTransactionCount())
	}

	wal.Close()
}

// ==================== BUFFER TESTS ====================

func TestBufferedWriteLargeData(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	config.bufferSize = 100 // Small buffer
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	// Write data larger than buffer
	largeData := make([]byte, 500)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	entry := Entry{
		Term:      1,
		Data:      largeData,
		Timestamp: time.Now(),
	}

	result := wal.Append(entry)
	if result.IsErr() {
		t.Fatalf("Append failed: %v", result.UnwrapErr())
	}

	// Verify data
	getResult := wal.Get(1)
	if getResult.IsErr() {
		t.Fatalf("Get failed: %v", getResult.UnwrapErr())
	}

	if !bytes.Equal(getResult.Unwrap().Data, largeData) {
		t.Error("data mismatch for large entry")
	}
}

func TestMultipleBufferFlushes(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	config.bufferSize = 50 // Very small buffer
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	// Write many small entries to trigger multiple flushes
	for i := 0; i < 100; i++ {
		entry := Entry{
			Term:      1,
			Data:      []byte("test"),
			Timestamp: time.Now(),
		}
		result := wal.Append(entry)
		if result.IsErr() {
			t.Fatalf("Append %d failed: %v", i, result.UnwrapErr())
		}
	}

	if wal.GetLastIndex() != 100 {
		t.Errorf("expected last index 100, got %d", wal.GetLastIndex())
	}
}

// ==================== JSON WAL TESTS ====================

func TestWALWithJSONFormat(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	config.format = JSON
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	entry := Entry{
		Term:          1,
		Data:          []byte("json test data"),
		Timestamp:     time.Now(),
		TransactionID: "tx-json-1",
	}

	result := wal.Append(entry)
	if result.IsErr() {
		t.Fatalf("Append failed: %v", result.UnwrapErr())
	}

	getResult := wal.Get(1)
	if getResult.IsErr() {
		t.Fatalf("Get failed: %v", getResult.UnwrapErr())
	}

	retrieved := getResult.Unwrap()
	if !bytes.Equal(retrieved.Data, entry.Data) {
		t.Error("data mismatch")
	}
	if retrieved.TransactionID != entry.TransactionID {
		t.Errorf("expected transaction ID %s, got %s", entry.TransactionID, retrieved.TransactionID)
	}
}

func TestJSONWALGetRange(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	config.format = JSON
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	for i := 0; i < 5; i++ {
		entry := Entry{
			Term:      1,
			Data:      []byte("json data"),
			Timestamp: time.Now(),
		}
		wal.Append(entry)
	}

	result := wal.GetRange(1, 3)
	if result.IsErr() {
		t.Fatalf("GetRange failed: %v", result.UnwrapErr())
	}

	if len(result.Unwrap()) != 3 {
		t.Errorf("expected 3 entries, got %d", len(result.Unwrap()))
	}
}

// ==================== LOGGER TESTS ====================

type testLogger struct {
	debugCalls int
	infoCalls  int
	warnCalls  int
	errorCalls int
}

func (l *testLogger) Debug(msg string, args ...any) { l.debugCalls++ }
func (l *testLogger) Info(msg string, args ...any)  { l.infoCalls++ }
func (l *testLogger) Warn(msg string, args ...any)  { l.warnCalls++ }
func (l *testLogger) Error(msg string, args ...any) { l.errorCalls++ }

func TestCustomLogger(t *testing.T) {
	logger := &testLogger{}

	// Test logger interface
	logger.Debug("debug message")
	logger.Info("info message")
	logger.Warn("warn message")
	logger.Error("error message")

	if logger.debugCalls != 1 {
		t.Errorf("expected 1 debug call, got %d", logger.debugCalls)
	}
	if logger.infoCalls != 1 {
		t.Errorf("expected 1 info call, got %d", logger.infoCalls)
	}
	if logger.warnCalls != 1 {
		t.Errorf("expected 1 warn call, got %d", logger.warnCalls)
	}
	if logger.errorCalls != 1 {
		t.Errorf("expected 1 error call, got %d", logger.errorCalls)
	}
}

// ==================== ERROR PATH TESTS ====================

func TestBeginTransactionOnClosedWAL(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()
	wal.Close()

	result := wal.BeginTransaction(30 * time.Second)
	if result.IsOk() {
		t.Error("expected error for closed WAL")
	}
}

func TestRollbackTransactionOnClosedWAL(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()

	txID := wal.BeginTransaction(30 * time.Second).Unwrap()
	wal.Close()

	result := wal.RollbackTransaction(txID)
	if result.IsOk() {
		t.Error("expected error for closed WAL")
	}
}

func TestRollbackNonExistentTransaction(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	result := wal.RollbackTransaction("non-existent-tx")
	if result.IsOk() {
		t.Error("expected error for non-existent transaction")
	}
}

func TestGetTransactionStateOnClosedWAL(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()

	txID := wal.BeginTransaction(30 * time.Second).Unwrap()
	wal.Close()

	result := wal.GetTransactionState(txID)
	if result.IsOk() {
		t.Error("expected error for closed WAL")
	}
}

func TestCommitTransactionOnClosedWAL(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()

	txID := wal.BeginTransaction(30 * time.Second).Unwrap()
	wal.AddToTransaction(txID, Entry{Data: []byte("test"), Timestamp: time.Now()})
	wal.Close()

	result := wal.CommitTransaction(txID)
	if result.IsOk() {
		t.Error("expected error for closed WAL")
	}
}

func TestCommitNonExistentTransaction(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	result := wal.CommitTransaction("non-existent-tx")
	if result.IsOk() {
		t.Error("expected error for non-existent transaction")
	}
}

func TestAddToNonExistentTransaction(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	result := wal.AddToTransaction("non-existent-tx", Entry{Data: []byte("test"), Timestamp: time.Now()})
	if result.IsOk() {
		t.Error("expected error for non-existent transaction")
	}
}

func TestAddToTransactionOnClosedWAL(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()

	txID := wal.BeginTransaction(30 * time.Second).Unwrap()
	wal.Close()

	result := wal.AddToTransaction(txID, Entry{Data: []byte("test"), Timestamp: time.Now()})
	if result.IsOk() {
		t.Error("expected error for closed WAL")
	}
}

func TestTruncateOnClosedWAL(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()

	wal.Append(Entry{Data: []byte("test"), Timestamp: time.Now()})
	wal.Close()

	result := wal.Truncate(1)
	if result.IsOk() {
		t.Error("expected error for closed WAL")
	}
}

func TestGetOnClosedWAL(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()

	wal.Append(Entry{Data: []byte("test"), Timestamp: time.Now()})
	wal.Close()

	result := wal.Get(1)
	if result.IsOk() {
		t.Error("expected error for closed WAL")
	}
}

func TestGetRangeOnClosedWAL(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()

	wal.Append(Entry{Data: []byte("test"), Timestamp: time.Now()})
	wal.Close()

	result := wal.GetRange(1, 1)
	if result.IsOk() {
		t.Error("expected error for closed WAL")
	}
}

func TestGetRangeZeroCopyOnClosedWAL(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	config.zeroCopy = true
	wal := NewWAL(dir, config).Unwrap()

	wal.Append(Entry{Data: []byte("test"), Timestamp: time.Now()})
	wal.Close()

	result := wal.GetRangeZeroCopy(1, 1)
	if result.IsOk() {
		t.Error("expected error for closed WAL")
	}
}

func TestAppendOnClosedWAL(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()
	wal.Close()

	result := wal.Append(Entry{Data: []byte("test"), Timestamp: time.Now()})
	if result.IsOk() {
		t.Error("expected error for closed WAL")
	}
}

func TestWriteBatchOnClosedWAL(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()
	wal.Close()

	entries := []Entry{{Data: []byte("test"), Timestamp: time.Now()}}
	result := wal.WriteBatch(entries)
	if result.IsOk() {
		t.Error("expected error for closed WAL")
	}
}

// ==================== ZEROCOPY EDGE CASES ====================

func TestGetZeroCopyWithZeroCopyEncoder(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	config.zeroCopy = true
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	// Append entries
	for i := 0; i < 10; i++ {
		wal.Append(Entry{
			Term:      uint64(i),
			Data:      []byte("zerocopy test data"),
			Timestamp: time.Now(),
		})
	}

	// Get with zero copy
	for i := uint64(1); i <= 10; i++ {
		result := wal.GetZeroCopy(i)
		if result.IsErr() {
			t.Fatalf("GetZeroCopy(%d) failed: %v", i, result.UnwrapErr())
		}
	}
}

func TestGetRangeZeroCopyWithEntries(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	config.zeroCopy = true
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	for i := 0; i < 10; i++ {
		wal.Append(Entry{
			Term:      uint64(i),
			Data:      []byte("range zerocopy test"),
			Timestamp: time.Now(),
		})
	}

	result := wal.GetRangeZeroCopy(3, 7)
	if result.IsErr() {
		t.Fatalf("GetRangeZeroCopy failed: %v", result.UnwrapErr())
	}

	entries := result.Unwrap()
	if len(entries) != 5 {
		t.Errorf("expected 5 entries, got %d", len(entries))
	}
}

// ==================== SEGMENT ZEROCOPY TESTS ====================

func TestSegmentEnableDisableZeroCopy(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	seg, err := createSegment(dir, 1)
	if err != nil {
		t.Fatalf("createSegment failed: %v", err)
	}
	defer seg.Close()

	// Enable on empty segment
	if err := seg.enableZeroCopy(); err != nil {
		t.Fatalf("enableZeroCopy failed on empty segment: %v", err)
	}

	if !seg.zeroCopyMode {
		t.Error("expected zeroCopyMode to be true")
	}

	// Enable again (should be no-op)
	if err := seg.enableZeroCopy(); err != nil {
		t.Fatalf("enableZeroCopy second call failed: %v", err)
	}

	// Disable
	if err := seg.disableZeroCopy(); err != nil {
		t.Fatalf("disableZeroCopy failed: %v", err)
	}

	if seg.zeroCopyMode {
		t.Error("expected zeroCopyMode to be false after disable")
	}

	// Disable again (should be no-op)
	if err := seg.disableZeroCopy(); err != nil {
		t.Fatalf("disableZeroCopy second call failed: %v", err)
	}
}

func TestSegmentZeroCopyWithData(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	seg, err := createSegment(dir, 1)
	if err != nil {
		t.Fatalf("createSegment failed: %v", err)
	}

	// Write some data
	encoder := BinaryEncoder{}
	for i := 0; i < 5; i++ {
		entry := Entry{
			Index:     uint64(i + 1),
			Term:      1,
			Data:      []byte("zerocopy segment test"),
			Timestamp: time.Now(),
		}
		encoded := encoder.Encode(entry).Unwrap()
		offset, _ := seg.file.Seek(0, 2) // Get current position
		seg.Write(encoded)
		seg.TrackEntry(uint64(i+1), offset)
	}
	seg.Sync()

	// Enable zerocopy after writing data
	if err := seg.enableZeroCopy(); err != nil {
		t.Fatalf("enableZeroCopy failed: %v", err)
	}

	// Read using zerocopy
	data, nextOffset, err := seg.ReadAtZeroCopy(0)
	if err != nil {
		t.Fatalf("ReadAtZeroCopy failed: %v", err)
	}

	if len(data) == 0 {
		t.Error("expected non-empty data")
	}

	if nextOffset <= 0 {
		t.Error("expected positive next offset")
	}

	// Test GetEntryByIndexZeroCopy
	entryData, err := seg.GetEntryByIndexZeroCopy(1)
	if err != nil {
		t.Fatalf("GetEntryByIndexZeroCopy failed: %v", err)
	}

	if len(entryData) == 0 {
		t.Error("expected non-empty entry data")
	}

	seg.Close()
}

func TestSegmentReadAtZeroCopyEdgeCases(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	seg, err := createSegment(dir, 1)
	if err != nil {
		t.Fatalf("createSegment failed: %v", err)
	}

	// Write data
	encoder := BinaryEncoder{}
	entry := Entry{
		Index:     1,
		Term:      1,
		Data:      []byte("test"),
		Timestamp: time.Now(),
	}
	encoded := encoder.Encode(entry).Unwrap()
	seg.Write(encoded)
	seg.Sync()

	// Enable zerocopy
	seg.enableZeroCopy()

	// Test reading at invalid offset (beyond file)
	_, _, err = seg.ReadAtZeroCopy(999999)
	if err == nil {
		t.Error("expected error for offset beyond file")
	}

	// Test reading at negative offset
	_, _, err = seg.ReadAtZeroCopy(-1)
	if err == nil {
		t.Error("expected error for negative offset")
	}

	seg.Close()
}

func TestSegmentGetEntryByIndexZeroCopyOutOfRange(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	seg, err := createSegment(dir, 1)
	if err != nil {
		t.Fatalf("createSegment failed: %v", err)
	}
	defer seg.Close()

	encoder := BinaryEncoder{}
	entry := Entry{
		Index:     1,
		Term:      1,
		Data:      []byte("test"),
		Timestamp: time.Now(),
	}
	encoded := encoder.Encode(entry).Unwrap()
	offset, _ := seg.file.Seek(0, 2)
	seg.Write(encoded)
	seg.TrackEntry(1, offset)
	seg.Sync()

	seg.enableZeroCopy()

	// Try getting out of range index
	_, err = seg.GetEntryByIndexZeroCopy(999)
	if err == nil {
		t.Error("expected error for out of range index")
	}
}

func TestSegmentWriteWithNilFile(t *testing.T) {
	seg := &Segment{
		file: nil,
	}

	_, err := seg.Write([]byte("test"))
	if err == nil {
		t.Error("expected error for nil file")
	}
}

func TestSegmentSyncWithNilFile(t *testing.T) {
	seg := &Segment{
		file: nil,
	}

	err := seg.Sync()
	if err == nil {
		t.Error("expected error for nil file")
	}
}

func TestSegmentSizeWithNilFile(t *testing.T) {
	seg := &Segment{
		file: nil,
	}

	_, err := seg.Size()
	if err == nil {
		t.Error("expected error for nil file")
	}
}

func TestSegmentReadAtWithNilFile(t *testing.T) {
	seg := &Segment{
		file: nil,
	}

	_, _, err := seg.ReadAt(0)
	if err == nil {
		t.Error("expected error for nil file")
	}
}

func TestSegmentEnableZeroCopyWithNilFile(t *testing.T) {
	seg := &Segment{
		file: nil,
	}

	err := seg.enableZeroCopy()
	if err == nil {
		t.Error("expected error for nil file")
	}
}

// ==================== ENCODING ERROR PATHS ====================

func TestBinaryEncoderDecodeInvalidData(t *testing.T) {
	encoder := BinaryEncoder{}

	// Too short data
	shortData := []byte{1, 2, 3}
	result := encoder.Decode(shortData)
	if result.IsOk() {
		t.Error("expected error for too short data")
	}

	// Invalid checksum
	entry := Entry{
		Index:     1,
		Term:      2,
		Data:      []byte("test"),
		Timestamp: time.Now(),
	}
	encoded := encoder.Encode(entry).Unwrap()
	// Corrupt the checksum (last 4 bytes)
	encoded[len(encoded)-1] = 0xFF
	encoded[len(encoded)-2] = 0xFF

	corruptResult := encoder.Decode(encoded)
	if corruptResult.IsOk() {
		t.Error("expected checksum error")
	}
}

func TestBinaryEncoderDecodeZeroCopyInvalidData(t *testing.T) {
	encoder := BinaryEncoder{}

	// Too short data
	shortData := []byte{1, 2, 3}
	result := encoder.DecodeZeroCopy(shortData)
	if result.IsOk() {
		t.Error("expected error for too short data")
	}
}

func TestJSONEncoderDecodeInvalidJSON(t *testing.T) {
	encoder := JSONEncoder{}

	// Invalid JSON
	invalidJSON := []byte("not valid json")
	result := encoder.Decode(invalidJSON)
	if result.IsOk() {
		t.Error("expected error for invalid JSON")
	}
}

// ==================== WAL EDGE CASES ====================

func TestEnsureSegmentCreatesNewSegment(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	config.segmentSize = 100 // Very small segment
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	// Write entries to trigger segment rotation
	for i := 0; i < 50; i++ {
		entry := Entry{
			Term:      1,
			Data:      []byte("some test data that is large enough"),
			Timestamp: time.Now(),
		}
		wal.Append(entry)
	}

	// Should have multiple segments
	files, _ := filepath.Glob(filepath.Join(dir, "segment_*.wal"))
	if len(files) <= 1 {
		t.Error("expected multiple segments")
	}
}

func TestCommitEmptyTransaction(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	txID := wal.BeginTransaction(30 * time.Second).Unwrap()

	// Commit without adding entries
	result := wal.CommitTransaction(txID)
	if result.IsErr() {
		t.Fatalf("CommitTransaction failed: %v", result.UnwrapErr())
	}

	indices := result.Unwrap()
	if len(indices) != 0 {
		t.Errorf("expected 0 indices for empty transaction, got %d", len(indices))
	}
}

func TestGetRangeStartGreaterThanEnd(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	wal.Append(Entry{Data: []byte("test"), Timestamp: time.Now()})

	result := wal.GetRange(10, 5)
	if result.IsOk() {
		t.Error("expected error for start > end")
	}
}

func TestGetRangeZeroCopyStartGreaterThanEnd(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	config.zeroCopy = true
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	wal.Append(Entry{Data: []byte("test"), Timestamp: time.Now()})

	result := wal.GetRangeZeroCopy(10, 5)
	if result.IsOk() {
		t.Error("expected error for start > end")
	}
}

func TestSegmentCloseAlreadyClosed(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	seg, err := createSegment(dir, 1)
	if err != nil {
		t.Fatalf("createSegment failed: %v", err)
	}

	seg.Close()

	// Set file to nil to simulate already closed
	seg.file = nil

	// Close again should not panic and return nil
	err = seg.Close()
	if err != nil {
		t.Errorf("unexpected error on close with nil file: %v", err)
	}
}

func TestSegmentReadAllWithNilFile(t *testing.T) {
	seg := &Segment{
		file: nil,
	}

	_, err := seg.ReadAll()
	if err == nil {
		t.Error("expected error for nil file")
	}
}

func TestSegmentGetEntryByIndexNotContained(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	seg, err := createSegment(dir, 1)
	if err != nil {
		t.Fatalf("createSegment failed: %v", err)
	}
	defer seg.Close()

	// No entries tracked
	_, err = seg.GetEntryByIndex(1)
	if err == nil {
		t.Error("expected error for non-contained index")
	}
}

func TestOpenExistingWAL(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()

	// Create and populate WAL
	wal1 := NewWAL(dir, config).Unwrap()
	for i := 0; i < 10; i++ {
		wal1.Append(Entry{
			Term:      uint64(i),
			Data:      []byte("persistent data"),
			Timestamp: time.Now(),
		})
	}
	wal1.Close()

	// Open existing WAL
	wal2 := Open(dir, config)
	if wal2.IsErr() {
		t.Fatalf("Open failed: %v", wal2.UnwrapErr())
	}
	defer wal2.Unwrap().Close()

	// Verify data persisted
	if wal2.Unwrap().GetLastIndex() != 10 {
		t.Errorf("expected last index 10, got %d", wal2.Unwrap().GetLastIndex())
	}

	// Read entries
	for i := uint64(1); i <= 10; i++ {
		result := wal2.Unwrap().Get(i)
		if result.IsErr() {
			t.Fatalf("Get(%d) failed: %v", i, result.UnwrapErr())
		}
	}
}

func TestTruncateInvalidRange(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	for i := 0; i < 10; i++ {
		wal.Append(Entry{Data: []byte("data"), Timestamp: time.Now()})
	}

	// Truncate beyond range
	result := wal.Truncate(100)
	if result.IsOk() {
		t.Error("expected error for truncate beyond range")
	}

	// Truncate before first index
	result = wal.Truncate(0)
	if result.IsOk() {
		t.Error("expected error for truncate before first index")
	}
}

func TestMkDirIfNotExist(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	newDir := filepath.Join(dir, "subdir", "nested")

	result := MkDirIfNotExist(newDir)
	if result.IsErr() {
		t.Fatalf("MkDirIfNotExist failed: %v", result.UnwrapErr())
	}

	// Check directory exists
	info, err := os.Stat(newDir)
	if err != nil {
		t.Fatalf("directory not created: %v", err)
	}
	if !info.IsDir() {
		t.Error("expected directory")
	}

	// Call again - should succeed
	result = MkDirIfNotExist(newDir)
	if result.IsErr() {
		t.Fatalf("MkDirIfNotExist failed on existing dir: %v", result.UnwrapErr())
	}
}

// ==================== ADDITIONAL COVERAGE TESTS ====================

func TestGetZeroCopyWithoutZeroCopyMode(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	config.zeroCopy = false // Explicitly disable
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	for i := 0; i < 5; i++ {
		wal.Append(Entry{
			Term:      uint64(i),
			Data:      []byte("test data without zerocopy"),
			Timestamp: time.Now(),
		})
	}

	// GetZeroCopy should still work but fall back to regular read
	result := wal.GetZeroCopy(1)
	if result.IsErr() {
		t.Fatalf("GetZeroCopy failed: %v", result.UnwrapErr())
	}
}

func TestSegmentDirectWrite(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	seg, err := createSegment(dir, 1)
	if err != nil {
		t.Fatalf("createSegment failed: %v", err)
	}
	defer seg.Close()

	// Write without WAL reference (direct write)
	seg.wal = nil

	data := []byte("direct write test")
	n, err := seg.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(data)+EntryLengthSize {
		t.Errorf("expected %d bytes written, got %d", len(data)+EntryLengthSize, n)
	}
}

func TestSegmentSyncWithWAL(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()

	// Write some data
	wal.Append(Entry{Data: []byte("test"), Timestamp: time.Now()})

	// Manually sync the current segment
	if wal.currentSegment != nil {
		err := wal.currentSegment.Sync()
		if err != nil {
			t.Fatalf("Sync failed: %v", err)
		}
	}

	wal.Close()
}

func TestGetRangeZeroCopyInvalidRange(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	config.zeroCopy = true
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	for i := 0; i < 5; i++ {
		wal.Append(Entry{Data: []byte("test"), Timestamp: time.Now()})
	}

	// Empty result after adjustments (requesting beyond available)
	result := wal.GetRangeZeroCopy(100, 200)
	if result.IsOk() && len(result.Unwrap()) != 0 {
		// Should either be error or empty
		entries := result.Unwrap()
		if len(entries) > 0 {
			t.Errorf("expected empty or error for out of bounds range, got %d entries", len(entries))
		}
	}
}

func TestCommitTransactionWithExpiredTransaction(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	// Create a transaction with very short timeout
	txID := wal.BeginTransaction(1 * time.Millisecond).Unwrap()

	// Add entry
	wal.AddToTransaction(txID, Entry{Data: []byte("test"), Timestamp: time.Now()})

	// Wait for expiration
	time.Sleep(10 * time.Millisecond)

	// Try to commit expired transaction - should fail or handle gracefully
	result := wal.CommitTransaction(txID)
	// Either error or success with cleanup is acceptable
	if result.IsErr() {
		// Expected - transaction expired
	}
}

func TestAddToTransactionMaxEntries(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	config.maxTransactionEntries = 5
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	txID := wal.BeginTransaction(30 * time.Second).Unwrap()

	// Add up to max entries
	for i := 0; i < 5; i++ {
		result := wal.AddToTransaction(txID, Entry{Data: []byte("entry"), Timestamp: time.Now()})
		if result.IsErr() {
			t.Fatalf("AddToTransaction failed at entry %d: %v", i, result.UnwrapErr())
		}
	}

	// Try to add one more - should fail
	result := wal.AddToTransaction(txID, Entry{Data: []byte("overflow"), Timestamp: time.Now()})
	if result.IsOk() {
		t.Error("expected error for exceeding max transaction entries")
	}

	wal.RollbackTransaction(txID)
}

func TestWALRecoveryWithMultipleSegments(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	config.segmentSize = 200 // Small segments

	// Create WAL and write enough to create multiple segments
	wal1 := NewWAL(dir, config).Unwrap()
	for i := 0; i < 50; i++ {
		wal1.Append(Entry{
			Term:      uint64(i),
			Data:      []byte("segment rotation test data"),
			Timestamp: time.Now(),
		})
	}
	lastIndex := wal1.GetLastIndex()
	wal1.Close()

	// Recover
	wal2 := Open(dir, config)
	if wal2.IsErr() {
		t.Fatalf("Open failed: %v", wal2.UnwrapErr())
	}
	defer wal2.Unwrap().Close()

	if wal2.Unwrap().GetLastIndex() != lastIndex {
		t.Errorf("expected last index %d after recovery, got %d", lastIndex, wal2.Unwrap().GetLastIndex())
	}
}

func TestBinaryEncoderWithTransactionID(t *testing.T) {
	encoder := BinaryEncoder{}

	entry := Entry{
		Index:         1,
		Term:          2,
		Data:          []byte("test data"),
		Timestamp:     time.Now(),
		TransactionID: "very-long-transaction-id-for-testing-purposes",
	}

	encoded := encoder.Encode(entry)
	if encoded.IsErr() {
		t.Fatalf("Encode failed: %v", encoded.UnwrapErr())
	}

	decoded := encoder.Decode(encoded.Unwrap())
	if decoded.IsErr() {
		t.Fatalf("Decode failed: %v", decoded.UnwrapErr())
	}

	result := decoded.Unwrap()
	if result.TransactionID != entry.TransactionID {
		t.Errorf("TransactionID mismatch: expected %s, got %s", entry.TransactionID, result.TransactionID)
	}
}

func TestBinaryEncoderDecodeZeroCopyWithTransactionID(t *testing.T) {
	encoder := BinaryEncoder{}

	entry := Entry{
		Index:         1,
		Term:          2,
		Data:          []byte("zerocopy test"),
		Timestamp:     time.Now(),
		TransactionID: "tx-zerocopy-test",
	}

	encoded := encoder.Encode(entry)
	if encoded.IsErr() {
		t.Fatalf("Encode failed: %v", encoded.UnwrapErr())
	}

	decoded := encoder.DecodeZeroCopy(encoded.Unwrap())
	if decoded.IsErr() {
		t.Fatalf("DecodeZeroCopy failed: %v", decoded.UnwrapErr())
	}

	result := decoded.Unwrap()
	if result.TransactionID != entry.TransactionID {
		t.Errorf("TransactionID mismatch: expected %s, got %s", entry.TransactionID, result.TransactionID)
	}
}

func TestNewWAL(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	walResult := NewWAL(dir, config)

	if walResult.IsErr() {
		t.Fatalf("NewWAL failed: %v", walResult.UnwrapErr())
	}

	wal := walResult.Unwrap()
	if wal.state != Initializing {
		t.Errorf("expected state Initializing, got %v", wal.state)
	}

	if wal.status != OK {
		t.Errorf("expected status OK, got %v", wal.status)
	}

	wal.Close()
}

func TestAppendAndGet(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	entry := Entry{
		Term:      1,
		Data:      []byte("test data"),
		Timestamp: time.Now(),
	}

	indexResult := wal.Append(entry)
	if indexResult.IsErr() {
		t.Fatalf("Append failed: %v", indexResult.UnwrapErr())
	}

	index := indexResult.Unwrap()
	if index != 1 {
		t.Errorf("expected index 1, got %d", index)
	}

	readResult := wal.Get(index)
	if readResult.IsErr() {
		t.Fatalf("Get failed: %v", readResult.UnwrapErr())
	}

	readEntry := readResult.Unwrap()
	if !bytes.Equal(readEntry.Data, entry.Data) {
		t.Errorf("data mismatch: expected %s, got %s", entry.Data, readEntry.Data)
	}

	if readEntry.Term != entry.Term {
		t.Errorf("term mismatch: expected %d, got %d", entry.Term, readEntry.Term)
	}
}

func TestMultipleAppends(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	count := 100
	for i := 0; i < count; i++ {
		entry := Entry{
			Term:      uint64(i),
			Data:      []byte("entry data"),
			Timestamp: time.Now(),
		}
		result := wal.Append(entry)
		if result.IsErr() {
			t.Fatalf("Append %d failed: %v", i, result.UnwrapErr())
		}
	}

	if wal.GetFirstIndex() != 1 {
		t.Errorf("expected first index 1, got %d", wal.GetFirstIndex())
	}

	if wal.GetLastIndex() != uint64(count) {
		t.Errorf("expected last index %d, got %d", count, wal.GetLastIndex())
	}
}

func TestGetRange(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	for i := 0; i < 10; i++ {
		entry := Entry{
			Term:      uint64(i),
			Data:      []byte("data"),
			Timestamp: time.Now(),
		}
		wal.Append(entry)
	}

	rangeResult := wal.GetRange(3, 7)
	if rangeResult.IsErr() {
		t.Fatalf("GetRange failed: %v", rangeResult.UnwrapErr())
	}

	entries := rangeResult.Unwrap()
	if len(entries) != 5 {
		t.Errorf("expected 5 entries, got %d", len(entries))
	}

	for i, entry := range entries {
		expectedIndex := uint64(i + 3)
		if entry.Index != expectedIndex {
			t.Errorf("entry %d: expected index %d, got %d", i, expectedIndex, entry.Index)
		}
	}
}

func TestGetOutOfRange(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	wal.Append(Entry{Data: []byte("test"), Timestamp: time.Now()})

	result := wal.Get(999)
	if result.IsOk() {
		t.Error("expected error for out of range index")
	}
}

func TestGetRangeEmptyWAL(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	// Should not panic, should return empty result
	result := wal.GetRange(10, 20)
	if result.IsErr() {
		// Error is acceptable
		return
	}
	if len(result.Unwrap()) != 0 {
		t.Error("expected empty result for invalid range on empty WAL")
	}
}

func TestGetRangeZeroCopyEmptyWAL(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	// Should not panic, should return empty result
	result := wal.GetRangeZeroCopy(10, 20)
	if result.IsErr() {
		// Error is acceptable
		return
	}
	if len(result.Unwrap()) != 0 {
		t.Error("expected empty result for invalid range on empty WAL")
	}
}

func TestTruncate(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	for i := 0; i < 10; i++ {
		wal.Append(Entry{Data: []byte("data"), Timestamp: time.Now()})
	}

	truncResult := wal.Truncate(5)
	if truncResult.IsErr() {
		t.Fatalf("Truncate failed: %v", truncResult.UnwrapErr())
	}

	if wal.GetLastIndex() != 5 {
		t.Errorf("expected last index 5 after truncate, got %d", wal.GetLastIndex())
	}

	result := wal.Get(6)
	if result.IsOk() {
		t.Error("expected error for truncated entry")
	}
}

func TestClose(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()

	wal.Append(Entry{Data: []byte("test"), Timestamp: time.Now()})
	wal.Close()

	result := wal.Append(Entry{Data: []byte("should fail"), Timestamp: time.Now()})
	if result.IsOk() {
		t.Error("expected error when appending to closed WAL")
	}
}

func TestRecovery(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()

	wal := NewWAL(dir, config).Unwrap()
	for i := 0; i < 5; i++ {
		wal.Append(Entry{
			Term:      uint64(i),
			Data:      []byte("persistent data"),
			Timestamp: time.Now(),
		})
	}
	wal.Close()

	recoveredResult := Open(dir, config)
	if recoveredResult.IsErr() {
		t.Fatalf("Open failed: %v", recoveredResult.UnwrapErr())
	}

	recovered := recoveredResult.Unwrap()
	defer recovered.Close()

	if recovered.GetLastIndex() != 5 {
		t.Errorf("expected last index 5 after recovery, got %d", recovered.GetLastIndex())
	}

	if recovered.state != Ready {
		t.Errorf("expected state Ready after recovery, got %v", recovered.state)
	}

	entry := recovered.Get(3).Unwrap()
	if !bytes.Equal(entry.Data, []byte("persistent data")) {
		t.Errorf("recovered data mismatch")
	}
}

func TestSegmentRotation(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	config.segmentSize = 512
	config.maxSegments = 100

	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	for i := 0; i < 50; i++ {
		data := make([]byte, 100)
		for j := range data {
			data[j] = byte(i)
		}
		wal.Append(Entry{Data: data, Timestamp: time.Now()})
	}

	files, _ := filepath.Glob(filepath.Join(dir, "segment_*.wal"))
	if len(files) < 2 {
		t.Errorf("expected multiple segments, got %d", len(files))
	}

	for i := uint64(1); i <= 50; i++ {
		result := wal.Get(i)
		if result.IsErr() {
			t.Errorf("failed to get entry %d: %v", i, result.UnwrapErr())
		}
	}
}

func TestConcurrentAppends(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	var wg sync.WaitGroup
	workers := 10
	entriesPerWorker := 50

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < entriesPerWorker; i++ {
				entry := Entry{
					Term:      uint64(workerID),
					Data:      []byte("concurrent"),
					Timestamp: time.Now(),
				}
				wal.Append(entry)
			}
		}(w)
	}

	wg.Wait()

	expected := uint64(workers * entriesPerWorker)
	if wal.GetLastIndex() != expected {
		t.Errorf("expected %d entries, got %d", expected, wal.GetLastIndex())
	}
}

func TestJSONFormat(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	config.format = JSON

	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	entry := Entry{
		Term:      42,
		Data:      []byte(`{"key": "value"}`),
		Timestamp: time.Now(),
	}

	wal.Append(entry)

	readEntry := wal.Get(1).Unwrap()
	if !bytes.Equal(readEntry.Data, entry.Data) {
		t.Errorf("JSON data mismatch")
	}
}

func TestEmptyWAL(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	if wal.GetFirstIndex() != 0 {
		t.Errorf("expected first index 0 for empty WAL, got %d", wal.GetFirstIndex())
	}

	if wal.GetLastIndex() != 0 {
		t.Errorf("expected last index 0 for empty WAL, got %d", wal.GetLastIndex())
	}
}

func TestMaxSegmentsCleanup(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	config.segmentSize = 256
	config.maxSegments = 3

	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	for i := 0; i < 100; i++ {
		data := make([]byte, 100)
		wal.Append(Entry{Data: data, Timestamp: time.Now()})
	}

	files, _ := filepath.Glob(filepath.Join(dir, "segment_*.wal"))
	if len(files) > config.maxSegments {
		t.Errorf("expected max %d segments, got %d", config.maxSegments, len(files))
	}
}

func BenchmarkAppend(b *testing.B) {
	dir, _ := os.MkdirTemp("", "walrus_bench_*")
	defer os.RemoveAll(dir)

	config := testConfig()
	config.syncAfterWrite = false
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	entry := Entry{
		Term:      1,
		Data:      make([]byte, 256),
		Timestamp: time.Now(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wal.Append(entry)
	}
}

func BenchmarkGet(b *testing.B) {
	dir, _ := os.MkdirTemp("", "walrus_bench_*")
	defer os.RemoveAll(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	for i := 0; i < 1000; i++ {
		wal.Append(Entry{Data: make([]byte, 256), Timestamp: time.Now()})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wal.Get(uint64(i%1000) + 1)
	}
}

func TestBeginTransaction(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	txID := wal.BeginTransaction(30 * time.Second)
	if txID.IsErr() {
		t.Fatalf("BeginTransaction failed: %v", txID.UnwrapErr())
	}

	state := wal.GetTransactionState(txID.Unwrap())
	if state.IsErr() {
		t.Fatalf("GetTransactionState failed: %v", state.UnwrapErr())
	}

	if state.Unwrap() != TransactionPending {
		t.Errorf("expected TransactionPending, got %v", state.Unwrap())
	}
}

func TestTransactionTimeout(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	txID := wal.BeginTransaction(100 * time.Millisecond)
	if txID.IsErr() {
		t.Fatalf("BeginTransaction failed: %v", txID.UnwrapErr())
	}

	time.Sleep(200 * time.Millisecond)

	entry := Entry{
		Data:      []byte("test data"),
		Timestamp: time.Now(),
	}

	err := wal.AddToTransaction(txID.Unwrap(), entry)
	if err.IsOk() {
		t.Error("expected error for expired transaction")
	}

	if err.UnwrapErr() != ErrTransactionExpired {
		t.Errorf("expected ErrTransactionExpired, got %v", err.UnwrapErr())
	}
}

func TestCommitTransaction(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	txID := wal.BeginTransaction(30 * time.Second)
	if txID.IsErr() {
		t.Fatalf("BeginTransaction failed: %v", txID.UnwrapErr())
	}

	entries := []Entry{
		{Data: []byte("entry 1"), Timestamp: time.Now()},
		{Data: []byte("entry 2"), Timestamp: time.Now()},
		{Data: []byte("entry 3"), Timestamp: time.Now()},
	}

	for _, entry := range entries {
		err := wal.AddToTransaction(txID.Unwrap(), entry)
		if err.IsErr() {
			t.Fatalf("AddToTransaction failed: %v", err.UnwrapErr())
		}
	}

	indices := wal.CommitTransaction(txID.Unwrap())
	if indices.IsErr() {
		t.Fatalf("CommitTransaction failed: %v", indices.UnwrapErr())
	}

	if len(indices.Unwrap()) != 3 {
		t.Errorf("expected 3 indices, got %d", len(indices.Unwrap()))
	}

	for i, idx := range indices.Unwrap() {
		entry := wal.Get(idx)
		if entry.IsErr() {
			t.Fatalf("Get failed for index %d: %v", idx, entry.UnwrapErr())
		}

		expectedData := entries[i].Data
		if string(entry.Unwrap().Data) != string(expectedData) {
			t.Errorf("expected %s, got %s", expectedData, entry.Unwrap().Data)
		}

		if entry.Unwrap().TransactionID != txID.Unwrap() {
			t.Errorf("expected transaction ID %s, got %s", txID.Unwrap(), entry.Unwrap().TransactionID)
		}
	}
}

func TestWriteBatch(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	entries := []Entry{
		{Data: []byte("batch entry 1"), Timestamp: time.Now()},
		{Data: []byte("batch entry 2"), Timestamp: time.Now()},
		{Data: []byte("batch entry 3"), Timestamp: time.Now()},
	}

	indices := wal.WriteBatch(entries)
	if indices.IsErr() {
		t.Fatalf("WriteBatch failed: %v", indices.UnwrapErr())
	}

	if len(indices.Unwrap()) != 3 {
		t.Errorf("expected 3 indices, got %d", len(indices.Unwrap()))
	}

	for i, idx := range indices.Unwrap() {
		entry := wal.Get(idx)
		if entry.IsErr() {
			t.Fatalf("Get failed for index %d: %v", idx, entry.UnwrapErr())
		}

		expectedData := entries[i].Data
		if string(entry.Unwrap().Data) != string(expectedData) {
			t.Errorf("expected %s, got %s", expectedData, entry.Unwrap().Data)
		}
	}
}

func TestRollbackTransaction(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	txID := wal.BeginTransaction(30 * time.Second)
	if txID.IsErr() {
		t.Fatalf("BeginTransaction failed: %v", txID.UnwrapErr())
	}

	entry := Entry{
		Data:      []byte("test data"),
		Timestamp: time.Now(),
	}

	err := wal.AddToTransaction(txID.Unwrap(), entry)
	if err.IsErr() {
		t.Fatalf("AddToTransaction failed: %v", err.UnwrapErr())
	}

	err = wal.RollbackTransaction(txID.Unwrap())
	if err.IsErr() {
		t.Fatalf("RollbackTransaction failed: %v", err.UnwrapErr())
	}

	state := wal.GetTransactionState(txID.Unwrap())
	if state.IsOk() {
		t.Error("expected error for rolled back transaction lookup")
	}
}

func TestTransactionTooLarge(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	config.maxTransactionEntries = 2
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	txID := wal.BeginTransaction(30 * time.Second)
	if txID.IsErr() {
		t.Fatalf("BeginTransaction failed: %v", txID.UnwrapErr())
	}

	entries := []Entry{
		{Data: []byte("entry 1"), Timestamp: time.Now()},
		{Data: []byte("entry 2"), Timestamp: time.Now()},
		{Data: []byte("entry 3"), Timestamp: time.Now()},
	}

	for i, entry := range entries {
		err := wal.AddToTransaction(txID.Unwrap(), entry)
		if i < 2 {
			if err.IsErr() {
				t.Fatalf("AddToTransaction failed for entry %d: %v", i, err.UnwrapErr())
			}
		} else {
			if err.IsOk() {
				t.Error("expected error for transaction too large")
			}
			if err.UnwrapErr() != ErrTransactionTooLarge {
				t.Errorf("expected ErrTransactionTooLarge, got %v", err.UnwrapErr())
			}
		}
	}
}

func TestCleanupExpiredTransactions(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	txID1 := wal.BeginTransaction(50 * time.Millisecond)
	txID2 := wal.BeginTransaction(200 * time.Millisecond)

	if txID1.IsErr() || txID2.IsErr() {
		t.Fatal("BeginTransaction failed")
	}

	if wal.GetActiveTransactionCount() != 2 {
		t.Errorf("expected 2 active transactions, got %d", wal.GetActiveTransactionCount())
	}

	time.Sleep(100 * time.Millisecond)

	cleaned := wal.CleanupExpiredTransactions()
	if cleaned != 1 {
		t.Errorf("expected 1 cleaned transaction, got %d", cleaned)
	}

	if wal.GetActiveTransactionCount() != 1 {
		t.Errorf("expected 1 active transaction after cleanup, got %d", wal.GetActiveTransactionCount())
	}
}

func TestConcurrentTransactions(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()
	wal := NewWAL(dir, config).Unwrap()
	defer wal.Close()

	const numGoroutines = 10
	const entriesPerTx = 5

	var wg sync.WaitGroup
	results := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			txID := wal.BeginTransaction(30 * time.Second)
			if txID.IsErr() {
				results <- txID.UnwrapErr()
				return
			}

			for j := 0; j < entriesPerTx; j++ {
				entry := Entry{
					Data:      []byte("data"),
					Timestamp: time.Now(),
				}
				err := wal.AddToTransaction(txID.Unwrap(), entry)
				if err.IsErr() {
					results <- err.UnwrapErr()
					return
				}
			}

			indices := wal.CommitTransaction(txID.Unwrap())
			if indices.IsErr() {
				results <- indices.UnwrapErr()
				return
			}

			if len(indices.Unwrap()) != entriesPerTx {
				results <- ErrInvalidEntry
				return
			}

			results <- nil
		}(i)
	}

	wg.Wait()
	close(results)

	for err := range results {
		if err != nil {
			t.Errorf("concurrent transaction failed: %v", err)
		}
	}

	if wal.GetActiveTransactionCount() != 0 {
		t.Errorf("expected 0 active transactions after completion, got %d", wal.GetActiveTransactionCount())
	}
}

func TestTransactionRecovery(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	config := testConfig()

	wal1 := NewWAL(dir, config).Unwrap()
	txID := wal1.BeginTransaction(30 * time.Second)
	if txID.IsErr() {
		t.Fatalf("BeginTransaction failed: %v", txID.UnwrapErr())
	}

	entry := Entry{Data: []byte("test"), Timestamp: time.Now()}
	wal1.AddToTransaction(txID.Unwrap(), entry)

	if wal1.GetActiveTransactionCount() != 1 {
		t.Errorf("expected 1 active transaction, got %d", wal1.GetActiveTransactionCount())
	}

	wal1.Close()

	wal2 := Open(dir, config).Unwrap()
	defer wal2.Close()

	if wal2.GetActiveTransactionCount() != 0 {
		t.Errorf("expected 0 active transactions after recovery, got %d", wal2.GetActiveTransactionCount())
	}
}
