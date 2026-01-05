package walrus

import (
	"bytes"
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

func TestConfigValidation(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(dir)

	badConfig := Config{
		segmentSize:    0,
		cachedSegments: 4,
		maxSegments:    10,
		syncInterval:   time.Second,
		bufferSize:     4096,
	}

	result := NewWAL(dir, badConfig)
	if result.IsOk() {
		t.Error("expected error for invalid config")
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
