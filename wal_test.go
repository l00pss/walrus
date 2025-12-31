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
		syncAfterWrite: true,
		dir:            "",
		segmentSize:    1024 * 1024,
		cachedSegments: 4,
		maxSegments:    10,
		syncInterval:   time.Second,
		bufferSize:     4096,
		zeroCopy:       false,
		format:         BINARY,
		compression:    false,
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
