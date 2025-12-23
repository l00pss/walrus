package walrus

import (
	"errors"
	"time"

	"github.com/l00pss/helpme/result"
)

/**
LogFormat defines the format in which log entries are stored in the Write-Ahead Log (WAL).
Possible values include:
* BINARY: Log entries are stored in a binary format for efficient storage and retrieval.
* JSON: Log entries are stored in JSON format for human readability and ease of debugging.
* PROTO: Log entries are serialized using Protocol Buffers for compactness and cross-language compatibility.
*/

type LogFormat int

const (
	BINARY  LogFormat = 0
	JSON    LogFormat = 1
	PROTO   LogFormat = 2
	DEFAULT           = BINARY
)

/**
Config defines the configuration for the Write-Ahead Log (WAL) system.
* SyncAfterWrite: If true, the WAL will perform an fsync operation after each write to ensure data durability.
* Dir: The directory where WAL files will be stored.
* SegmentSize: The size of each WAL segment file in bytes.
* CachedSegments: The number of WAL segments to keep in memory for quick access.
* MaxSegments: The maximum number of WAL segments to retain before old segments are deleted.
* SyncInterval: The interval at which the WAL will perform periodic fsync operations if SyncAfterWrite is false.
* BufferSize: The size of the write buffer used for batching writes to the WAL.
* ZeroCopy: If true, the WAL will use zero-copy techniques to optimize write performance.
* Format: The format in which log entries are stored (e.g., BINARY, JSON, PROTO).
* Compression: If true, log entries will be compressed before being written to disk to save space.
*/

type Config struct {
	syncAfterWrite bool
	dir            string
	segmentSize    int64
	cachedSegments int
	maxSegments    int
	syncInterval   time.Duration
	bufferSize     int
	zeroCopy       bool
	format         LogFormat
	compression    bool
}

func (c *Config) Validate() result.Result[Config] {
	if c.segmentSize <= 0 {
		return result.Err[Config](errors.New("segment size must be greater than 0"))
	}
	if c.cachedSegments <= 0 {
		return result.Err[Config](errors.New("cached segments must be greater than 0"))
	}
	if c.maxSegments <= 0 {
		return result.Err[Config](errors.New("max segments must be greater than 0"))
	}
	if c.syncInterval <= 0 {
		return result.Err[Config](errors.New("sync interval must be greater than zero"))
	}
	if c.bufferSize <= 0 {
		return result.Err[Config](errors.New("buffer size must be greater than 0"))
	}
	return result.Ok(*c)
}

func DefaultConfig() Config {
	return Config{
		syncAfterWrite: true,
		dir:            "./wal",
		segmentSize:    64 * 1024 * 1024, // 64 MB
		cachedSegments: 4,
		maxSegments:    100,
		syncInterval:   1 * time.Second,
		bufferSize:     4096, // 4 KB
		zeroCopy:       false,
		format:         BINARY,
		compression:    false,
	}
}
