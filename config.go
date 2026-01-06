package walrus

import (
	"errors"
	"time"

	"github.com/l00pss/helpme/result"
	"github.com/l00pss/littlecache"
)

type LogFormat int

const (
	BINARY  LogFormat = 0
	JSON    LogFormat = 1
	DEFAULT           = BINARY
)

type Config struct {
	syncAfterWrite             bool
	dir                        string
	segmentSize                int64
	cachedSegments             int
	cacheMaxItemSize           int
	evictionPolicy             littlecache.EvictionPolicy
	maxSegments                int
	syncInterval               time.Duration
	bufferSize                 int
	zeroCopy                   bool
	format                     LogFormat
	compression                bool
	transactionTimeout         time.Duration
	maxTransactionEntries      int
	transactionCleanupInterval time.Duration
}

func (c *Config) Validate() result.Result[Config] {
	if c.segmentSize <= 0 {
		return result.Err[Config](errors.New("segment size must be greater than 0"))
	}
	if c.cachedSegments <= 0 {
		return result.Err[Config](errors.New("cached segments must be greater than 0"))
	}
	if c.cacheMaxItemSize <= 0 {
		c.cacheMaxItemSize = 1024
	}
	if c.evictionPolicy == littlecache.EvictionPolicy(0) {
		c.evictionPolicy = littlecache.LRU
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
	if c.transactionTimeout <= 0 {
		return result.Err[Config](errors.New("transaction timeout must be greater than 0"))
	}
	if c.maxTransactionEntries <= 0 {
		return result.Err[Config](errors.New("max transaction entries must be greater than 0"))
	}
	if c.transactionCleanupInterval <= 0 {
		return result.Err[Config](errors.New("transaction cleanup interval must be greater than 0"))
	}
	return result.Ok(*c)
}

func DefaultConfig() Config {
	return Config{
		syncAfterWrite:             true,
		dir:                        "./wal",
		segmentSize:                64 * 1024 * 1024, // 64 MB
		cachedSegments:             4,
		maxSegments:                100,
		syncInterval:               1 * time.Second,
		bufferSize:                 4096, // 4 KB
		zeroCopy:                   false,
		format:                     BINARY,
		compression:                false,
		transactionTimeout:         30 * time.Second,
		maxTransactionEntries:      1000,
		transactionCleanupInterval: 5 * time.Minute,
	}
}
