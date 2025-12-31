package walrus

import (
	"errors"
	"time"

	"github.com/l00pss/helpme/result"
)

type LogFormat int

const (
	BINARY  LogFormat = 0
	JSON    LogFormat = 1
	DEFAULT           = BINARY
)

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
