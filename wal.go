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
	seralizedEntryResult := w.encoder.Encode(entry)
	if seralizedEntryResult.IsErr() {
		return result.Err[uint64](seralizedEntryResult.UnwrapErr())
	}

	return result.Err[uint64](UnknownError)
}
