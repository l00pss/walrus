package walrus

import (
	"path/filepath"
	"sync"

	"github.com/l00pss/helpme/result"
)

type WAL struct {
	mu             sync.RWMutex
	dir            string
	segments       []*Segment
	currentSegment *Segment
	lastIndex      uint64
	config         Config
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

	w := &WAL{
		dir:    dir,
		config: configResult.Unwrap(),
	}
	return result.Ok(w)
}
