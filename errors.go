package walrus

import "errors"

var (
	ErrChecksumMismatch      = errors.New("checksum mismatch: data may be corrupted")
	ErrEmptyData             = errors.New("entry data cannot be empty")
	ErrInvalidEntry          = errors.New("invalid entry")
	ErrWALCorrupted          = errors.New("WAL is corrupted")
	ErrWALClosed             = errors.New("WAL is closed")
	ErrIndexOutOfRange       = errors.New("index out of range")
	ErrSegmentNotFound       = errors.New("segment not found")
	ErrSegmentFull           = errors.New("segment is full")
	ErrUnsupportedFormat     = errors.New("unsupported encode format")
	ErrTransactionNotFound   = errors.New("transaction not found")
	ErrTransactionNotPending = errors.New("transaction is not in pending state")
	ErrTransactionExpired    = errors.New("transaction has expired")
	ErrTransactionTooLarge   = errors.New("transaction has too many entries")
	UnknownError             = errors.New("Unknown error")
)
