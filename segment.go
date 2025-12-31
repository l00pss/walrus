package walrus

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	SegmentFileExtension = ".wal"
	SegmentFilePrefix    = "segment_"
	EntryLengthSize      = 4 // 4 bytes
)

type Segment struct {
	path         string
	index        uint64
	filePath     string
	file         *os.File
	firstEntry   uint64
	lastEntry    uint64
	entryOffsets []int64
}

func (s *Segment) Write(data []byte) (int, error) {
	if s.file == nil {
		return 0, ErrSegmentNotFound
	}

	// Write entry length prefix (4 bytes)
	lengthBuf := make([]byte, EntryLengthSize)
	binary.LittleEndian.PutUint32(lengthBuf, uint32(len(data)))

	n, err := s.file.Write(lengthBuf)
	if err != nil {
		return n, err
	}

	n2, err := s.file.Write(data)
	return n + n2, err
}

func (s *Segment) Sync() error {
	if s.file == nil {
		return ErrSegmentNotFound
	}
	return s.file.Sync()
}

func (s *Segment) Close() error {
	if s.file == nil {
		return nil
	}
	return s.file.Close()
}

func (s *Segment) Size() (int64, error) {
	if s.file == nil {
		return 0, ErrSegmentNotFound
	}
	info, err := s.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (s *Segment) ReadAt(offset int64) ([]byte, int64, error) {
	if s.file == nil {
		return nil, 0, ErrSegmentNotFound
	}

	lengthBuf := make([]byte, EntryLengthSize)
	_, err := s.file.ReadAt(lengthBuf, offset)
	if err != nil {
		return nil, 0, err
	}

	entryLen := binary.LittleEndian.Uint32(lengthBuf)
	data := make([]byte, entryLen)

	_, err = s.file.ReadAt(data, offset+EntryLengthSize)
	if err != nil {
		return nil, 0, err
	}

	nextOffset := offset + EntryLengthSize + int64(entryLen)
	return data, nextOffset, nil
}

func (s *Segment) ReadAll() ([][]byte, error) {
	if s.file == nil {
		return nil, ErrSegmentNotFound
	}

	var entries [][]byte
	var offset int64 = 0

	for {
		data, nextOffset, err := s.ReadAt(offset)
		if err == io.EOF {
			break
		}
		if err != nil {
			return entries, err
		}
		entries = append(entries, data)
		offset = nextOffset
	}

	return entries, nil
}

func createSegment(dir string, index uint64) (*Segment, error) {
	filename := fmt.Sprintf("%s%020d%s", SegmentFilePrefix, index, SegmentFileExtension)
	filePath := filepath.Join(dir, filename)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.FileMode(FilePermission))
	if err != nil {
		return nil, err
	}

	return &Segment{
		path:         dir,
		index:        index,
		filePath:     filePath,
		file:         file,
		firstEntry:   0,
		lastEntry:    0,
		entryOffsets: make([]int64, 0),
	}, nil
}

func openSegment(filePath string) (*Segment, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_APPEND, os.FileMode(FilePermission))
	if err != nil {
		return nil, err
	}

	index, err := parseSegmentIndex(filepath.Base(filePath))
	if err != nil {
		file.Close()
		return nil, err
	}

	return &Segment{
		path:         filepath.Dir(filePath),
		index:        index,
		filePath:     filePath,
		file:         file,
		firstEntry:   0,
		lastEntry:    0,
		entryOffsets: make([]int64, 0),
	}, nil
}

func parseSegmentIndex(filename string) (uint64, error) {
	name := strings.TrimPrefix(filename, SegmentFilePrefix)
	name = strings.TrimSuffix(name, SegmentFileExtension)
	return strconv.ParseUint(name, 10, 64)
}

func loadSegments(dir string) ([]*Segment, error) {
	pattern := filepath.Join(dir, SegmentFilePrefix+"*"+SegmentFileExtension)
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	sort.Strings(files)

	segments := make([]*Segment, 0, len(files))
	for _, f := range files {
		seg, err := openSegment(f)
		if err != nil {
			for _, s := range segments {
				s.Close()
			}
			return nil, err
		}
		segments = append(segments, seg)
	}

	return segments, nil
}

func (s *Segment) TrackEntry(entryIndex uint64, offset int64) {
	if s.firstEntry == 0 {
		s.firstEntry = entryIndex
	}
	s.lastEntry = entryIndex
	s.entryOffsets = append(s.entryOffsets, offset)
}

func (s *Segment) ContainsIndex(entryIndex uint64) bool {
	return s.firstEntry > 0 && entryIndex >= s.firstEntry && entryIndex <= s.lastEntry
}

func (s *Segment) GetEntryByIndex(entryIndex uint64) ([]byte, error) {
	if !s.ContainsIndex(entryIndex) {
		return nil, ErrIndexOutOfRange
	}

	localIndex := entryIndex - s.firstEntry
	if localIndex >= uint64(len(s.entryOffsets)) {
		return nil, ErrIndexOutOfRange
	}

	offset := s.entryOffsets[localIndex]
	data, _, err := s.ReadAt(offset)
	return data, err
}
