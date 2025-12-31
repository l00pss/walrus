package walrus

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"time"

	"github.com/l00pss/helpme/result"
)

const (
	EntryHeaderSize = 8 + 8 + 4 + 8 + 4 // Index + Term + DataLen + Timestamp + Checksum
)

type Encoder interface {
	Encode(entry Entry) result.Result[[]byte]
	Decode(data []byte) result.Result[Entry]
}

type BinaryEncoder struct {
	Encoder
}

func (e *BinaryEncoder) encode(entry Entry) result.Result[[]byte] {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.LittleEndian, entry.Index); err != nil {
		return result.Err[[]byte](err)
	}
	if err := binary.Write(buf, binary.LittleEndian, entry.Term); err != nil {
		return result.Err[[]byte](err)
	}
	if err := binary.Write(buf, binary.LittleEndian, entry.Timestamp.UnixNano()); err != nil {
		return result.Err[[]byte](err)
	}
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(entry.Data))); err != nil {
		return result.Err[[]byte](err)
	}
	if _, err := buf.Write(entry.Data); err != nil {
		return result.Err[[]byte](err)
	}

	checksum := crc32.ChecksumIEEE(buf.Bytes())
	if err := binary.Write(buf, binary.LittleEndian, checksum); err != nil {
		return result.Err[[]byte](err)
	}

	return result.Ok(buf.Bytes())
}

func (e *BinaryEncoder) decode(data []byte) result.Result[Entry] {
	buf := bytes.NewReader(data)
	var entry Entry

	if err := binary.Read(buf, binary.LittleEndian, &entry.Index); err != nil {
		return result.Err[Entry](err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &entry.Term); err != nil {
		return result.Err[Entry](err)
	}
	var timestamp int64
	if err := binary.Read(buf, binary.LittleEndian, &timestamp); err != nil {
		return result.Err[Entry](err)
	}
	entry.Timestamp = time.Unix(0, timestamp)

	var dataLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &dataLen); err != nil {
		return result.Err[Entry](err)
	}

	entry.Data = make([]byte, dataLen)
	if _, err := buf.Read(entry.Data); err != nil {
		return result.Err[Entry](err)
	}

	if err := binary.Read(buf, binary.LittleEndian, &entry.Checksum); err != nil {
		return result.Err[Entry](err)
	}

	dataWithoutChecksum := data[:len(data)-4]
	expectedChecksum := crc32.ChecksumIEEE(dataWithoutChecksum)
	if entry.Checksum != expectedChecksum {
		return result.Err[Entry](ErrChecksumMismatch)
	}

	return result.Ok(entry)
}

type JSONEncoder struct {
	Encoder
}

type jsonEntry struct {
	Index     uint64 `json:"index"`
	Term      uint64 `json:"term"`
	Data      []byte `json:"data"`
	Checksum  uint32 `json:"checksum"`
	Timestamp int64  `json:"timestamp"`
}

func (e *JSONEncoder) encode(entry Entry) result.Result[[]byte] {
	je := jsonEntry{
		Index:     entry.Index,
		Term:      entry.Term,
		Data:      entry.Data,
		Timestamp: entry.Timestamp.UnixNano(),
	}

	dataForChecksum, err := json.Marshal(jsonEntry{
		Index:     je.Index,
		Term:      je.Term,
		Data:      je.Data,
		Timestamp: je.Timestamp,
	})
	if err != nil {
		return result.Err[[]byte](err)
	}
	je.Checksum = crc32.ChecksumIEEE(dataForChecksum)

	res, err := json.Marshal(je)
	if err != nil {
		return result.Err[[]byte](err)
	} else {
		return result.Ok(res)
	}
}

func (e *JSONEncoder) decode(data []byte) result.Result[Entry] {
	var je jsonEntry
	if err := json.Unmarshal(data, &je); err != nil {
		return result.Err[Entry](err)
	}

	checksumData, err := json.Marshal(jsonEntry{
		Index:     je.Index,
		Term:      je.Term,
		Data:      je.Data,
		Timestamp: je.Timestamp,
	})
	if err != nil {
		return result.Err[Entry](err)
	}
	expectedChecksum := crc32.ChecksumIEEE(checksumData)
	if je.Checksum != expectedChecksum {
		return result.Err[Entry](ErrChecksumMismatch)
	}

	return result.Ok(Entry{
		Index:     je.Index,
		Term:      je.Term,
		Data:      je.Data,
		Checksum:  je.Checksum,
		Timestamp: time.Unix(0, je.Timestamp),
	})
}
