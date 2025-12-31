package walrus

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"time"
)

const (
	EntryHeaderSize = 8 + 8 + 4 + 8 + 4 // Index + Term + DataLen + Timestamp + Checksum
)

func encodeBinary(entry Entry) ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.LittleEndian, entry.Index); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, entry.Term); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, entry.Timestamp.UnixNano()); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(entry.Data))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(entry.Data); err != nil {
		return nil, err
	}

	checksum := crc32.ChecksumIEEE(buf.Bytes())
	if err := binary.Write(buf, binary.LittleEndian, checksum); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func decodeBinary(data []byte) (Entry, error) {
	buf := bytes.NewReader(data)
	var entry Entry

	if err := binary.Read(buf, binary.LittleEndian, &entry.Index); err != nil {
		return Entry{}, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &entry.Term); err != nil {
		return Entry{}, err
	}
	var timestamp int64
	if err := binary.Read(buf, binary.LittleEndian, &timestamp); err != nil {
		return Entry{}, err
	}
	entry.Timestamp = time.Unix(0, timestamp)

	var dataLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &dataLen); err != nil {
		return Entry{}, err
	}

	entry.Data = make([]byte, dataLen)
	if _, err := buf.Read(entry.Data); err != nil {
		return Entry{}, err
	}

	if err := binary.Read(buf, binary.LittleEndian, &entry.Checksum); err != nil {
		return Entry{}, err
	}

	dataWithoutChecksum := data[:len(data)-4]
	expectedChecksum := crc32.ChecksumIEEE(dataWithoutChecksum)
	if entry.Checksum != expectedChecksum {
		return Entry{}, ErrChecksumMismatch
	}

	return entry, nil
}

type jsonEntry struct {
	Index     uint64 `json:"index"`
	Term      uint64 `json:"term"`
	Data      []byte `json:"data"`
	Checksum  uint32 `json:"checksum"`
	Timestamp int64  `json:"timestamp"`
}

func encodeJSON(entry Entry) ([]byte, error) {
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
		return nil, err
	}
	je.Checksum = crc32.ChecksumIEEE(dataForChecksum)

	return json.Marshal(je)
}

func decodeJSON(data []byte) (Entry, error) {
	var je jsonEntry
	if err := json.Unmarshal(data, &je); err != nil {
		return Entry{}, err
	}

	checksumData, err := json.Marshal(jsonEntry{
		Index:     je.Index,
		Term:      je.Term,
		Data:      je.Data,
		Timestamp: je.Timestamp,
	})
	if err != nil {
		return Entry{}, err
	}
	expectedChecksum := crc32.ChecksumIEEE(checksumData)
	if je.Checksum != expectedChecksum {
		return Entry{}, ErrChecksumMismatch
	}

	return Entry{
		Index:     je.Index,
		Term:      je.Term,
		Data:      je.Data,
		Checksum:  je.Checksum,
		Timestamp: time.Unix(0, je.Timestamp),
	}, nil
}
