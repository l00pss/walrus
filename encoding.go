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
	EntryHeaderSize = 8 + 8 + 4 + 8 + 4 + 4 // Index + Term + DataLen + Timestamp + Checksum + TxIDLen
)

type Encoder interface {
	Encode(entry Entry) result.Result[[]byte]
	Decode(data []byte) result.Result[Entry]
	EncodeInPlace(entry Entry, buffer []byte) result.Result[int]
}

type ZeroCopyEncoder interface {
	DecodeZeroCopy(data []byte) result.Result[Entry]
	EstimateSize(entry Entry) int
}

type BinaryEncoder struct{}

func (e BinaryEncoder) Encode(entry Entry) result.Result[[]byte] {
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

	txIDBytes := []byte(string(entry.TransactionID))
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(txIDBytes))); err != nil {
		return result.Err[[]byte](err)
	}
	if _, err := buf.Write(txIDBytes); err != nil {
		return result.Err[[]byte](err)
	}

	checksum := crc32.ChecksumIEEE(buf.Bytes())
	if err := binary.Write(buf, binary.LittleEndian, checksum); err != nil {
		return result.Err[[]byte](err)
	}

	return result.Ok(buf.Bytes())
}

func (e BinaryEncoder) Decode(data []byte) result.Result[Entry] {
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

	var txIDLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &txIDLen); err != nil {
		return result.Err[Entry](err)
	}

	if txIDLen > 0 {
		txIDBytes := make([]byte, txIDLen)
		if _, err := buf.Read(txIDBytes); err != nil {
			return result.Err[Entry](err)
		}
		entry.TransactionID = TransactionID(string(txIDBytes))
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

func (e BinaryEncoder) EncodeInPlace(entry Entry, buffer []byte) result.Result[int] {
	if len(buffer) < e.EstimateSize(entry) {
		return result.Err[int](ErrInvalidEntry)
	}

	pos := 0

	// Write Index
	binary.LittleEndian.PutUint64(buffer[pos:], entry.Index)
	pos += 8

	// Write Term
	binary.LittleEndian.PutUint64(buffer[pos:], entry.Term)
	pos += 8

	// Write Timestamp
	binary.LittleEndian.PutUint64(buffer[pos:], uint64(entry.Timestamp.UnixNano()))
	pos += 8

	// Write Data Length
	binary.LittleEndian.PutUint32(buffer[pos:], uint32(len(entry.Data)))
	pos += 4

	// Write Data
	copy(buffer[pos:], entry.Data)
	pos += len(entry.Data)

	// Write Transaction ID
	txIDBytes := []byte(string(entry.TransactionID))
	binary.LittleEndian.PutUint32(buffer[pos:], uint32(len(txIDBytes)))
	pos += 4
	copy(buffer[pos:], txIDBytes)
	pos += len(txIDBytes)

	// Calculate and write checksum
	checksum := crc32.ChecksumIEEE(buffer[:pos])
	binary.LittleEndian.PutUint32(buffer[pos:], checksum)
	pos += 4

	return result.Ok(pos)
}

func (e BinaryEncoder) EstimateSize(entry Entry) int {
	return EntryHeaderSize + len(entry.Data) + len([]byte(string(entry.TransactionID)))
}

func (e BinaryEncoder) DecodeZeroCopy(data []byte) result.Result[Entry] {
	if len(data) < EntryHeaderSize {
		return result.Err[Entry](ErrInvalidEntry)
	}

	pos := 0
	var entry Entry

	// Read Index
	entry.Index = binary.LittleEndian.Uint64(data[pos:])
	pos += 8

	// Read Term
	entry.Term = binary.LittleEndian.Uint64(data[pos:])
	pos += 8

	// Read Timestamp
	timestamp := binary.LittleEndian.Uint64(data[pos:])
	entry.Timestamp = time.Unix(0, int64(timestamp))
	pos += 8

	// Read Data Length
	dataLen := binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	if pos+int(dataLen) > len(data) {
		return result.Err[Entry](ErrInvalidEntry)
	}

	// Zero copy data reference (no copying!)
	entry.Data = data[pos : pos+int(dataLen)]
	pos += int(dataLen)

	// Read Transaction ID Length
	if pos+4 > len(data) {
		return result.Err[Entry](ErrInvalidEntry)
	}
	txIDLen := binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	if txIDLen > 0 {
		if pos+int(txIDLen) > len(data) {
			return result.Err[Entry](ErrInvalidEntry)
		}
		entry.TransactionID = TransactionID(string(data[pos : pos+int(txIDLen)]))
		pos += int(txIDLen)
	}

	if pos+4 > len(data) {
		return result.Err[Entry](ErrInvalidEntry)
	}
	entry.Checksum = binary.LittleEndian.Uint32(data[pos:])

	expectedChecksum := crc32.ChecksumIEEE(data[:len(data)-4])
	if entry.Checksum != expectedChecksum {
		return result.Err[Entry](ErrChecksumMismatch)
	}

	return result.Ok(entry)
}

type JSONEncoder struct{}

type jsonEntry struct {
	Index         uint64 `json:"index"`
	Term          uint64 `json:"term"`
	Data          []byte `json:"data"`
	Checksum      uint32 `json:"checksum"`
	Timestamp     int64  `json:"timestamp"`
	TransactionID string `json:"transaction_id"`
}

func (e JSONEncoder) Encode(entry Entry) result.Result[[]byte] {
	je := jsonEntry{
		Index:         entry.Index,
		Term:          entry.Term,
		Data:          entry.Data,
		Timestamp:     entry.Timestamp.UnixNano(),
		TransactionID: string(entry.TransactionID),
	}

	dataForChecksum, err := json.Marshal(jsonEntry{
		Index:         je.Index,
		Term:          je.Term,
		Data:          je.Data,
		Timestamp:     je.Timestamp,
		TransactionID: je.TransactionID,
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

func (e JSONEncoder) Decode(data []byte) result.Result[Entry] {
	var je jsonEntry
	if err := json.Unmarshal(data, &je); err != nil {
		return result.Err[Entry](err)
	}

	checksumData, err := json.Marshal(jsonEntry{
		Index:         je.Index,
		Term:          je.Term,
		Data:          je.Data,
		Timestamp:     je.Timestamp,
		TransactionID: je.TransactionID,
	})
	if err != nil {
		return result.Err[Entry](err)
	}
	expectedChecksum := crc32.ChecksumIEEE(checksumData)
	if je.Checksum != expectedChecksum {
		return result.Err[Entry](ErrChecksumMismatch)
	}

	return result.Ok(Entry{
		Index:         je.Index,
		Term:          je.Term,
		Data:          je.Data,
		Checksum:      je.Checksum,
		Timestamp:     time.Unix(0, je.Timestamp),
		TransactionID: TransactionID(je.TransactionID),
	})
}

func (e JSONEncoder) EncodeInPlace(entry Entry, buffer []byte) result.Result[int] {
	regularResult := e.Encode(entry)
	if regularResult.IsErr() {
		return result.Err[int](regularResult.UnwrapErr())
	}

	data := regularResult.Unwrap()
	if len(buffer) < len(data) {
		return result.Err[int](ErrInvalidEntry)
	}

	copy(buffer, data)
	return result.Ok(len(data))
}

func (e JSONEncoder) EstimateSize(entry Entry) int {
	baseSize := 200
	return baseSize + len(entry.Data) + len([]byte(string(entry.TransactionID)))
}

func (e JSONEncoder) DecodeZeroCopy(data []byte) result.Result[Entry] {
	return e.Decode(data)
}
