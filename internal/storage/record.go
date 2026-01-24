package storage

import (
	"encoding/binary"
	"errors"
)

// EncodeRecord packs a timestamp and data into a byte slice.
// Format: [Timestamp (8 bytes)][Data...]
func EncodeRecord(timestamp int64, data []byte) []byte {
	// Allocate buffer: 8 bytes for timestamp + length of data
	buf := make([]byte, 8+len(data))
	binary.BigEndian.PutUint64(buf[0:8], uint64(timestamp))
	copy(buf[8:], data)
	return buf
}

// DecodeRecord unpacks a byte slice into a timestamp and data.
func DecodeRecord(raw []byte) (int64, []byte, error) {
	if len(raw) < 8 {
		return 0, nil, errors.New("record too short to contain timestamp")
	}
	timestamp := int64(binary.BigEndian.Uint64(raw[0:8]))
	data := make([]byte, len(raw)-8)
	copy(data, raw[8:])
	return timestamp, data, nil
}
