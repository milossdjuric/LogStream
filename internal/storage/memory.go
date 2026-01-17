package storage

import (
	"errors"
	"sync"
)

// MemoryLog implements a simple in-memory log.
// It stores records in a slice of byte slices.
type MemoryLog struct {
	records [][]byte
	mu      sync.RWMutex
}

// NewMemoryLog creates a new in-memory log.
func NewMemoryLog() *MemoryLog {
	return &MemoryLog{
		records: make([][]byte, 0),
	}
}

// Append adds a record to the log and returns its offset (index).
func (m *MemoryLog) Append(record []byte) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	offset := uint64(len(m.records))
	// Copy the record to ensure safety
	recCopy := make([]byte, len(record))
	copy(recCopy, record)
	m.records = append(m.records, recCopy)

	return offset, nil
}

// Read retrieves a record at the given offset.
func (m *MemoryLog) Read(offset uint64) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if offset >= uint64(len(m.records)) {
		return nil, errors.New("offset out of range")
	}

	record := m.records[offset]
	// Return a copy to prevent modification of internal state
	ret := make([]byte, len(record))
	copy(ret, record)
	return ret, nil
}

func (m *MemoryLog) Close() error {
	// Nothing to close for in-memory log
	return nil
}

func (m *MemoryLog) HighestOffset() (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.records) == 0 {
		return 0, errors.New("empty log")
	}
	return uint64(len(m.records) - 1), nil
}

// LowestOffset returns the lowest offset (always 0 for memory log).
func (m *MemoryLog) LowestOffset() uint64 {
	return 0
}
