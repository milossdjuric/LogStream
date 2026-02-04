package storage

import (
	"errors"
	"sync"
	"time"
)

// MemoryLog implements a simple in-memory log.
// It stores records in a slice of byte slices.
type MemoryLog struct {
	records    [][]byte
	baseOffset uint64
	mu         sync.RWMutex
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

	offset := m.baseOffset + uint64(len(m.records))
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

	if offset < m.baseOffset || offset >= m.baseOffset+uint64(len(m.records)) {
		return nil, errors.New("offset out of range")
	}

	record := m.records[offset-m.baseOffset]
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
	return m.baseOffset + uint64(len(m.records)-1), nil
}

// LowestOffset returns the lowest offset (always 0 for memory log).
func (m *MemoryLog) LowestOffset() uint64 {
	return m.baseOffset
}

// TruncateTo removes all records with offsets greater than the given offset.
// Used during view-synchronous recovery to ensure log consistency.
func (m *MemoryLog) TruncateTo(maxOffset uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.records) == 0 {
		return nil
	}

	// Calculate how many records to keep
	// If maxOffset is below baseOffset, we need to clear everything
	if maxOffset < m.baseOffset {
		m.records = make([][]byte, 0)
		return nil
	}

	// Calculate the number of records to keep
	keepCount := int(maxOffset - m.baseOffset + 1)
	if keepCount > len(m.records) {
		// Nothing to truncate
		return nil
	}

	if keepCount <= 0 {
		m.records = make([][]byte, 0)
	} else {
		m.records = m.records[:keepCount]
	}

	return nil
}

// LogEntry represents a single entry with offset and data
// Used for view-synchronous log merge during state exchange
type LogEntry struct {
	Offset    uint64
	Data      []byte
	Timestamp int64
}

// GetAllEntries returns all entries in the log with their offsets
// Used for view-synchronous state exchange to collect full log state
func (m *MemoryLog) GetAllEntries() []LogEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entries := make([]LogEntry, len(m.records))
	for i, record := range m.records {
		offset := m.baseOffset + uint64(i)
		// Decode timestamp from record if possible
		ts, _, err := DecodeRecord(record)
		if err != nil {
			ts = 0
		}
		// Make a copy of the data
		dataCopy := make([]byte, len(record))
		copy(dataCopy, record)
		entries[i] = LogEntry{
			Offset:    offset,
			Data:      dataCopy,
			Timestamp: ts,
		}
	}
	return entries
}

// AppendAtOffset appends a record at a specific offset
// Used during view-synchronous log merge to apply entries from other members
// If the offset already exists and data matches, it's a no-op
// If offset is beyond current log, fills gaps with nil
func (m *MemoryLog) AppendAtOffset(offset uint64, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// If log is empty, set base offset
	if len(m.records) == 0 {
		m.baseOffset = offset
		recCopy := make([]byte, len(data))
		copy(recCopy, data)
		m.records = append(m.records, recCopy)
		return nil
	}

	// Calculate position in slice
	if offset < m.baseOffset {
		// Offset is before our base, need to prepend
		// This is rare but can happen during merge
		newBase := offset
		gap := int(m.baseOffset - offset)
		newRecords := make([][]byte, gap+len(m.records))

		// First entry is our new data
		recCopy := make([]byte, len(data))
		copy(recCopy, data)
		newRecords[0] = recCopy

		// Copy existing records at their new positions
		copy(newRecords[gap:], m.records)

		m.records = newRecords
		m.baseOffset = newBase
		return nil
	}

	pos := int(offset - m.baseOffset)

	// If position is within existing range
	if pos < len(m.records) {
		// Entry already exists - overwrite with merged data
		// (In VS protocol, all entries at same offset should be identical)
		recCopy := make([]byte, len(data))
		copy(recCopy, data)
		m.records[pos] = recCopy
		return nil
	}

	// Position is beyond current log - extend
	// Fill gaps with empty entries
	for len(m.records) < pos {
		m.records = append(m.records, nil)
	}

	// Append the new record
	recCopy := make([]byte, len(data))
	copy(recCopy, data)
	m.records = append(m.records, recCopy)

	return nil
}

// CleanupOldLogs removes records older than the retention duration.
func (m *MemoryLog) CleanupOldLogs(retention time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.records) == 0 {
		return nil
	}

	cutoffTime := time.Now().Add(-retention)
	var removeCount int

	for _, record := range m.records {
		ts, _, err := DecodeRecord(record)
		if err != nil {
			// If we can't decode, we probably shouldn't blindly delete, but
			// in a strict system we might. Let's assume errors mean we stop checking or skip.
			// Ideally corruption is handled elsewhere.
			break
		}
		t := time.Unix(0, ts)
		if t.After(cutoffTime) {
			break
		}
		removeCount++
	}

	if removeCount > 0 {
		m.records = m.records[removeCount:]
		m.baseOffset += uint64(removeCount)
	}
	return nil
}
