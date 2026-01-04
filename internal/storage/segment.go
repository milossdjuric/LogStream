// Defines the `Segment` type and its operations for append, read, close and file removal.
package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type Segment struct {
	baseOffset uint64
	nextOffset uint64
	data       *os.File
	idx        *os.File
	size       int64
	dir        string
	mu         sync.Mutex
}

// Segment represents a contiguous range of log entries stored in a pair of
// files: a data file (.seg) containing length-prefixed records and an index
// file (.idx) that stores positions of each record within the data file.

// newSegment opens or creates the segment data and index files for the given
// baseOffset. It computes the starting nextOffset from the index file size
// (each index entry is 8 bytes) and returns an initialized Segment.
func newSegment(dir string, baseOffset uint64) (*Segment, error) {
	dataPath := filepath.Join(dir, fmt.Sprintf("%020d.seg", baseOffset))
	idxPath := filepath.Join(dir, fmt.Sprintf("%020d.idx", baseOffset))

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	data, err := os.OpenFile(dataPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	idx, err := os.OpenFile(idxPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		data.Close()
		return nil, err
	}

	// compute nextOffset using index size (each entry is 8 bytes position)
	stat, err := idx.Stat()
	if err != nil {
		data.Close()
		idx.Close()
		return nil, err
	}
	entries := stat.Size() / 8
	next := baseOffset + uint64(entries)

	dstat, err := data.Stat()
	if err != nil {
		data.Close()
		idx.Close()
		return nil, err
	}

	return &Segment{
		baseOffset: baseOffset,
		nextOffset: next,
		data:       data,
		idx:        idx,
		size:       dstat.Size(),
		dir:        dir,
	}, nil
}

// Append writes a length-prefixed record to the segment data file, records
// the position of the written record in the index file, syncs both files to
// ensure durability, and returns the logical offset assigned to the record.
func (s *Segment) Append(record []byte) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// get current end position
	pos, err := s.data.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	// write length prefix
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(record)))
	if _, err := s.data.Write(lenBuf[:]); err != nil {
		return 0, err
	}
	if _, err := s.data.Write(record); err != nil {
		return 0, err
	}

	// write index entry (position)
	var posBuf [8]byte
	binary.BigEndian.PutUint64(posBuf[:], uint64(pos))
	if _, err := s.idx.Write(posBuf[:]); err != nil {
		return 0, err
	}

	// ensure durability of writes
	if err := s.data.Sync(); err != nil {
		return 0, err
	}
	if err := s.idx.Sync(); err != nil {
		return 0, err
	}

	off := s.nextOffset
	s.nextOffset++
	s.size += int64(4 + len(record))
	return off, nil
}

// Read reads the record at the given global offset. It looks up the record's
// position from the index file (relative to the segment's baseOffset), seeks
// to that position in the data file, reads the 4-byte length prefix and then
// the record bytes, and returns them.
func (s *Segment) Read(offset uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if offset < s.baseOffset || offset >= s.nextOffset {
		return nil, fmt.Errorf("offset %d out of range [%d,%d)", offset, s.baseOffset, s.nextOffset)
	}

	rel := offset - s.baseOffset
	// read position from index
	if _, err := s.idx.Seek(int64(rel*8), io.SeekStart); err != nil {
		return nil, err
	}
	var posBuf [8]byte
	if _, err := io.ReadFull(s.idx, posBuf[:]); err != nil {
		return nil, err
	}
	pos := int64(binary.BigEndian.Uint64(posBuf[:]))

	if _, err := s.data.Seek(pos, io.SeekStart); err != nil {
		return nil, err
	}
	var lenBuf [4]byte
	if _, err := io.ReadFull(s.data, lenBuf[:]); err != nil {
		return nil, err
	}
	l := binary.BigEndian.Uint32(lenBuf[:])
	buf := make([]byte, l)
	if _, err := io.ReadFull(s.data, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// Close closes the segment's data and index files. It returns the first
// non-nil error encountered when closing the files, if any.
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var err1, err2 error
	if s.data != nil {
		err1 = s.data.Close()
		s.data = nil
	}
	if s.idx != nil {
		err2 = s.idx.Close()
		s.idx = nil
	}
	if err1 != nil {
		return err1
	}
	return err2
}

// RemoveFiles closes any open file handles and removes the segment's data and
// index files from disk. It ignores errors for missing files but returns any
// other remove errors.
func (s *Segment) RemoveFiles() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	dataPath := filepath.Join(s.dir, fmt.Sprintf("%020d.seg", s.baseOffset))
	idxPath := filepath.Join(s.dir, fmt.Sprintf("%020d.idx", s.baseOffset))
	if s.data != nil {
		s.data.Close()
		s.data = nil
	}
	if s.idx != nil {
		s.idx.Close()
		s.idx = nil
	}
	if err := os.Remove(dataPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Remove(idxPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
