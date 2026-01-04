// Package storage implements a simple persistent, segmented log. The log is
// stored as pairs of files per segment: a data file (".seg") containing
// length-prefixed records and an index file (".idx") holding positions for
// quick lookup. This file, log.go, provides the `Log` manager which
// coordinates segments, handles rotation, truncation and exposes Append/Read
// operations.
package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const DefaultMaxSegmentBytes = 1 << 20 // 1 MiB

type Log struct {
	dir             string
	segments        []*Segment
	mu              sync.RWMutex
	maxSegmentBytes int64
}

// NewLog creates or opens a log stored in the given directory
func NewLog(dir string, maxSegmentBytes int64) (*Log, error) {
	if maxSegmentBytes <= 0 {
		maxSegmentBytes = DefaultMaxSegmentBytes
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	// scan for existing segments
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var bases []uint64
	for _, e := range entries {
		name := e.Name()
		if filepath.Ext(name) == ".seg" {
			var b uint64
			_, err := fmt.Sscanf(name, "%020d.seg", &b)
			if err == nil {
				bases = append(bases, b)
			}
		}
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })

	l := &Log{dir: dir, maxSegmentBytes: maxSegmentBytes}
	for _, b := range bases {
		s, err := newSegment(dir, b)
		if err != nil {
			return nil, err
		}
		l.segments = append(l.segments, s)
	}

	if len(l.segments) == 0 {
		s, err := newSegment(dir, 0)
		if err != nil {
			return nil, err
		}
		l.segments = append(l.segments, s)
	}

	return l, nil
}

// Append writes a record to the active segment.It returns the
// assigned global offset for the record
func (l *Log) Append(record []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	last := l.segments[len(l.segments)-1]
	if last.size+int64(4+len(record)) > l.maxSegmentBytes { //If the active segment would exceed the configured maxSegmentBytes after the write, Append rotates to a new segment starting at the next offset and appends there
		// rotate
		s, err := newSegment(l.dir, last.nextOffset)
		if err != nil {
			return 0, err
		}
		l.segments = append(l.segments, s)
		last = s
	}
	return last.Append(record)
}

// Read locates the segment that contains the given global offset and usesthe segment's Read method to retrieve the record bytes
func (l *Log) Read(offset uint64) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	for _, s := range l.segments {
		if offset >= s.baseOffset && offset < s.nextOffset {
			return s.Read(offset)
		}
	}
	return nil, errors.New("offset not found")
}

// Close closes all underlying segment files and returns the first non-nil error encountered while closing (if any)
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var firstErr error
	for _, s := range l.segments {
		if err := s.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// LowestOffset returns the base offset of the first segment, or 0 if there are no segments
func (l *Log) LowestOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.segments) == 0 {
		return 0
	}
	return l.segments[0].baseOffset
}

// HighestOffset returns the highest valid record offset in the log
func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.segments) == 0 {
		return 0, errors.New("no segments")
	}
	last := l.segments[len(l.segments)-1]
	if last.nextOffset == last.baseOffset {
		return 0, errors.New("no entries")
	}
	return last.nextOffset - 1, nil
}

// TruncateBefore removes whole segments with last offset < offset
func (l *Log) TruncateBefore(offset uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.segments) == 0 {
		return nil
	}
	var keep []*Segment
	for _, s := range l.segments {
		if s.nextOffset-1 < offset {
			// drop
			if err := s.RemoveFiles(); err != nil {
				return err
			}
		} else {
			keep = append(keep, s)
		}
	}
	if len(keep) == 0 {
		// create new empty segment at offset
		s, err := newSegment(l.dir, offset)
		if err != nil {
			return err
		}
		keep = append(keep, s)
	}
	l.segments = keep
	return nil
}
