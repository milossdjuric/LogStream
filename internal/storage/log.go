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
	"time"
)

const DefaultMaxSegmentBytes = 1 << 20 // 1 MiB

type Log struct {
	dir             string
	segments        []*Segment
	mu              sync.RWMutex
	maxSegmentBytes int64
}

func NewLog(dir string, maxSegmentBytes int64) (*Log, error) {
	if maxSegmentBytes <= 0 {
		maxSegmentBytes = DefaultMaxSegmentBytes
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

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
	if last.size+int64(4+len(record)) > l.maxSegmentBytes {
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

func (l *Log) LowestOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.segments) == 0 {
		return 0
	}
	return l.segments[0].baseOffset
}

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

func (l *Log) TruncateBefore(offset uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.segments) == 0 {
		return nil
	}
	var keep []*Segment
	for _, s := range l.segments {
		if s.nextOffset-1 < offset {
			if err := s.RemoveFiles(); err != nil {
				return err
			}
		} else {
			keep = append(keep, s)
		}
	}
	if len(keep) == 0 {
		s, err := newSegment(l.dir, offset)
		if err != nil {
			return err
		}
		keep = append(keep, s)
	}
	l.segments = keep
	return nil
}

func (l *Log) CleanupOldLogs(retention time.Duration) error {
	l.mu.RLock()
	if len(l.segments) == 0 {
		l.mu.RUnlock()
		return nil
	}
	cutoffTime := time.Now().Add(-retention)

	// Default: keep from the last segment (conservative)
	safeOffset := l.segments[len(l.segments)-1].baseOffset

	for _, s := range l.segments {
		// If empty or new, we stop checking and keep from here
		if s.size == 0 {
			safeOffset = s.baseOffset
			break
		}

		// Read first record to check timestamp
		rec, err := s.Read(s.baseOffset)
		if err != nil {
			// If we can't read, default to keeping from here
			safeOffset = s.baseOffset
			break
		}

		ts, _, err := DecodeRecord(rec)
		if err != nil {
			safeOffset = s.baseOffset
			break
		}

		t := time.Unix(0, ts)
		if t.After(cutoffTime) || t.Equal(cutoffTime) {
			safeOffset = s.baseOffset
			break
		}
	}
	l.mu.RUnlock()

	return l.TruncateBefore(safeOffset)
}
