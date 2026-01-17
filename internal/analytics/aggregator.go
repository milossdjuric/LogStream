package analytics

import (
	"strings"
	"time"

	"github.com/milossdjuric/logstream/internal/storage"
)

// LogReader works with both storage.Log and storage.MemoryLog
type LogReader interface {
	Read(offset uint64) ([]byte, error)
	HighestOffset() (uint64, error)
	LowestOffset() uint64
}

type LogAggregator struct {
	log LogReader
}

func NewLogAggregator(l LogReader) *LogAggregator {
	return &LogAggregator{log: l}
}

func (a *LogAggregator) TotalCount() int64 {
	high, err := a.log.HighestOffset()
	if err != nil {
		return 0
	}
	low := a.log.LowestOffset()
	return int64(high - low + 1)
}

func (a *LogAggregator) CountInWindow(start, end time.Time) int64 {
	high, err := a.log.HighestOffset()
	if err != nil {
		return 0
	}
	low := a.log.LowestOffset()

	var count int64
	for i := low; i <= high; i++ {
		raw, err := a.log.Read(i)
		if err != nil {
			continue
		}
		ts, _, err := storage.DecodeRecord(raw)
		if err != nil {
			continue
		}
		t := time.Unix(0, ts)
		if (t.Equal(start) || t.After(start)) && t.Before(end) {
			count++
		}
	}
	return count
}

func (a *LogAggregator) CountByPattern(pattern string) int64 {
	high, err := a.log.HighestOffset()
	if err != nil {
		return 0
	}
	low := a.log.LowestOffset()

	var count int64
	for i := low; i <= high; i++ {
		raw, err := a.log.Read(i)
		if err != nil {
			continue
		}
		// We only care about data content
		_, data, err := storage.DecodeRecord(raw)
		if err != nil {
			// Let's assume all valid records are timestamped now.
			continue
		}

		if strings.Contains(string(data), pattern) {
			count++
		}
	}
	return count
}

func (a *LogAggregator) GetRate(duration time.Duration) float64 {
	now := time.Now()
	start := now.Add(-duration)
	count := a.CountInWindow(start, now)
	return float64(count) / duration.Seconds()
}
