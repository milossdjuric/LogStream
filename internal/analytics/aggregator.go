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

type TopicAnalytics struct {
	Topic          string
	TotalCount     int64
	WindowDuration time.Duration // Configured window duration
	WindowCount    int64         // Count of records in the configured window
	WindowRate     float64       // Messages per second within the configured window
}

func ComputeTopicAnalytics(topic string, log LogReader, windowDuration time.Duration) *TopicAnalytics {
	agg := NewLogAggregator(log)
	now := time.Now()
	windowStart := now.Add(-windowDuration)
	windowCount := agg.CountInWindow(windowStart, now)

	return &TopicAnalytics{
		Topic:          topic,
		TotalCount:     agg.TotalCount(),
		WindowDuration: windowDuration,
		WindowCount:    windowCount,
		WindowRate:     float64(windowCount) / windowDuration.Seconds(),
	}
}

type MultiLogAggregator struct {
	logs map[string]LogReader
}

func NewMultiLogAggregator() *MultiLogAggregator {
	return &MultiLogAggregator{
		logs: make(map[string]LogReader),
	}
}

func (m *MultiLogAggregator) AddLog(topic string, log LogReader) {
	m.logs[topic] = log
}

func (m *MultiLogAggregator) CountByPatternAcrossTopics(pattern string, topic string) int64 {
	if topic != "" {
		if log, exists := m.logs[topic]; exists {
			return NewLogAggregator(log).CountByPattern(pattern)
		}
		return 0
	}

	var total int64
	for _, log := range m.logs {
		total += NewLogAggregator(log).CountByPattern(pattern)
	}
	return total
}

func (m *MultiLogAggregator) CountInWindowAcrossTopics(start, end time.Time, topic string) int64 {
	if topic != "" {
		if log, exists := m.logs[topic]; exists {
			return NewLogAggregator(log).CountInWindow(start, end)
		}
		return 0
	}

	var total int64
	for _, log := range m.logs {
		total += NewLogAggregator(log).CountInWindow(start, end)
	}
	return total
}

func (m *MultiLogAggregator) GetAllTopicsStats(windowDuration time.Duration) map[string]*TopicAnalytics {
	stats := make(map[string]*TopicAnalytics)
	for topic, log := range m.logs {
		stats[topic] = ComputeTopicAnalytics(topic, log, windowDuration)
	}
	return stats
}
