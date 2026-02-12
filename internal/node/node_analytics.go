package node

import (
	"fmt"
	"time"

	"github.com/milossdjuric/logstream/internal/analytics"
)

func (n *Node) getAnalyticsWindow() time.Duration {
	return time.Duration(n.config.AnalyticsWindowSeconds) * time.Second
}

func (n *Node) GetTopicAnalytics(topic string) (*analytics.LogAggregator, error) {
	topicLog, exists := n.GetTopicLog(topic)
	if !exists {
		return nil, fmt.Errorf("topic not found: %s", topic)
	}
	return analytics.NewLogAggregator(topicLog), nil
}

func (n *Node) GetTopicStats(topic string) (*analytics.TopicAnalytics, error) {
	topicLog, exists := n.GetTopicLog(topic)
	if !exists {
		return nil, fmt.Errorf("topic not found: %s", topic)
	}
	return analytics.ComputeTopicAnalytics(topic, topicLog, n.getAnalyticsWindow()), nil
}

func (n *Node) GetAllTopicsStats() map[string]*analytics.TopicAnalytics {
	n.dataLogsMu.RLock()
	defer n.dataLogsMu.RUnlock()

	window := n.getAnalyticsWindow()
	stats := make(map[string]*analytics.TopicAnalytics)
	for topic, topicLog := range n.dataLogs {
		stats[topic] = analytics.ComputeTopicAnalytics(topic, topicLog, window)
	}
	return stats
}

func (n *Node) GetMultiLogAggregator() *analytics.MultiLogAggregator {
	n.dataLogsMu.RLock()
	defer n.dataLogsMu.RUnlock()

	agg := analytics.NewMultiLogAggregator()
	for topic, topicLog := range n.dataLogs {
		agg.AddLog(topic, topicLog)
	}
	return agg
}

func (n *Node) CountByPattern(pattern string, topic string) int64 {
	return n.GetMultiLogAggregator().CountByPatternAcrossTopics(pattern, topic)
}

func (n *Node) CountInTimeWindow(start, end time.Time, topic string) int64 {
	return n.GetMultiLogAggregator().CountInWindowAcrossTopics(start, end, topic)
}

func (n *Node) PrintStorageStatus() {
	fmt.Printf("\n=== Storage & Analytics (window: %ds) ===\n", n.config.AnalyticsWindowSeconds)

	stats := n.GetStorageStats()
	if len(stats) == 0 {
		fmt.Printf("No topics with data\n")
		return
	}

	for topic, s := range stats {
		topicStats, _ := n.GetTopicStats(topic)
		fmt.Printf("Topic: %s\n", topic)
		fmt.Printf("  Records:     %d (offsets %d to %d)\n", s.RecordCount, s.LowestOffset, s.HighestOffset)
		if topicStats != nil {
			fmt.Printf("  Window:      %d msgs in last %ds (%.2f msg/s)\n",
				topicStats.WindowCount, n.config.AnalyticsWindowSeconds, topicStats.WindowRate)
		}
	}

	// Print consumer offset tracking
	fmt.Println()
	n.consumerOffsets.PrintStatus()
}
