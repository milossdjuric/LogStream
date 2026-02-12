package state

import (
	"fmt"
	"sync"
)

// ConsumerOffsetTracker tracks read offsets for each consumer per topic
type ConsumerOffsetTracker struct {
	mu      sync.RWMutex
	offsets map[string]map[string]uint64 // consumerID -> topic -> offset
}

func NewConsumerOffsetTracker() *ConsumerOffsetTracker {
	return &ConsumerOffsetTracker{
		offsets: make(map[string]map[string]uint64),
	}
}

func (t *ConsumerOffsetTracker) GetOffset(consumerID, topic string) uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if topics, exists := t.offsets[consumerID]; exists {
		return topics[topic]
	}
	return 0
}

func (t *ConsumerOffsetTracker) SetOffset(consumerID, topic string, offset uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, exists := t.offsets[consumerID]; !exists {
		t.offsets[consumerID] = make(map[string]uint64)
	}
	t.offsets[consumerID][topic] = offset
}

func (t *ConsumerOffsetTracker) RemoveConsumer(consumerID string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.offsets, consumerID)
}

// PrintStatus prints current offset tracking status
func (t *ConsumerOffsetTracker) PrintStatus() {
	t.mu.RLock()
	defer t.mu.RUnlock()

	fmt.Println("\n=== Consumer Offset Tracker ===")
	if len(t.offsets) == 0 {
		fmt.Println("No consumer offsets tracked")
		return
	}

	for consumerID, topics := range t.offsets {
		var shortID string
		if len(consumerID) > 8 {
			shortID = consumerID[:8]
		} else {
			shortID = consumerID
		}
		
		for topic, offset := range topics {
			fmt.Printf("Consumer %s - Topic %s: offset %d\n", shortID, topic, offset)
		}
	}
	fmt.Println("===============================")
}
