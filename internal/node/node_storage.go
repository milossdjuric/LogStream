package node

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/milossdjuric/logstream/internal/protocol"
	"github.com/milossdjuric/logstream/internal/state"
	"github.com/milossdjuric/logstream/internal/storage"
)

// StorageStats holds statistics for a topic's storage
type StorageStats struct {
	Topic         string
	RecordCount   uint64
	LowestOffset  uint64
	HighestOffset uint64
}

// TopicRecord represents a single record from a topic
type TopicRecord struct {
	Offset    uint64
	Timestamp int64
	Data      []byte
}

func (n *Node) getOrCreateTopicLog(topic string) *storage.MemoryLog {
	n.dataLogsMu.RLock()
	if topicLog, exists := n.dataLogs[topic]; exists {
		n.dataLogsMu.RUnlock()
		return topicLog
	}
	n.dataLogsMu.RUnlock()

	n.dataLogsMu.Lock()
	defer n.dataLogsMu.Unlock()

	if topicLog, exists := n.dataLogs[topic]; exists {
		return topicLog
	}

	topicLog := storage.NewMemoryLog()
	n.dataLogs[topic] = topicLog
	fmt.Printf("[%s] Created in-memory log for topic: %s\n", n.id[:8], topic)
	return topicLog
}

func (n *Node) GetTopicLog(topic string) (*storage.MemoryLog, bool) {
	n.dataLogsMu.RLock()
	defer n.dataLogsMu.RUnlock()
	topicLog, exists := n.dataLogs[topic]
	return topicLog, exists
}

func (n *Node) GetStorageStats() map[string]StorageStats {
	n.dataLogsMu.RLock()
	defer n.dataLogsMu.RUnlock()

	stats := make(map[string]StorageStats)
	for topic, topicLog := range n.dataLogs {
		lowest := topicLog.LowestOffset()
		highest, err := topicLog.HighestOffset()
		if err != nil {
			// Empty log
			stats[topic] = StorageStats{
				Topic:         topic,
				RecordCount:   0,
				LowestOffset:  lowest,
				HighestOffset: 0,
			}
		} else {
			stats[topic] = StorageStats{
				Topic:         topic,
				RecordCount:   highest - lowest + 1,
				LowestOffset:  lowest,
				HighestOffset: highest,
			}
		}
	}
	return stats
}

func (n *Node) ReadFromTopic(topic string, offset uint64) (int64, []byte, error) {
	n.dataLogsMu.RLock()
	topicLog, exists := n.dataLogs[topic]
	n.dataLogsMu.RUnlock()

	if !exists {
		return 0, nil, fmt.Errorf("topic not found: %s", topic)
	}

	record, err := topicLog.Read(offset)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read offset %d: %w", offset, err)
	}

	return storage.DecodeRecord(record)
}

func (n *Node) ReadRangeFromTopic(topic string, startOffset, endOffset uint64) ([]TopicRecord, error) {
	n.dataLogsMu.RLock()
	topicLog, exists := n.dataLogs[topic]
	n.dataLogsMu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("topic not found: %s", topic)
	}

	var records []TopicRecord
	for offset := startOffset; offset <= endOffset; offset++ {
		raw, err := topicLog.Read(offset)
		if err != nil {
			break // End of available data
		}

		timestamp, data, err := storage.DecodeRecord(raw)
		if err != nil {
			continue // Skip corrupted records
		}

		records = append(records, TopicRecord{
			Offset:    offset,
			Timestamp: timestamp,
			Data:      data,
		})
	}

	return records, nil
}

// handleData processes incoming DATA messages from producers
// Uses holdback queue for FIFO ordering per producer
func (n *Node) handleData(msg *protocol.DataMsg, sender *net.UDPAddr) {
	producerID := protocol.GetSenderID(msg)
	seqNum := protocol.GetSequenceNum(msg)
	topic := msg.Topic

	// Check if operations are frozen during view change - queue instead of reject
	if n.IsFrozen() {
		fmt.Printf("[%s] DATA from %s queued - operations frozen for view change\n",
			n.id[:8], producerID[:8])
		n.queueFrozenMessage("DATA", msg, nil)
		return
	}

	fmt.Printf("[%s] <- DATA seq=%d from %s (topic: %s, size: %d bytes)\n",
		n.id[:8], seqNum, producerID[:8], topic, len(msg.Data))

	n.clusterState.UpdateProducerHeartbeat(producerID)

	holdbackMsg := &state.DataHoldbackMessage{
		ProducerID:  producerID,
		Topic:       topic,
		SequenceNum: seqNum,
		Data:        msg.Data,
		Timestamp:   time.Now().UnixNano(),
	}

	if err := n.dataHoldbackQueue.Enqueue(holdbackMsg); err != nil {
		log.Printf("[%s] Failed to enqueue data in holdback: %v\n", n.id[:8], err)
		return
	}
}

// storeDataFromHoldback is called by the data holdback queue when a message
// is ready for FIFO delivery (i.e., all preceding messages have been delivered)
func (n *Node) storeDataFromHoldback(msg *state.DataHoldbackMessage) error {
	topic := msg.Topic
	seqNum := msg.SequenceNum

	topicLog := n.getOrCreateTopicLog(topic)
	encoded := storage.EncodeRecord(msg.Timestamp, msg.Data)
	offset, err := topicLog.Append(encoded)
	if err != nil {
		log.Printf("[%s] Failed to store data for topic %s: %v\n", n.id[:8], topic, err)
		return err
	}

	fmt.Printf("[%s] Stored data seq=%d at offset=%d for topic: %s (FIFO delivered)\n",
		n.id[:8], seqNum, offset, topic)

	if n.config.MaxRecordsPerTopic > 0 {
		if removed := topicLog.Compact(n.config.MaxRecordsPerTopic); removed > 0 {
			fmt.Printf("[%s] Compacted topic %s: removed %d old records (keeping %d)\n",
				n.id[:8], topic, removed, n.config.MaxRecordsPerTopic)
		}
	}

	// Log if no consumers are subscribed to this topic
	subscribers := n.clusterState.GetConsumerSubscribers(topic)
	if len(subscribers) == 0 {
		fmt.Printf("[%s] No consumers subscribed to topic: %s (data stored for future consumers)\n",
			n.id[:8], topic)
	}

	return nil
}

func (n *Node) closeAllDataLogs() {
	n.dataLogsMu.Lock()
	defer n.dataLogsMu.Unlock()

	for topic, topicLog := range n.dataLogs {
		topicLog.Close()
		fmt.Printf("[Node %s] Closed log for topic: %s\n", n.id[:8], topic)
	}
	n.dataLogs = nil
}
