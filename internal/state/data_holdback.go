package state

import (
	"fmt"
	"sync"
)

// DataHoldbackMessage represents a data chunk waiting for FIFO delivery
type DataHoldbackMessage struct {
	ProducerID  string
	Topic       string
	SequenceNum int64
	Data        []byte
	Timestamp   int64
}

// ProducerHoldback tracks sequence state for a single producer
type ProducerHoldback struct {
	nextExpected int64
	buffer       map[int64]*DataHoldbackMessage
}

// DataHoldbackQueue manages FIFO ordering for data messages per producer
type DataHoldbackQueue struct {
	mu        sync.RWMutex
	producers map[string]*ProducerHoldback
	callback  func(*DataHoldbackMessage) error
}

func NewDataHoldbackQueue(callback func(*DataHoldbackMessage) error) *DataHoldbackQueue {
	return &DataHoldbackQueue{
		producers: make(map[string]*ProducerHoldback),
		callback:  callback,
	}
}

// Enqueue adds a message to the holdback queue
func (q *DataHoldbackQueue) Enqueue(msg *DataHoldbackMessage) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	producerID := msg.ProducerID
	seqNum := msg.SequenceNum

	// Initialize producer state if needed
	// When seeing a producer for the first time, use the incoming message's sequence
	// as the starting point. This handles rejoining nodes that missed earlier messages.
	if _, exists := q.producers[producerID]; !exists {
		var shortID string
		if len(producerID) > 8 {
			shortID = producerID[:8]
		} else {
			shortID = producerID
		}
		fmt.Printf("[Holdback] New producer %s detected, starting from seq=%d\n", shortID, seqNum)
		q.producers[producerID] = &ProducerHoldback{
			nextExpected: seqNum, // Start from the first message we see, not always 1
			buffer:       make(map[int64]*DataHoldbackMessage),
		}
	}

	ph := q.producers[producerID]

	// If this is the expected sequence, deliver it and check buffer
	if seqNum == ph.nextExpected {
		// Deliver immediately
		if q.callback != nil {
			if err := q.callback(msg); err != nil {
				return fmt.Errorf("callback failed for seq %d: %w", seqNum, err)
			}
		}
		ph.nextExpected++

		// Check buffer for subsequent messages
		for {
			if buffered, exists := ph.buffer[ph.nextExpected]; exists {
				if q.callback != nil {
					if err := q.callback(buffered); err != nil {
						return fmt.Errorf("callback failed for buffered seq %d: %w", ph.nextExpected, err)
					}
				}
				delete(ph.buffer, ph.nextExpected)
				ph.nextExpected++
			} else {
				break
			}
		}
	} else if seqNum > ph.nextExpected {
		// Future message, buffer it
		ph.buffer[seqNum] = msg
		
		var shortID string
		if len(producerID) > 8 {
			shortID = producerID[:8]
		} else {
			shortID = producerID
		}
		fmt.Printf("[Holdback] Buffering out-of-order message from %s: seq=%d (expected=%d, buffered=%d)\n",
			shortID, seqNum, ph.nextExpected, len(ph.buffer))
	} else {
		// Old message, ignore (already delivered)
		var shortID string
		if len(producerID) > 8 {
			shortID = producerID[:8]
		} else {
			shortID = producerID
		}
		fmt.Printf("[Holdback] Ignoring old message from %s: seq=%d (expected=%d)\n",
			shortID, seqNum, ph.nextExpected)
	}

	return nil
}

func (q *DataHoldbackQueue) RemoveProducer(producerID string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	delete(q.producers, producerID)
}

func (q *DataHoldbackQueue) Reset(producerID string, newExpected int64) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if ph, exists := q.producers[producerID]; exists {
		ph.nextExpected = newExpected
		ph.buffer = make(map[int64]*DataHoldbackMessage)
	}
}

// ResetAll clears all producer tracking state.
// Called on view changes so stale sequence expectations don't cause holdback gaps.
// The next DATA from any producer will auto-initialize from its current sequence.
func (q *DataHoldbackQueue) ResetAll() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.producers = make(map[string]*ProducerHoldback)
}
