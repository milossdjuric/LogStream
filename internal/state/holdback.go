package state

import (
	"fmt"
	"sync"
)

// HoldbackMessage represents a registry update waiting for FIFO delivery
type HoldbackMessage struct {
	SequenceNum   int64  // Sequence number from leader
	StateSnapshot []byte // Serialized registry state
	UpdateType    string // Type of update (e.g., "REGISTRY")
}

// RegistryHoldbackQueue buffers out-of-order REPLICATE messages and delivers them in sequence.
type RegistryHoldbackQueue struct {
	mu           sync.Mutex
	nextExpected int64                        // Next sequence number we expect from leader
	buffer       map[int64]*HoldbackMessage   // Buffered out-of-order messages
	deliverFunc  func(*HoldbackMessage) error // Callback to apply the update
	initialized  bool                         // Has queue been initialized with first message?
}

// NewRegistryHoldbackQueue creates a new holdback queue
// deliverFunc is called when a message is ready to be delivered in FIFO order
func NewRegistryHoldbackQueue(deliverFunc func(*HoldbackMessage) error) *RegistryHoldbackQueue {
	return &RegistryHoldbackQueue{
		nextExpected: 1, // Default to 1, but will auto-adjust on first message
		buffer:       make(map[int64]*HoldbackMessage),
		deliverFunc:  deliverFunc,
		initialized:  false,
	}
}

// Enqueue adds a registry update message
// If it's the next expected message, deliver it immediately
// If it's out of order, buffer it until we can deliver in sequence
func (hq *RegistryHoldbackQueue) Enqueue(msg *HoldbackMessage) error {
	hq.mu.Lock()
	defer hq.mu.Unlock()

	seqNum := msg.SequenceNum

	// If this is the first message ever, initialize to this sequence
	if !hq.initialized {
		fmt.Printf("[Holdback] First message seq=%d, initializing queue\n", seqNum)
		hq.nextExpected = seqNum
		hq.initialized = true
	}

	// This is the next expected message - deliver immediately
	if seqNum == hq.nextExpected {
		fmt.Printf("[Holdback] Delivering registry update seq=%d (in order)\n", seqNum)
		// Deliver this message
		if err := hq.deliverFunc(msg); err != nil {
			return fmt.Errorf("failed to deliver message seq=%d: %w", seqNum, err)
		}
		fmt.Printf("[Holdback] Successfully delivered registry update seq=%d\n", seqNum)

		hq.nextExpected++

		// Check if we can now deliver any buffered messages
		for {
			if buffered, ok := hq.buffer[hq.nextExpected]; ok {
				// We have the next expected message in buffer - deliver it
				fmt.Printf("[Holdback] Delivering buffered registry update seq=%d\n", hq.nextExpected)
				if err := hq.deliverFunc(buffered); err != nil {
					return fmt.Errorf("failed to deliver buffered message seq=%d: %w",
						hq.nextExpected, err)
				}
				fmt.Printf("[Holdback] Successfully delivered buffered registry update seq=%d\n", hq.nextExpected)

				// Remove from buffer and advance
				delete(hq.buffer, hq.nextExpected)
				hq.nextExpected++
			} else {
				// No more consecutive messages available
				break
			}
		}

		return nil
	}

	// Future message (out of order) - buffer it
	if seqNum > hq.nextExpected {
		hq.buffer[seqNum] = msg
		fmt.Printf("[Holdback] Buffered registry update seq=%d (expected %d, buffered=%d)\n",
			seqNum, hq.nextExpected, len(hq.buffer))
		return nil
	}

	// Old/duplicate message (already delivered) - ignore
	fmt.Printf("[Holdback] Ignoring duplicate/old registry update seq=%d (expected %d)\n",
		seqNum, hq.nextExpected)

	return nil
}

func (hq *RegistryHoldbackQueue) GetNextExpected() int64 {
	hq.mu.Lock()
	defer hq.mu.Unlock()
	return hq.nextExpected
}

func (hq *RegistryHoldbackQueue) GetBufferSize() int {
	hq.mu.Lock()
	defer hq.mu.Unlock()
	return len(hq.buffer)
}

func (hq *RegistryHoldbackQueue) Reset(newExpected int64) {
	hq.mu.Lock()
	defer hq.mu.Unlock()

	hq.nextExpected = newExpected
	hq.buffer = make(map[int64]*HoldbackMessage)
	hq.initialized = true // Mark as initialized after reset

	fmt.Printf("[Holdback] Reset to sequence %d\n", newExpected)
}

func (hq *RegistryHoldbackQueue) PrintStatus() {
	hq.mu.Lock()
	defer hq.mu.Unlock()

	fmt.Printf("[Holdback] Next expected: %d, Buffered: %d messages\n",
		hq.nextExpected, len(hq.buffer))

	if len(hq.buffer) > 0 {
		fmt.Print("[Holdback] Buffer contains sequences: ")
		for seq := range hq.buffer {
			fmt.Printf("%d ", seq)
		}
		fmt.Println()
	}
}
