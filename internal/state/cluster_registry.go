package state

import (
	"fmt"
	"sync"
	"time"

	"github.com/milossdjuric/logstream/internal/protocol"
	"google.golang.org/protobuf/proto"
)

// Registry maintains cluster state (replicated on all nodes via multicast)
// Uses protobuf-generated types for serialization
type Registry struct {
	mu      sync.RWMutex
	brokers map[string]*protocol.BrokerInfo // key: broker ID
	seqNum  int64                           // monotonic sequence for FIFO ordering
}

// NewRegistry creates a new registry
func NewRegistry() *Registry {
	return &Registry{
		brokers: make(map[string]*protocol.BrokerInfo),
		seqNum:  0,
	}
}

// RegisterBroker adds or updates a broker in the registry
func (r *Registry) RegisterBroker(id, address string, isLeader bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.brokers[id] = &protocol.BrokerInfo{
		Id:            id,
		Address:       address,
		IsLeader:      isLeader,
		LastHeartbeat: time.Now().UnixNano(),
	}

	// Increment sequence number for replication ordering
	r.seqNum++

	fmt.Printf("[Registry] Registered broker %s at %s (leader=%v, seq=%d)\n",
		id[:8], address, isLeader, r.seqNum)

	return nil
}

// ✅ NEW: RemoveBroker removes a broker from the registry
func (r *Registry) RemoveBroker(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.brokers[id]; !exists {
		return fmt.Errorf("broker %s not found", id)
	}

	delete(r.brokers, id)
	r.seqNum++

	fmt.Printf("[Registry] Removed broker %s (seq=%d)\n", id[:8], r.seqNum)
	return nil
}

// ✅ NEW: CheckTimeouts removes brokers that haven't sent heartbeat in timeout period
// Returns list of removed broker IDs
func (r *Registry) CheckTimeouts(timeout time.Duration) []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now().UnixNano()
	removed := []string{}

	for id, broker := range r.brokers {
		lastSeen := time.Duration(now - broker.LastHeartbeat)

		if lastSeen > timeout {
			delete(r.brokers, id)
			r.seqNum++
			removed = append(removed, id)

			fmt.Printf("[Registry] Timeout: Removed broker %s (last seen: %s, seq=%d)\n",
				id[:8], lastSeen.Round(time.Second), r.seqNum)
		}
	}

	return removed
}

// UpdateHeartbeat updates the last heartbeat timestamp for a broker
func (r *Registry) UpdateHeartbeat(brokerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if broker, ok := r.brokers[brokerID]; ok {
		broker.LastHeartbeat = time.Now().UnixNano()
	}
}

// GetBroker retrieves broker info (read-only, returns a copy)
func (r *Registry) GetBroker(id string) (*protocol.BrokerInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	broker, ok := r.brokers[id]
	if !ok {
		return nil, false
	}

	// Return a copy to prevent external modification
	return proto.Clone(broker).(*protocol.BrokerInfo), true
}

// GetBrokerCount returns the number of registered brokers
func (r *Registry) GetBrokerCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.brokers)
}

// GetSequenceNum returns the current registry sequence number
func (r *Registry) GetSequenceNum() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.seqNum
}

// ListBrokers returns all broker IDs
func (r *Registry) ListBrokers() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	brokerIDs := make([]string, 0, len(r.brokers))
	for id := range r.brokers {
		brokerIDs = append(brokerIDs, id)
	}
	return brokerIDs
}

// Serialize converts registry to protobuf bytes for replication
func (r *Registry) Serialize() ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	fmt.Printf("[Registry-Serialize] Starting serialization (seq=%d, brokers=%d)\n",
		r.seqNum, len(r.brokers))

	// Log broker details
	for id, broker := range r.brokers {
		fmt.Printf("[Registry-Serialize]   Broker: id=%s addr=%s leader=%v\n",
			id[:8], broker.Address, broker.IsLeader)
	}

	// Create protobuf snapshot
	snapshot := &protocol.RegistrySnapshot{
		Brokers: r.brokers, // Map is directly compatible
		SeqNum:  r.seqNum,
	}

	// Marshal to protobuf bytes
	data, err := proto.Marshal(snapshot)
	if err != nil {
		fmt.Printf("[Registry-Serialize] FAILED: %v\n", err)
		return nil, fmt.Errorf("failed to serialize registry: %w", err)
	}

	fmt.Printf("[Registry-Serialize] Success: %d bytes\n", len(data))
	fmt.Printf("[Registry-Serialize]   First 64 bytes: %x\n", data[:min(64, len(data))])

	return data, nil
}

// Deserialize updates registry from protobuf bytes (used by followers)
func (r *Registry) Deserialize(data []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	fmt.Printf("[Registry-Deserialize] Starting deserialization (%d bytes)\n", len(data))
	fmt.Printf("[Registry-Deserialize]   First 64 bytes: %x\n", data[:min(64, len(data))])

	// Unmarshal protobuf snapshot
	snapshot := &protocol.RegistrySnapshot{}
	if err := proto.Unmarshal(data, snapshot); err != nil {
		fmt.Printf("[Registry-Deserialize] FAILED: %v\n", err)
		return fmt.Errorf("failed to deserialize registry: %w", err)
	}

	fmt.Printf("[Registry-Deserialize] Parsed snapshot: seq=%d brokers=%d\n",
		snapshot.SeqNum, len(snapshot.Brokers))

	// Log received broker details
	for id, broker := range snapshot.Brokers {
		fmt.Printf("[Registry-Deserialize]   Broker: id=%s addr=%s leader=%v\n",
			id[:8], broker.Address, broker.IsLeader)
	}

	// ✅ FIX: Initialize map if nil, then copy instead of direct assignment
	if snapshot.Brokers == nil {
		r.brokers = make(map[string]*protocol.BrokerInfo)
	} else {
		// Make a copy to avoid nil assignment
		r.brokers = make(map[string]*protocol.BrokerInfo, len(snapshot.Brokers))
		for id, broker := range snapshot.Brokers {
			r.brokers[id] = broker
		}
	}

	r.seqNum = snapshot.SeqNum

	return nil
}

// PrintStatus displays current registry state (for debugging)
func (r *Registry) PrintStatus() {
	r.mu.RLock()
	defer r.mu.RUnlock()

	fmt.Println("\n=== Registry Status ===")
	fmt.Printf("Sequence Number: %d\n", r.seqNum)
	fmt.Printf("Total Brokers: %d\n", len(r.brokers))

	for id, broker := range r.brokers {
		role := "Follower"
		if broker.IsLeader {
			role = "Leader"
		}

		// Convert nanoseconds timestamp to time.Time for display
		lastSeen := time.Unix(0, broker.LastHeartbeat)
		fmt.Printf("  %s: %s [%s] (last seen: %s)\n",
			id[:8],
			broker.Address,
			role,
			time.Since(lastSeen).Round(time.Second))
	}
	fmt.Println("========================")
}

// Helper function
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
