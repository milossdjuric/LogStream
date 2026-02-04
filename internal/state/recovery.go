package state

import (
	"fmt"
	"sync"
)

// RecoveryResponse tracks a broker's state during recovery
type RecoveryResponse struct {
	BrokerID       string
	LastAppliedSeq int64
	StateSnapshot  []byte
	HasState       bool
}

// ViewRecoveryManager manages view-synchronous state exchange
type ViewRecoveryManager struct {
	mu              sync.RWMutex
	inRecovery      bool
	electionID      int64
	proposedView    int64
	totalBrokers    int
	responses       map[string]*RecoveryResponse
	agreedSequence  int64
}

// NewViewRecoveryManager creates a new recovery manager
func NewViewRecoveryManager() *ViewRecoveryManager {
	return &ViewRecoveryManager{
		responses: make(map[string]*RecoveryResponse),
	}
}

// StartRecovery initiates a recovery session
func (m *ViewRecoveryManager) StartRecovery(electionID int64, proposedView int64, totalBrokers int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.inRecovery = true
	m.electionID = electionID
	m.proposedView = proposedView
	m.totalBrokers = totalBrokers
	m.responses = make(map[string]*RecoveryResponse)
	m.agreedSequence = 0

	fmt.Printf("[RecoveryManager] Started recovery for view %d (election %d, %d brokers)\n",
		proposedView, electionID, totalBrokers)
}

// AddResponse adds a broker's state response
func (m *ViewRecoveryManager) AddResponse(brokerID string, lastAppliedSeq int64, stateSnapshot []byte, hasState bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.responses[brokerID] = &RecoveryResponse{
		BrokerID:       brokerID,
		LastAppliedSeq: lastAppliedSeq,
		StateSnapshot:  stateSnapshot,
		HasState:       hasState,
	}

	var shortID string
	if len(brokerID) > 8 {
		shortID = brokerID[:8]
	} else {
		shortID = brokerID
	}

	fmt.Printf("[RecoveryManager] Received state from %s: seq=%d hasState=%v\n",
		shortID, lastAppliedSeq, hasState)
}

// HasMajority checks if we have responses from majority of brokers
func (m *ViewRecoveryManager) HasMajority() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	required := (m.totalBrokers / 2) + 1
	return len(m.responses) >= required
}

// GetResponseCount returns the number of responses received
func (m *ViewRecoveryManager) GetResponseCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.responses)
}

// GetAgreedSequence returns the agreed-upon sequence number
// Uses majority agreement: the median of all reported sequences
func (m *ViewRecoveryManager) GetAgreedSequence() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.responses) == 0 {
		return 0
	}

	// Collect all sequence numbers
	sequences := make([]int64, 0, len(m.responses))
	for _, resp := range m.responses {
		if resp.HasState {
			sequences = append(sequences, resp.LastAppliedSeq)
		}
	}

	if len(sequences) == 0 {
		return 0
	}

	// Find maximum (most up-to-date) sequence
	var maxSeq int64
	for _, seq := range sequences {
		if seq > maxSeq {
			maxSeq = seq
		}
	}

	m.agreedSequence = maxSeq

	fmt.Printf("[RecoveryManager] Agreed on sequence %d (from %d responses)\n",
		maxSeq, len(sequences))

	return maxSeq
}

// EndRecovery marks recovery as complete
func (m *ViewRecoveryManager) EndRecovery() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.inRecovery = false
	fmt.Printf("[RecoveryManager] Recovery complete for view %d\n", m.proposedView)
}

// IsInRecovery returns whether recovery is in progress
func (m *ViewRecoveryManager) IsInRecovery() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.inRecovery
}
