package state

import (
	"fmt"
	"sync"
	"time"
)

// ElectionState tracks the current election process
type ElectionState struct {
	mu sync.RWMutex

	// Election tracking
	inProgress  bool
	electionID  int64
	candidateID string
	startTime   time.Time

	// Election results
	winnerID string
	isLeader bool
}

// NewElectionState creates a new election state tracker
func NewElectionState() *ElectionState {
	return &ElectionState{
		inProgress: false,
		electionID: 0,
	}
}

// StartElection initiates a new election
func (es *ElectionState) StartElection(nodeID string, electionID int64) {
	es.mu.Lock()
	defer es.mu.Unlock()

	es.inProgress = true
	es.electionID = electionID
	es.candidateID = nodeID
	es.startTime = time.Now()
	es.winnerID = ""
	es.isLeader = false

	fmt.Printf("[Election] Started election %d with candidate %s\n", electionID, nodeID[:8])
}

// UpdateCandidate updates the current candidate if new one is higher
func (es *ElectionState) UpdateCandidate(candidateID string) bool {
	es.mu.Lock()
	defer es.mu.Unlock()

	if !es.inProgress {
		return false
	}

	// Only update if new candidate has higher ID
	if candidateID > es.candidateID {
		fmt.Printf("[Election] New candidate: %s (was: %s)\n", candidateID[:8], es.candidateID[:8])
		es.candidateID = candidateID
		return true
	}

	return false
}

// DeclareVictory marks this node as the leader
func (es *ElectionState) DeclareVictory(nodeID string) {
	es.mu.Lock()
	defer es.mu.Unlock()

	es.inProgress = false
	es.winnerID = nodeID
	es.isLeader = true

	fmt.Printf("[Election] VICTORY! Node %s is the new leader\n", nodeID[:8])
}

// AcceptLeader marks another node as the leader
func (es *ElectionState) AcceptLeader(leaderID string) {
	es.mu.Lock()
	defer es.mu.Unlock()

	es.inProgress = false
	es.winnerID = leaderID
	es.isLeader = false

	fmt.Printf("[Election] Accepted %s as new leader\n", leaderID[:8])
}

// IsInProgress returns whether an election is currently running
func (es *ElectionState) IsInProgress() bool {
	es.mu.RLock()
	defer es.mu.RUnlock()
	return es.inProgress
}

// GetCandidate returns the current candidate ID
func (es *ElectionState) GetCandidate() string {
	es.mu.RLock()
	defer es.mu.RUnlock()
	return es.candidateID
}

// GetElectionID returns the current election ID
func (es *ElectionState) GetElectionID() int64 {
	es.mu.RLock()
	defer es.mu.RUnlock()
	return es.electionID
}

// IsLeaderNode returns whether this node is the leader
func (es *ElectionState) IsLeaderNode() bool {
	es.mu.RLock()
	defer es.mu.RUnlock()
	return es.isLeader
}

// GetLeader returns the current leader ID
func (es *ElectionState) GetLeader() string {
	es.mu.RLock()
	defer es.mu.RUnlock()
	return es.winnerID
}

// Reset clears the election state
func (es *ElectionState) Reset() {
	es.mu.Lock()
	defer es.mu.Unlock()

	es.inProgress = false
	es.electionID = 0
	es.candidateID = ""
	es.winnerID = ""

	fmt.Printf("[Election] State reset\n")
}

// IsStuck checks if the election has been running for too long (60 seconds)
// Returns true if election is stuck and should be reset
func (es *ElectionState) IsStuck() bool {
	es.mu.RLock()
	defer es.mu.RUnlock()

	if !es.inProgress {
		return false
	}

	// If election has been running for more than 60 seconds, it's stuck
	if time.Since(es.startTime) > 60*time.Second {
		return true
	}

	return false
}

// GetStartTime returns when the election started (for debugging)
func (es *ElectionState) GetStartTime() time.Time {
	es.mu.RLock()
	defer es.mu.RUnlock()
	return es.startTime
}
