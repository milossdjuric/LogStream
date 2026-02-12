package state

import (
	"fmt"
	"sync"
	"time"
)

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

	// Ring participants for consistent ring computation
	// All nodes in the same election use this list to ensure ring consistency
	ringParticipants []string
}

func NewElectionState() *ElectionState {
	return &ElectionState{
		inProgress: false,
		electionID: 0,
	}
}

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

func (es *ElectionState) UpdateCandidate(candidateID string) bool {
	es.mu.Lock()
	defer es.mu.Unlock()

	if !es.inProgress {
		return false
	}

	if candidateID > es.candidateID {
		fmt.Printf("[Election] New candidate: %s (was: %s)\n", candidateID[:8], es.candidateID[:8])
		es.candidateID = candidateID
		return true
	}

	return false
}

func (es *ElectionState) DeclareVictory(nodeID string) {
	es.mu.Lock()
	defer es.mu.Unlock()

	es.inProgress = false
	es.winnerID = nodeID
	es.isLeader = true

	fmt.Printf("[Election] VICTORY! Node %s is the new leader\n", nodeID[:8])
}

func (es *ElectionState) AcceptLeader(leaderID string) {
	es.mu.Lock()
	defer es.mu.Unlock()

	es.inProgress = false
	es.winnerID = leaderID
	es.isLeader = false

	fmt.Printf("[Election] Accepted %s as new leader\n", leaderID[:8])
}

func (es *ElectionState) IsInProgress() bool {
	es.mu.RLock()
	defer es.mu.RUnlock()
	return es.inProgress
}

func (es *ElectionState) GetCandidate() string {
	es.mu.RLock()
	defer es.mu.RUnlock()
	return es.candidateID
}

func (es *ElectionState) GetElectionID() int64 {
	es.mu.RLock()
	defer es.mu.RUnlock()
	return es.electionID
}

func (es *ElectionState) IsLeaderNode() bool {
	es.mu.RLock()
	defer es.mu.RUnlock()
	return es.isLeader
}

func (es *ElectionState) GetLeader() string {
	es.mu.RLock()
	defer es.mu.RUnlock()
	return es.winnerID
}

func (es *ElectionState) Reset() {
	es.mu.Lock()
	defer es.mu.Unlock()

	es.inProgress = false
	es.electionID = 0
	es.candidateID = ""
	es.winnerID = ""
	es.ringParticipants = nil

	fmt.Printf("[Election] State reset\n")
}

func (es *ElectionState) IsStuck() bool {
	es.mu.RLock()
	defer es.mu.RUnlock()

	if !es.inProgress {
		return false
	}

	if time.Since(es.startTime) > 60*time.Second {
		return true
	}

	return false
}

func (es *ElectionState) GetStartTime() time.Time {
	es.mu.RLock()
	defer es.mu.RUnlock()
	return es.startTime
}

func (es *ElectionState) SetRingParticipants(participants []string) {
	es.mu.Lock()
	defer es.mu.Unlock()

	es.ringParticipants = make([]string, len(participants))
	copy(es.ringParticipants, participants)

	fmt.Printf("[Election] Set ring participants: %d nodes\n", len(participants))
}

func (es *ElectionState) GetRingParticipants() []string {
	es.mu.RLock()
	defer es.mu.RUnlock()

	if es.ringParticipants == nil {
		return nil
	}

	result := make([]string, len(es.ringParticipants))
	copy(result, es.ringParticipants)
	return result
}

