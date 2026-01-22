package state

import (
	"fmt"
	"sync"
	"time"
)

// LeaderFailureDetector monitors leader heartbeats and detects failures
type LeaderFailureDetector struct {
	mu               sync.RWMutex
	leaderID         string
	lastHeartbeat    time.Time
	suspicionTimeout time.Duration // Time before raising suspicion
	failureTimeout   time.Duration // Time before declaring failure
	suspicionRaised  bool
	suspicionTime    time.Time
}

// NewLeaderFailureDetector creates a new failure detector
func NewLeaderFailureDetector(suspicionTimeout, failureTimeout time.Duration) *LeaderFailureDetector {
	return &LeaderFailureDetector{
		leaderID:         "",
		lastHeartbeat:    time.Now(),
		suspicionTimeout: suspicionTimeout,
		failureTimeout:   failureTimeout,
		suspicionRaised:  false,
	}
}

// UpdateHeartbeat records a heartbeat from the leader
func (fd *LeaderFailureDetector) UpdateHeartbeat(leaderID string) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	// Update leader ID if changed (e.g., after election)
	if fd.leaderID != leaderID {
		fmt.Printf("[FailureDetector] New leader detected: %s (was: %s)\n",
			leaderID[:8], fd.leaderID[:8])
		fd.leaderID = leaderID
	}

	fd.lastHeartbeat = time.Now()

	// Clear suspicion if it was raised
	if fd.suspicionRaised {
		fmt.Printf("[FailureDetector] Leader %s heartbeat received, suspicion cleared\n",
			leaderID[:8])
		fd.suspicionRaised = false
	}
}

// CheckStatus checks if the leader is suspected or failed
// Returns: (suspected, failed, timeSinceLastHB)
func (fd *LeaderFailureDetector) CheckStatus() (bool, bool, time.Duration) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	if fd.leaderID == "" {
		// No leader known yet
		return false, false, 0
	}

	timeSinceLastHB := time.Since(fd.lastHeartbeat)

	// Check for failure first (more severe)
	if timeSinceLastHB > fd.failureTimeout {
		fmt.Printf("[FailureDetector] LEADER FAILURE DETECTED!\n")
		fmt.Printf("[FailureDetector]   Leader: %s\n", fd.leaderID[:8])
		fmt.Printf("[FailureDetector]   Last heartbeat: %v ago\n", timeSinceLastHB.Round(time.Second))
		fmt.Printf("[FailureDetector]   Failure threshold: %v\n", fd.failureTimeout)
		return true, true, timeSinceLastHB
	}

	// Check for suspicion
	if timeSinceLastHB > fd.suspicionTimeout {
		if !fd.suspicionRaised {
			fd.suspicionRaised = true
			fd.suspicionTime = time.Now()
			fmt.Printf("[FailureDetector] SUSPICION RAISED on leader %s\n", fd.leaderID[:8])
			fmt.Printf("[FailureDetector]   Time since last heartbeat: %v\n",
				timeSinceLastHB.Round(time.Second))
		}
		return true, false, timeSinceLastHB
	}

	return false, false, timeSinceLastHB
}

// GetLeaderID returns the current leader ID being monitored
func (fd *LeaderFailureDetector) GetLeaderID() string {
	fd.mu.RLock()
	defer fd.mu.RUnlock()
	return fd.leaderID
}

// GetLastHeartbeat returns the time of the last heartbeat
func (fd *LeaderFailureDetector) GetLastHeartbeat() time.Time {
	fd.mu.RLock()
	defer fd.mu.RUnlock()
	return fd.lastHeartbeat
}

// Reset clears the failure detector state (e.g., after election)
func (fd *LeaderFailureDetector) Reset(newLeaderID string) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	fd.leaderID = newLeaderID
	fd.lastHeartbeat = time.Now()
	fd.suspicionRaised = false

	fmt.Printf("[FailureDetector] Reset for new leader: %s\n", newLeaderID[:8])
}

// PrintStatus displays current failure detector state (for debugging)
func (fd *LeaderFailureDetector) PrintStatus() {
	fd.mu.RLock()
	defer fd.mu.RUnlock()

	if fd.leaderID == "" {
		fmt.Println("[FailureDetector] No leader being monitored")
		return
	}

	timeSinceLastHB := time.Since(fd.lastHeartbeat)

	fmt.Printf("[FailureDetector] Monitoring leader: %s\n", fd.leaderID[:8])
	fmt.Printf("[FailureDetector]   Last heartbeat: %v ago\n", timeSinceLastHB.Round(time.Second))
	fmt.Printf("[FailureDetector]   Suspicion threshold: %v\n", fd.suspicionTimeout)
	fmt.Printf("[FailureDetector]   Failure threshold: %v\n", fd.failureTimeout)
	fmt.Printf("[FailureDetector]   Suspicion raised: %v\n", fd.suspicionRaised)

	if fd.suspicionRaised {
		fmt.Printf("[FailureDetector]   Suspicion duration: %v\n",
			time.Since(fd.suspicionTime).Round(time.Second))
	}
}
