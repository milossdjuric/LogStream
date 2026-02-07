package node

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/milossdjuric/logstream/internal/protocol"
)

// Follower-related functions for broker node

// runFollowerDuties runs the main follower loop
func (n *Node) runFollowerDuties() {
	fmt.Printf("[Follower %s] [Follower-Duties] ========================================\n", n.id[:8])
	fmt.Printf("[Follower %s] [Follower-Duties] Starting follower duties\n", n.id[:8])
	fmt.Printf("[Follower %s] [Follower-Duties] Node ID: %s\n", n.id[:8], n.id[:8])
	fmt.Printf("[Follower %s] [Follower-Duties] Address: %s\n", n.id[:8], n.address)
	fmt.Printf("[Follower %s] [Follower-Duties] ========================================\n", n.id[:8])

	// Followers send heartbeats so leader knows they're alive
	// Increased frequency to 2 seconds to prevent timeout issues
	heartbeatTicker := time.NewTicker(2 * time.Second)
	defer heartbeatTicker.Stop()

	// Send immediate heartbeat right after joining (before waiting for first tick)
	fmt.Printf("[Follower %s] Sending immediate heartbeat to leader after joining...\n", n.id[:8])
	if err := n.sendFollowerHeartbeat(); err != nil {
		log.Printf("[Follower %s] Failed to send initial heartbeat: %v\n", n.id[:8], err)
	} else {
		fmt.Printf("[Follower %s] Initial heartbeat sent successfully\n", n.id[:8])
	}

	// Check for leader failure periodically
	failureCheckTicker := time.NewTicker(3 * time.Second)
	defer failureCheckTicker.Stop()

	checkCount := 0
	for {
		// Proactive check: if we've become leader, exit this goroutine immediately
		// This handles the case where the stop signal was missed during leader transition
		if n.IsLeader() {
			fmt.Printf("[Follower %s] Became leader - exiting follower duties goroutine\n", n.id[:8])
			return
		}

		select {
		case <-heartbeatTicker.C:
			// Send periodic heartbeat
			if err := n.sendFollowerHeartbeat(); err != nil {
				log.Printf("[Follower %s] Failed to send heartbeat: %v\n", n.id[:8], err)
			}

		case <-failureCheckTicker.C:
			checkCount++
			
			// === SIMPLIFIED TIMEOUT-BASED FAILURE DETECTION ===
			// Phi accrual detector disabled for debugging - using simple timeout
			
			leaderID := n.getLeaderIDFromRegistry()
			leaderAddr := n.leaderAddress
			
			// Check if we have leader information (either from registry or from JOIN_RESPONSE)
			if leaderID == "" && leaderAddr == "" {
				// No leader information at all - skip check
				fmt.Printf("[Follower %s] No leader information available yet, skipping failure check\n", n.id[:8])
				continue
			}
			
			// If we have leader address but not in registry yet, we can still detect timeouts
			if leaderID == "" && leaderAddr != "" {
				fmt.Printf("[Follower %s] Leader not in registry yet, but tracking heartbeats from %s\n",
					n.id[:8], leaderAddr)
				// Continue with timeout check below - we'll use leaderAddr as identifier
				leaderID = "pending-leader" // Temporary identifier for logging
			}

			// Simple timeout-based detection
			n.lastLeaderHeartbeatMu.RLock()
			timeSinceLastHeartbeat := time.Since(n.lastLeaderHeartbeat)
			n.lastLeaderHeartbeatMu.RUnlock()
			
			// Timeout thresholds (simple, no Phi complexity)
			suspicionTimeout := 10 * time.Second  // Warn after 10s
			failureTimeout := 15 * time.Second    // Fail after 15s
			
			suspected := timeSinceLastHeartbeat > suspicionTimeout && timeSinceLastHeartbeat < failureTimeout
			failed := timeSinceLastHeartbeat > failureTimeout

			// Log periodic status (every 5th check to avoid spam, ~15 seconds)
			if checkCount%5 == 0 {
				fmt.Printf("[Follower %s] ========================================\n", n.id[:8])
				fmt.Printf("[Follower %s] Timeout-based failure check #%d:\n", n.id[:8], checkCount)
				fmt.Printf("[Follower %s]   Leader ID: %s\n", n.id[:8], leaderID[:8])
				fmt.Printf("[Follower %s]   Time since last HB: %v\n", n.id[:8], timeSinceLastHeartbeat.Round(time.Second))
				fmt.Printf("[Follower %s]   Suspicion timeout: %v\n", n.id[:8], suspicionTimeout)
				fmt.Printf("[Follower %s]   Failure timeout: %v\n", n.id[:8], failureTimeout)
				fmt.Printf("[Follower %s]   Suspected: %v\n", n.id[:8], suspected)
				fmt.Printf("[Follower %s]   Failed: %v\n", n.id[:8], failed)
				fmt.Printf("[Follower %s] ========================================\n", n.id[:8])
			}

			if failed {
				// Leader has failed - simple timeout exceeded
				fmt.Printf("\n[Follower %s] ========================================\n", n.id[:8])
				fmt.Printf("[Follower %s] LEADER FAILURE DETECTED (Timeout)!\n", n.id[:8])
				fmt.Printf("[Follower %s] No heartbeat for: %v (threshold: %v)\n", n.id[:8], 
					timeSinceLastHeartbeat.Round(time.Second), failureTimeout)
				fmt.Printf("[Follower %s] Starting leader election...\n", n.id[:8])
				fmt.Printf("[Follower %s] ========================================\n\n", n.id[:8])

				// Check if election is already in progress - if so, don't start a new one
				if !n.election.IsInProgress() {
					if err := n.StartElection(); err != nil {
						fmt.Printf("[Follower %s] Failed to start election: %v\n", n.id[:8], err)
						fmt.Printf("[Follower %s] Will retry on next failure check if registry syncs\n", n.id[:8])
						// Don't log as error - this is expected if registry isn't ready
					} else {
						fmt.Printf("[Follower %s] Election started successfully\n", n.id[:8])
					}
				} else {
					// Election already in progress - check if it's stuck (timeout)
					if n.election.IsStuck() {
						// Election has been running for more than 60 seconds - it's stuck, reset it
						elapsed := time.Since(n.election.GetStartTime())
						fmt.Printf("[Follower %s] Election stuck - has been running for %v (timeout: 60s), resetting and starting new election\n",
							n.id[:8], elapsed.Round(time.Second))
						n.election.Reset()
						// Start a new election
						if err := n.StartElection(); err != nil {
							fmt.Printf("[Follower %s] Failed to start new election after reset: %v\n", n.id[:8], err)
						} else {
							fmt.Printf("[Follower %s] New election started after reset\n", n.id[:8])
						}
					} else {
						// Election in progress and not stuck - just wait for it to complete
						fmt.Printf("[Follower %s] Election already in progress, waiting for completion...\n", n.id[:8])
					}
				}

			} else if suspected {
				// Leader is suspected but not yet declared failed
				fmt.Printf("[Follower %s] Leader suspected (no HB for %v, threshold: %v)\n",
					n.id[:8], timeSinceLastHeartbeat.Round(time.Second), suspicionTimeout)
			}

		case <-n.stopFollowerDuties:
			fmt.Printf("[Follower %s] Stopping follower duties\n", n.id[:8])
			return
		}
	}
}

// sendFollowerHeartbeat sends a heartbeat to the leader via TCP unicast
// Proposal requirement: "broker nodes send periodic time-to-live messages to the leader node"
func (n *Node) sendFollowerHeartbeat() error {
	if n.IsLeader() {
		return fmt.Errorf("only followers should call this")
	}

	// Try to get leader address from stored address first (from JOIN_RESPONSE)
	// This allows heartbeats even before registry is synchronized
	leaderAddr := n.leaderAddress

	// If not available, try to get from registry (for cases where leader changed)
	if leaderAddr == "" {
		brokers := n.clusterState.ListBrokers()
		for _, brokerID := range brokers {
			if broker, ok := n.clusterState.GetBroker(brokerID); ok && broker.IsLeader {
				leaderAddr = broker.Address
				// Update stored address for future use
				n.leaderAddress = leaderAddr
				break
			}
		}
	}

	if leaderAddr == "" {
		return fmt.Errorf("leader address not found (neither stored nor in registry)")
	}

	// Send TCP unicast heartbeat to leader (proposal: TCP for critical messages)
	conn, err := net.DialTimeout("tcp", leaderAddr, 3*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to leader: %w", err)
	}
	defer conn.Close()

	// Create heartbeat message with current view number
	viewNumber := n.viewState.GetViewNumber()
	heartbeat := protocol.NewHeartbeatMsg(n.id, protocol.NodeType_BROKER, 0, viewNumber, n.address)

	// Send heartbeat
	if err := protocol.WriteTCPMessage(conn, heartbeat); err != nil {
		return fmt.Errorf("failed to send heartbeat: %w", err)
	}

	fmt.Printf("[Follower %s] -> HEARTBEAT (TCP) to leader\n", n.id[:8])
	return nil
}

