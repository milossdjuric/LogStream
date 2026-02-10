package node

import (
	"fmt"
	"log"
	"net"
	"sort"
	"time"

	"github.com/milossdjuric/logstream/internal/protocol"
)

// Election-related functions for LCR (LeLann-Chang-Roberts) algorithm

// detectReachableNodesInParallel probes all nodes in parallel to determine which are reachable
// This is done BEFORE computing the ring to ensure all nodes agree on the ring topology
// Returns a list of reachable node IDs (including self)
func (n *Node) detectReachableNodesInParallel() []string {
	brokers := n.clusterState.ListBrokers()

	fmt.Printf("[Node %s] [Failure-Detection] Probing %d nodes in parallel...\n", n.id[:8], len(brokers))

	// Channel to collect reachable node IDs
	type result struct {
		id        string
		reachable bool
	}
	results := make(chan result, len(brokers))

	// Probe all nodes in parallel
	for _, nodeID := range brokers {
		go func(id string) {
			// Self is always reachable
			if id == n.id {
				results <- result{id: id, reachable: true}
				return
			}

			broker, ok := n.clusterState.GetBroker(id)
			if !ok {
				fmt.Printf("[Node %s] [Failure-Detection] Node %s not in registry, skipping\n", n.id[:8], id[:8])
				results <- result{id: id, reachable: false}
				return
			}

			// Quick connectivity test (short timeout)
			conn, err := net.DialTimeout("tcp", broker.Address, 2*time.Second)
			if err != nil {
				fmt.Printf("[Node %s] [Failure-Detection] Node %s at %s: UNREACHABLE (%v)\n",
					n.id[:8], id[:8], broker.Address, err)
				results <- result{id: id, reachable: false}
				return
			}
			conn.Close()

			fmt.Printf("[Node %s] [Failure-Detection] Node %s at %s: REACHABLE\n",
				n.id[:8], id[:8], broker.Address)
			results <- result{id: id, reachable: true}
		}(nodeID)
	}

	// Collect results with timeout
	reachable := []string{}
	timeout := time.After(3 * time.Second)

	for i := 0; i < len(brokers); i++ {
		select {
		case res := <-results:
			if res.reachable {
				reachable = append(reachable, res.id)
			}
		case <-timeout:
			fmt.Printf("[Node %s] [Failure-Detection] Timeout waiting for probe responses\n", n.id[:8])
			break
		}
	}

	fmt.Printf("[Node %s] [Failure-Detection] Reachable nodes: %d/%d\n",
		n.id[:8], len(reachable), len(brokers))

	return reachable
}

// computeRingFromFiltered computes the logical ring topology from a filtered list of reachable nodes
// This ensures all nodes that run this function with the same input will compute the same ring
func (n *Node) computeRingFromFiltered(reachableIDs []string) error {
	if len(reachableIDs) == 0 {
		return fmt.Errorf("no reachable nodes to form ring")
	}

	// Sort to create consistent ring order across all nodes
	sort.Strings(reachableIDs)

	fmt.Printf("[Node %s] [Ring-Computation] Creating ring from %d reachable nodes\n",
		n.id[:8], len(reachableIDs))

	// Find our position in the ring
	myPos := -1
	for i, id := range reachableIDs {
		if id == n.id {
			myPos = i
			break
		}
	}

	if myPos == -1 {
		return fmt.Errorf("node %s not in reachable set", n.id[:8])
	}

	// Compute next node in ring (with wraparound)
	nextPos := (myPos + 1) % len(reachableIDs)
	nextID := reachableIDs[nextPos]

	// Get next node's address
	broker, ok := n.clusterState.GetBroker(nextID)
	if !ok {
		return fmt.Errorf("next node %s not found in registry", nextID[:8])
	}

	n.nextNode = broker.Address
	n.ringPosition = myPos

	fmt.Printf("[Node %s] [Ring-Computation] Ring topology:\n", n.id[:8])
	fmt.Printf("[Node %s] [Ring-Computation]   Total nodes: %d\n", n.id[:8], len(reachableIDs))
	fmt.Printf("[Node %s] [Ring-Computation]   My position: %d/%d\n", n.id[:8], myPos+1, len(reachableIDs))
	fmt.Printf("[Node %s] [Ring-Computation]   Next node: %s at %s\n",
		n.id[:8], nextID[:8], n.nextNode)

	// Log full ring for debugging
	fmt.Printf("[Node %s] [Ring-Computation]   Ring order: ", n.id[:8])
	for i, id := range reachableIDs {
		if i == myPos {
			fmt.Printf("[%s] -> ", id[:8])
		} else {
			fmt.Printf("%s -> ", id[:8])
		}
	}
	fmt.Printf("(wrap)\n")

	return nil
}

// computeRing computes the logical ring topology from the registry
func (n *Node) computeRing() {
	brokers := n.clusterState.ListBrokers()

	if len(brokers) == 0 {
		fmt.Printf("[Ring] No brokers in registry\n")
		return
	}

	// Sort broker IDs to form consistent ring
	sort.Strings(brokers)

	// Find our position
	myPos := -1
	for i, id := range brokers {
		if id == n.id {
			myPos = i
			break
		}
	}

	if myPos == -1 {
		fmt.Printf("[Ring] ERROR: Node %s not in registry!\n", n.id[:8])
		return
	}

	n.ringPosition = myPos

	// Next node is (myPos + 1) % len(brokers) - wrap around
	nextPos := (myPos + 1) % len(brokers)
	nextID := brokers[nextPos]

	// Get next node's address
	broker, ok := n.clusterState.GetBroker(nextID)
	if !ok {
		fmt.Printf("[Ring] ERROR: Next node %s not found in registry!\n", nextID[:8])
		return
	}

	n.nextNode = broker.Address

	fmt.Printf("[Ring] Position %d/%d, Next node: %s (addr: %s)\n",
		myPos+1, len(brokers), nextID[:8], n.nextNode)
}

// tryNextAvailableNode attempts to find and connect to the next available node in the ring
// ringParticipants is the frozen list of nodes participating in this election
// Returns true if successful, false if no other nodes are available
func (n *Node) tryNextAvailableNode(electionID int64, ringParticipants []string) bool {
	fmt.Printf("[Node %s] [Election] Attempting to find next available node in ring...\n", n.id[:8])
	fmt.Printf("[Node %s] [Election] Ring participants: %d nodes\n", n.id[:8], len(ringParticipants))

	if len(ringParticipants) < 2 {
		fmt.Printf("[Node %s] [Election] Only %d node(s) in ring - cannot find next node\n", n.id[:8], len(ringParticipants))
		return false
	}

	// Sort to ensure consistent ring order
	sort.Strings(ringParticipants)

	// Find our position in the ring
	myPos := -1
	for i, id := range ringParticipants {
		if id == n.id {
			myPos = i
			break
		}
	}

	if myPos == -1 {
		fmt.Printf("[Node %s] [Election] ERROR: Node not in ring participants!\n", n.id[:8])
		return false
	}

	fmt.Printf("[Node %s] [Election] Ring has %d nodes, my position: %d/%d\n", n.id[:8], len(ringParticipants), myPos+1, len(ringParticipants))

	// Try each node in the ring starting from the immediate next one
	for offset := 1; offset < len(ringParticipants); offset++ {
		nextPos := (myPos + offset) % len(ringParticipants)
		nextID := ringParticipants[nextPos]

		// Get next node's address from registry
		broker, ok := n.clusterState.GetBroker(nextID)
		if !ok {
			fmt.Printf("[Node %s] [Election] Node %s not found in registry, skipping...\n", n.id[:8], nextID[:8])
			continue
		}

		// Try to connect to this node
		fmt.Printf("[Node %s] [Election] Trying to connect to next node in ring: %s at %s (position %d/%d)...\n",
			n.id[:8], nextID[:8], broker.Address, nextPos+1, len(ringParticipants))
		conn, err := net.DialTimeout("tcp", broker.Address, 2*time.Second)
		if err != nil {
			fmt.Printf("[Node %s] [Election] Node %s at %s is unreachable: %v, trying next in ring...\n",
				n.id[:8], nextID[:8], broker.Address, err)
			continue
		}
		conn.Close()

		// Found an available node - update nextNode and retry forwarding
		fmt.Printf("[Node %s] [Election] Found available node %s at %s in ring\n", n.id[:8], nextID[:8], broker.Address)
		n.nextNode = broker.Address

		// Retry sending the election message with ring participants
		candidateID := n.election.GetCandidate()
		if candidateID == "" {
			candidateID = n.id // Fallback to our own ID
		}
		if err := n.sendElectionMessage(candidateID, electionID, protocol.ElectionMessage_ANNOUNCE, ringParticipants); err != nil {
			fmt.Printf("[Node %s] [Election] Still failed to send to %s: %v, trying next in ring...\n", n.id[:8], broker.Address, err)
			continue
		}

		fmt.Printf("[Node %s] [Election] Successfully forwarded election message to available node %s\n", n.id[:8], nextID[:8])
		return true
	}

	// Tried all nodes in the ring - none are reachable
	fmt.Printf("[Node %s] [Election] No available nodes found in ring (tried all %d nodes)\n", n.id[:8], len(ringParticipants))
	return false
}

// StartElection initiates a new leader election using LCR algorithm with failure detection
// This implementation follows the LCR algorithm but adds a pre-election failure detection phase
// to ensure all nodes agree on which nodes are reachable before computing the ring topology
func (n *Node) StartElection() error {
	fmt.Printf("\n[Node %s] ======================================\n", n.id[:8])
	fmt.Printf("[Node %s] STARTING LEADER ELECTION (LCR + Failure Detection)\n", n.id[:8])
	fmt.Printf("[Node %s] ======================================\n\n", n.id[:8])

	// PHASE 0: Freeze operations for view-synchronous recovery
	// Per spec: "First, all brokers detect the failure and freeze operations"
	fmt.Printf("[Node %s] Phase 0: Freezing operations for view-synchronous recovery...\n", n.id[:8])
	n.freezeOperations()

	// Check if we have a synchronized registry
	brokerCount := n.clusterState.GetBrokerCount()
	fmt.Printf("[Node %s] Registry has %d brokers in frozen state\n", n.id[:8], brokerCount)

	if brokerCount < 1 {
		// No brokers at all - something is seriously wrong
		fmt.Printf("[Node %s] Cannot start election - no brokers in registry\n", n.id[:8])
		fmt.Printf("[Node %s] Resetting any existing election state\n", n.id[:8])
		n.election.Reset()
		n.unfreezeOperations() // Unfreeze since we're not proceeding
		return fmt.Errorf("registry empty: no brokers registered")
	}

	// If only 1 broker (this node), become leader immediately - no election needed
	if brokerCount == 1 {
		fmt.Printf("[Node %s] Only 1 broker in registry - becoming leader immediately (sole survivor)\n", n.id[:8])
		electionID := time.Now().UnixNano()
		n.election.StartElection(n.id, electionID)
		n.election.DeclareVictory(n.id)
		n.becomeLeaderAfterElection(electionID)
		n.unfreezeOperations()
		return nil
	}

	// PHASE 1: Detect which nodes are reachable (failure detection)
	// This is the KEY difference from standard LCR: we probe nodes BEFORE computing the ring
	// This ensures all nodes that detect the same set of reachable nodes will compute the same ring
	fmt.Printf("\n[Node %s] Phase 1: Detecting reachable nodes (parallel probing)...\n", n.id[:8])
	reachableNodes := n.detectReachableNodesInParallel()

	if len(reachableNodes) == 0 {
		fmt.Printf("[Node %s] ERROR: No reachable nodes found (not even self!)\n", n.id[:8])
		n.election.Reset()
		n.unfreezeOperations()
		return fmt.Errorf("no reachable nodes found")
	}

	if len(reachableNodes) == 1 {
		// Only this node is reachable - declare victory immediately
		fmt.Printf("[Node %s] Only 1 reachable node (me) - declaring victory immediately\n", n.id[:8])
		electionID := time.Now().UnixNano()
		n.election.StartElection(n.id, electionID)
		n.election.DeclareVictory(n.id)
		fmt.Printf("[Node %s] Becoming leader (sole survivor)...\n", n.id[:8])
		n.becomeLeaderAfterElection(electionID)
		return nil
	}

	// PHASE 2: Compute ring topology from ONLY reachable nodes
	// Since all reachable nodes detect the same set of reachable nodes,
	// they will all compute the same ring topology (assuming consistent failure detection)
	fmt.Printf("\n[Node %s] Phase 2: Computing ring topology from reachable nodes only...\n", n.id[:8])
	if err := n.computeRingFromFiltered(reachableNodes); err != nil {
		fmt.Printf("[Node %s] ERROR: Failed to compute ring from reachable nodes: %v\n", n.id[:8], err)
		n.election.Reset()
		n.unfreezeOperations()
		return fmt.Errorf("ring computation failed: %w", err)
	}

	// Verify ring computation succeeded
	if n.nextNode == "" {
		fmt.Printf("[Node %s] ERROR: Ring computation failed (nextNode is empty)\n", n.id[:8])
		n.election.Reset()
		n.unfreezeOperations()
		return fmt.Errorf("ring computation failed: nextNode is empty")
	}

	// PHASE 3: Run standard LCR algorithm on the filtered ring
	fmt.Printf("\n[Node %s] Phase 3: Starting LCR algorithm on filtered ring...\n", n.id[:8])
	electionID := time.Now().UnixNano()
	n.election.StartElection(n.id, electionID)

	// Store ring participants for this election
	n.election.SetRingParticipants(reachableNodes)

	// Send ANNOUNCE message to next node in ring with ring participants
	fmt.Printf("[Node %s] Sending ELECTION ANNOUNCE to next node: %s\n", n.id[:8], n.nextNode)
	err := n.sendElectionMessage(n.id, electionID, protocol.ElectionMessage_ANNOUNCE, reachableNodes)

	// If send fails despite pre-filtering, use fallback (node may have failed between probe and send)
	if err != nil {
		fmt.Printf("[Node %s] [Election-Start] UNEXPECTED: Send to %s failed after pre-filtering: %v\n",
			n.id[:8], n.nextNode, err)
		fmt.Printf("[Node %s] [Election-Start] Node may have failed between probe and send (race condition)\n", n.id[:8])
		fmt.Printf("[Node %s] [Election-Start] Falling back to dynamic node discovery...\n", n.id[:8])

		if n.tryNextAvailableNode(electionID, reachableNodes) {
			fmt.Printf("[Node %s] [Election-Start] Successfully found alternative node\n", n.id[:8])
			return nil
		}

		// No other nodes reachable - we must be the only one left
		fmt.Printf("[Node %s] [Election-Start] No other nodes reachable - declaring victory\n", n.id[:8])
		n.election.DeclareVictory(n.id)
		n.becomeLeaderAfterElection(electionID)
		return nil
	}

	fmt.Printf("[Node %s] Election started successfully - message sent to ring\n", n.id[:8])
	return nil
}

// sendElectionMessage sends an election message to the next node in the ring
// ringParticipants is passed through to ensure all nodes use the same ring
func (n *Node) sendElectionMessage(candidateID string, electionID int64, phase protocol.ElectionMessage_Phase, ringParticipants []string) error {
	phaseStr := "ANNOUNCE"
	if phase == protocol.ElectionMessage_VICTORY {
		phaseStr = "VICTORY"
	}

	fmt.Printf("[Node %s] [Election-Send] ========================================\n", n.id[:8])
	fmt.Printf("[Node %s] [Election-Send] Preparing to send ELECTION %s\n", n.id[:8], phaseStr)
	fmt.Printf("[Node %s] [Election-Send] Candidate: %s\n", n.id[:8], candidateID[:8])
	fmt.Printf("[Node %s] [Election-Send] Election ID: %d\n", n.id[:8], electionID)
	fmt.Printf("[Node %s] [Election-Send] Target: %s\n", n.id[:8], n.nextNode)
	fmt.Printf("[Node %s] [Election-Send] Ring participants: %d nodes\n", n.id[:8], len(ringParticipants))

	if n.nextNode == "" {
		fmt.Printf("[Node %s] [Election-Send] ERROR: next node not set\n", n.id[:8])
		return fmt.Errorf("next node not set - call computeRing() first")
	}

	// Create election message using protocol helper with ring participants
	fmt.Printf("[Node %s] [Election-Send] Creating election message...\n", n.id[:8])
	msg := protocol.NewElectionMsg(n.id, candidateID, electionID, phase, ringParticipants)
	fmt.Printf("[Node %s] [Election-Send] Message created successfully\n", n.id[:8])

	// Connect to next node via TCP
	fmt.Printf("[Node %s] [Election-Send] Connecting to %s via TCP (timeout: 5s)...\n", n.id[:8], n.nextNode)
	conn, err := net.DialTimeout("tcp", n.nextNode, 5*time.Second)
	if err != nil {
		fmt.Printf("[Node %s] [Election-Send] ERROR: Failed to connect: %v\n", n.id[:8], err)
		return fmt.Errorf("failed to connect to next node %s: %w", n.nextNode, err)
	}
	fmt.Printf("[Node %s] [Election-Send] TCP connection established to %s\n", n.id[:8], n.nextNode)
	defer func() {
		conn.Close()
		fmt.Printf("[Node %s] [Election-Send] TCP connection to %s closed\n", n.id[:8], n.nextNode)
	}()

	// Send message
	fmt.Printf("[Node %s] [Election-Send] Sending ELECTION %s message...\n", n.id[:8], phaseStr)
	if err := protocol.WriteTCPMessage(conn, msg); err != nil {
		fmt.Printf("[Node %s] [Election-Send] ERROR: Failed to send message: %v\n", n.id[:8], err)
		return fmt.Errorf("failed to send ELECTION message: %w", err)
	}

	fmt.Printf("[Node %s] [Election-Send] Successfully sent ELECTION %s (candidate: %s) to %s\n",
		n.id[:8], phaseStr, candidateID[:8], n.nextNode)
	fmt.Printf("[Node %s] [Election-Send] ========================================\n", n.id[:8])

	return nil
}

// handleElection routes election messages to the appropriate handler
func (n *Node) handleElection(msg *protocol.ElectionMsg, sender *net.UDPAddr) {
	candidateID := msg.CandidateId
	electionID := msg.ElectionId
	phase := msg.Phase
	ringParticipants := msg.RingParticipants

	senderStr := "TCP"
	if sender != nil {
		senderStr = sender.String()
	}

	fmt.Printf("\n[Node %s] [Election-Handle] ========================================\n", n.id[:8])
	fmt.Printf("[Node %s] [Election-Handle] Received ELECTION message\n", n.id[:8])
	fmt.Printf("[Node %s] [Election-Handle] Phase: %v\n", n.id[:8], phase)
	fmt.Printf("[Node %s] [Election-Handle] Candidate: %s\n", n.id[:8], candidateID[:8])
	fmt.Printf("[Node %s] [Election-Handle] Election ID: %d\n", n.id[:8], electionID)
	fmt.Printf("[Node %s] [Election-Handle] Ring participants: %d nodes\n", n.id[:8], len(ringParticipants))
	fmt.Printf("[Node %s] [Election-Handle] Sender address: %s\n", n.id[:8], senderStr)
	fmt.Printf("[Node %s] [Election-Handle] My ID: %s\n", n.id[:8], n.id[:8])
	fmt.Printf("[Node %s] [Election-Handle] ========================================\n\n", n.id[:8])

	switch phase {
	case protocol.ElectionMessage_ANNOUNCE:
		fmt.Printf("[Node %s] [Election-Handle] Processing ANNOUNCE phase...\n", n.id[:8])
		n.handleElectionAnnounce(candidateID, electionID, ringParticipants)

	case protocol.ElectionMessage_VICTORY:
		fmt.Printf("[Node %s] [Election-Handle] Processing VICTORY phase...\n", n.id[:8])
		n.handleElectionVictory(candidateID, electionID, ringParticipants)
	}
}

// handleElectionAnnounce processes ANNOUNCE phase messages
// ringParticipants is the list of nodes participating in this election (from the message)
func (n *Node) handleElectionAnnounce(candidateID string, electionID int64, ringParticipants []string) {
	fmt.Printf("[Node %s] [Election-Announce] ========================================\n", n.id[:8])
	fmt.Printf("[Node %s] [Election-Announce] Processing ANNOUNCE message\n", n.id[:8])
	fmt.Printf("[Node %s] [Election-Announce] Candidate: %s\n", n.id[:8], candidateID[:8])
	fmt.Printf("[Node %s] [Election-Announce] My ID: %s\n", n.id[:8], n.id[:8])
	fmt.Printf("[Node %s] [Election-Announce] Election ID: %d\n", n.id[:8], electionID)
	fmt.Printf("[Node %s] [Election-Announce] Ring participants from message: %d nodes\n", n.id[:8], len(ringParticipants))

	// VIEW-SYNCHRONOUS: Freeze operations when participating in election
	// All nodes must freeze during view change, not just the initiator
	if !n.IsFrozen() {
		fmt.Printf("[Node %s] [Election-Announce] Freezing operations for view-synchronous election\n", n.id[:8])
		n.freezeOperations()
	}

	// Use ring participants from the message for consistency
	// This is the key fix: all nodes use the same ring, not their own computation
	if len(ringParticipants) < 2 {
		fmt.Printf("[Node %s] [Election-Announce] WARNING: Ring participants too small (%d)\n", n.id[:8], len(ringParticipants))
		fmt.Printf("[Node %s] [Election-Announce] Ignoring election - invalid ring, unfreezing\n", n.id[:8])
		n.unfreezeOperations()
		return
	}

	// Store ring participants and compute our position in the ring from the message's list
	n.election.SetRingParticipants(ringParticipants)

	// Compute ring from the message's ring participants (NOT from registry)
	if n.nextNode == "" || true { // Always recompute to use message's ring
		fmt.Printf("[Node %s] [Election-Announce] Computing ring from message's participants...\n", n.id[:8])
		if err := n.computeRingFromFiltered(ringParticipants); err != nil {
			fmt.Printf("[Node %s] [Election-Announce] ERROR: Cannot compute ring: %v, unfreezing\n", n.id[:8], err)
			log.Printf("[Election] ERROR: Cannot compute ring: %v\n", err)
			n.unfreezeOperations()
			return
		}
		fmt.Printf("[Node %s] [Election-Announce] Ring computed, next node: %s\n", n.id[:8], n.nextNode)
	}

	// LCR Algorithm Logic:
	// - If candidate ID > my ID: Forward the message with the higher candidate
	// - If candidate ID < my ID: Replace candidate with my ID and forward
	// - If candidate ID == my ID: I win, declare victory
	fmt.Printf("[Node %s] [Election-Announce] Comparing IDs: candidate=%s, mine=%s\n", n.id[:8], candidateID[:8], n.id[:8])
	if candidateID > n.id {
		fmt.Printf("[Node %s] [Election-Announce] Candidate %s is HIGHER than %s - FORWARDING\n",
			n.id[:8], candidateID[:8], n.id[:8])
		n.election.UpdateCandidate(candidateID)

		if err := n.sendElectionMessage(candidateID, electionID, protocol.ElectionMessage_ANNOUNCE, ringParticipants); err != nil {
			fmt.Printf("[Node %s] [Election-Announce] ERROR: Failed to forward: %v\n", n.id[:8], err)
			log.Printf("[Election] Failed to forward: %v\n", err)

			// Try to find next available node in ring
			if n.tryNextAvailableNode(electionID, ringParticipants) {
				fmt.Printf("[Node %s] [Election-Announce] Successfully forwarded to next available node\n", n.id[:8])
			} else {
				// No other nodes in the ring are reachable - including the higher candidate
				// The higher candidate is dead (unreachable), so we are the sole survivor
				fmt.Printf("[Node %s] [Election-Announce] ======================================\n", n.id[:8])
				fmt.Printf("[Node %s] [Election-Announce] No nodes reachable (including candidate %s) - I WIN as sole survivor!\n", n.id[:8], candidateID[:8])
				fmt.Printf("[Node %s] [Election-Announce] ======================================\n\n", n.id[:8])

				n.election.DeclareVictory(n.id)
				n.becomeLeaderAfterElection(electionID)
			}
		} else {
			fmt.Printf("[Node %s] [Election-Announce] Successfully forwarded candidate\n", n.id[:8])
		}

	} else if candidateID < n.id {
		// LCR: Replace candidate with my ID and forward (don't drop!)
		fmt.Printf("[Node %s] [Election-Announce] Candidate %s is LOWER than %s - REPLACING with my ID and FORWARDING\n",
			n.id[:8], candidateID[:8], n.id[:8])

		// Update election state with my ID as the new candidate
		n.election.UpdateCandidate(n.id)

		// Forward the election message with my ID as the candidate
		if err := n.sendElectionMessage(n.id, electionID, protocol.ElectionMessage_ANNOUNCE, ringParticipants); err != nil {
			fmt.Printf("[Node %s] [Election-Announce] ERROR: Failed to forward with my ID: %v\n", n.id[:8], err)
			log.Printf("[Election] Failed to forward with my ID: %v\n", err)

			// Try to find next available node in ring
			if n.tryNextAvailableNode(electionID, ringParticipants) {
				fmt.Printf("[Node %s] [Election-Announce] Successfully forwarded to next available node\n", n.id[:8])
			} else {
				// No other nodes are reachable - we are the only available node
				// Since we're forwarding our own ID and no one else is available, we win!
				fmt.Printf("[Node %s] [Election-Announce] ======================================\n", n.id[:8])
				fmt.Printf("[Node %s] [Election-Announce] No other nodes in ring are reachable - I WIN!\n", n.id[:8])
				fmt.Printf("[Node %s] [Election-Announce] Tried all nodes in ring - all unreachable\n", n.id[:8])
				fmt.Printf("[Node %s] [Election-Announce] Declaring victory as the only available node\n", n.id[:8])
				fmt.Printf("[Node %s] [Election-Announce] ======================================\n\n", n.id[:8])

				n.election.DeclareVictory(n.id)
				fmt.Printf("[Node %s] [Election-Announce] Becoming leader (with view-sync recovery)...\n", n.id[:8])
				n.becomeLeaderAfterElection(electionID)
			}
		} else {
			fmt.Printf("[Node %s] [Election-Announce] Successfully forwarded with my ID %s as candidate\n", n.id[:8], n.id[:8])
		}

	} else {
		fmt.Printf("\n[Node %s] [Election-Announce] ======================================\n", n.id[:8])
		fmt.Printf("[Node %s] [Election-Announce] Message returned to me - I WIN!\n", n.id[:8])
		fmt.Printf("[Node %s] [Election-Announce] ======================================\n\n", n.id[:8])

		n.election.DeclareVictory(n.id)

		fmt.Printf("[Node %s] [Election-Announce] Sending VICTORY message...\n", n.id[:8])
		if err := n.sendElectionMessage(n.id, electionID, protocol.ElectionMessage_VICTORY, ringParticipants); err != nil {
			fmt.Printf("[Node %s] [Election-Announce] ERROR: Failed to send VICTORY: %v\n", n.id[:8], err)
			log.Printf("[Election] Failed to send VICTORY: %v\n", err)
			// If we can't send VICTORY, try next available node
			if len(ringParticipants) > 1 {
				fmt.Printf("[Node %s] [Election-Announce] Trying to send VICTORY to next available node...\n", n.id[:8])
				n.tryNextAvailableNode(electionID, ringParticipants)
			}
		} else {
			fmt.Printf("[Node %s] [Election-Announce] VICTORY message sent successfully\n", n.id[:8])
		}

		fmt.Printf("[Node %s] [Election-Announce] Becoming leader (with view-sync recovery)...\n", n.id[:8])
		n.becomeLeaderAfterElection(electionID)
	}
	fmt.Printf("[Node %s] [Election-Announce] ========================================\n", n.id[:8])
}

// handleElectionVictory processes VICTORY phase messages
// ringParticipants is the list of nodes participating in this election (from the message)
func (n *Node) handleElectionVictory(leaderID string, electionID int64, ringParticipants []string) {
	fmt.Printf("\n[Node %s] [Election-Victory] ========================================\n", n.id[:8])
	fmt.Printf("[Node %s] [Election-Victory] Processing VICTORY message\n", n.id[:8])
	fmt.Printf("[Node %s] [Election-Victory] New leader: %s\n", n.id[:8], leaderID[:8])
	fmt.Printf("[Node %s] [Election-Victory] Election ID: %d\n", n.id[:8], electionID)
	fmt.Printf("[Node %s] [Election-Victory] Ring participants: %d nodes\n", n.id[:8], len(ringParticipants))
	fmt.Printf("[Node %s] [Election-Victory] My ID: %s\n", n.id[:8], n.id[:8])

	// If VICTORY completed the circuit back to us (the winner), just return.
	// Do NOT freeze - we already ran becomeLeaderAfterElection which handles unfreeze
	// via installNewView's defer. Re-freezing here after installNewView unfroze would
	// leave the node permanently frozen with no unfreeze path.
	if leaderID == n.id {
		fmt.Printf("[Election] VICTORY message completed circuit back to winner\n")
		return
	}

	// VIEW-SYNCHRONOUS: Ensure we're frozen during election
	// (should already be frozen from ANNOUNCE, but ensure consistency)
	if !n.IsFrozen() {
		fmt.Printf("[Node %s] [Election-Victory] Freezing operations for view-synchronous election\n", n.id[:8])
		n.freezeOperations()
	}

	// Use ring participants from the message
	if len(ringParticipants) < 2 {
		fmt.Printf("[Node %s] [Election-Victory] WARNING: Ring participants too small (%d)\n", n.id[:8], len(ringParticipants))
		fmt.Printf("[Node %s] [Election-Victory] Ignoring VICTORY message, unfreezing\n", n.id[:8])
		n.unfreezeOperations()
		return
	}

	// Compute ring from message's participants (NOT from registry)
	if n.nextNode == "" || true { // Always use message's ring
		fmt.Printf("[Node %s] [Election-Victory] Computing ring from message's participants...\n", n.id[:8])
		if err := n.computeRingFromFiltered(ringParticipants); err != nil {
			fmt.Printf("[Node %s] [Election-Victory] ERROR: Cannot compute ring: %v, unfreezing\n", n.id[:8], err)
			log.Printf("[Election] ERROR: Cannot compute ring: %v\n", err)
			n.unfreezeOperations()
			return
		}
		fmt.Printf("[Node %s] [Election-Victory] Ring computed, next node: %s\n", n.id[:8], n.nextNode)
	}

	fmt.Printf("[Election] Accepting %s as new leader\n", leaderID[:8])
	n.election.AcceptLeader(leaderID)

	// Phi accrual detector will automatically track new leader when heartbeats arrive
	fmt.Printf("[Node %s] Phi accrual detector ready to track new leader: %s\n", n.id[:8], leaderID[:8])

	// Update stored leader address from registry
	if broker, ok := n.clusterState.GetBroker(leaderID); ok {
		n.leaderAddress = broker.Address
		fmt.Printf("[Node %s] Updated stored leader address to: %s\n", n.id[:8], n.leaderAddress)
	}

	// Forward VICTORY with ring participants
	if err := n.sendElectionMessage(leaderID, electionID, protocol.ElectionMessage_VICTORY, ringParticipants); err != nil {
		log.Printf("[Election] Failed to forward VICTORY: %v\n", err)
		// Try next available node in the ring
		n.tryNextAvailableNode(electionID, ringParticipants)
	}

	n.becomeFollower()
}
