package node

import (
	"fmt"
	"log"
	"net"
	"sort"
	"time"

	"github.com/milossdjuric/logstream/internal/protocol"
)

func (n *Node) detectReachableNodesInParallel() []string {
	brokers := n.clusterState.ListBrokers()

	fmt.Printf("[Node %s] [Failure-Detection] Probing %d nodes in parallel...\n", n.id[:8], len(brokers))

	type result struct {
		id        string
		reachable bool
	}
	results := make(chan result, len(brokers))

	for _, nodeID := range brokers {
		go func(id string) {
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

func (n *Node) computeRingFromFiltered(reachableIDs []string, ringAddrs map[string]string) error {
	if len(reachableIDs) == 0 {
		return fmt.Errorf("no reachable nodes to form ring")
	}

	sort.Strings(reachableIDs)

	fmt.Printf("[Node %s] [Ring-Computation] Creating ring from %d reachable nodes\n",
		n.id[:8], len(reachableIDs))

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

	nextPos := (myPos + 1) % len(reachableIDs)
	nextID := reachableIDs[nextPos]

	nextAddr := ""
	if broker, ok := n.clusterState.GetBroker(nextID); ok {
		nextAddr = broker.Address
	} else if ringAddrs != nil {
		if addr, hasAddr := ringAddrs[nextID]; hasAddr && addr != "" {
			nextAddr = addr
			fmt.Printf("[Node %s] [Ring-Computation] Using address from election message for %s: %s\n",
				n.id[:8], nextID[:8], addr)
		}
	}

	if nextAddr == "" {
		return fmt.Errorf("next node %s not found in registry or election message", nextID[:8])
	}

	n.nextNode = nextAddr
	n.ringPosition = myPos

	fmt.Printf("[Node %s] [Ring-Computation] Ring topology:\n", n.id[:8])
	fmt.Printf("[Node %s] [Ring-Computation]   Total nodes: %d\n", n.id[:8], len(reachableIDs))
	fmt.Printf("[Node %s] [Ring-Computation]   My position: %d/%d\n", n.id[:8], myPos+1, len(reachableIDs))
	fmt.Printf("[Node %s] [Ring-Computation]   Next node: %s at %s\n",
		n.id[:8], nextID[:8], n.nextNode)

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

func (n *Node) computeRing() {
	brokers := n.clusterState.ListBrokers()

	if len(brokers) == 0 {
		fmt.Printf("[Ring] No brokers in registry\n")
		return
	}

	sort.Strings(brokers)

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

	nextPos := (myPos + 1) % len(brokers)
	nextID := brokers[nextPos]

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
// candidateID is the candidate to forward (from the message being forwarded)
// phase is the election phase (ANNOUNCE or VICTORY)
// ringParticipants is the frozen list of nodes participating in this election
// ringAddrs maps participant IDs to their addresses
// Returns true if successful, false if no other nodes are available
func (n *Node) tryNextAvailableNode(candidateID string, electionID int64, phase protocol.ElectionMessage_Phase, ringParticipants []string, ringAddrs map[string]string) bool {
	fmt.Printf("[Node %s] [Election] Attempting to find next available node in ring...\n", n.id[:8])
	fmt.Printf("[Node %s] [Election] Ring participants: %d nodes\n", n.id[:8], len(ringParticipants))

	if len(ringParticipants) < 2 {
		fmt.Printf("[Node %s] [Election] Only %d node(s) in ring - cannot find next node\n", n.id[:8], len(ringParticipants))
		return false
	}

	sort.Strings(ringParticipants)

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

	for offset := 1; offset < len(ringParticipants); offset++ {
		nextPos := (myPos + offset) % len(ringParticipants)
		nextID := ringParticipants[nextPos]

		nodeAddr := ""
		if broker, ok := n.clusterState.GetBroker(nextID); ok {
			nodeAddr = broker.Address
		} else if ringAddrs != nil {
			if addr, hasAddr := ringAddrs[nextID]; hasAddr && addr != "" {
				nodeAddr = addr
			}
		}
		if nodeAddr == "" {
			fmt.Printf("[Node %s] [Election] Node %s has no known address, skipping...\n", n.id[:8], nextID[:8])
			continue
		}

		fmt.Printf("[Node %s] [Election] Trying to connect to next node in ring: %s at %s (position %d/%d)...\n",
			n.id[:8], nextID[:8], nodeAddr, nextPos+1, len(ringParticipants))
		conn, err := net.DialTimeout("tcp", nodeAddr, 2*time.Second)
		if err != nil {
			fmt.Printf("[Node %s] [Election] Node %s at %s is unreachable: %v, trying next in ring...\n",
				n.id[:8], nextID[:8], nodeAddr, err)
			continue
		}
		conn.Close()

		fmt.Printf("[Node %s] [Election] Found available node %s at %s in ring\n", n.id[:8], nextID[:8], nodeAddr)
		n.nextNode = nodeAddr

		if err := n.sendElectionMessage(candidateID, electionID, phase, ringParticipants, ringAddrs); err != nil {
			fmt.Printf("[Node %s] [Election] Still failed to send to %s: %v, trying next in ring...\n", n.id[:8], nodeAddr, err)
			continue
		}

		fmt.Printf("[Node %s] [Election] Successfully forwarded election message to available node %s\n", n.id[:8], nextID[:8])
		return true
	}

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

	n.freezeOperations()

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

	if brokerCount == 1 {
		fmt.Printf("[Node %s] Only 1 broker in registry - becoming leader immediately (sole survivor)\n", n.id[:8])
		electionID := time.Now().UnixNano()
		n.election.StartElection(n.id, electionID)
		n.election.DeclareVictory(n.id)
		n.becomeLeaderAfterElection(electionID)
		n.unfreezeOperations()
		return nil
	}

	reachableNodes := n.detectReachableNodesInParallel()

	if len(reachableNodes) == 0 {
		fmt.Printf("[Node %s] ERROR: No reachable nodes found (not even self!)\n", n.id[:8])
		n.election.Reset()
		n.unfreezeOperations()
		return fmt.Errorf("no reachable nodes found")
	}

	if len(reachableNodes) == 1 {
		fmt.Printf("[Node %s] Only 1 reachable node (me) - declaring victory immediately\n", n.id[:8])
		electionID := time.Now().UnixNano()
		n.election.StartElection(n.id, electionID)
		n.election.DeclareVictory(n.id)
		fmt.Printf("[Node %s] Becoming leader (sole survivor)...\n", n.id[:8])
		n.becomeLeaderAfterElection(electionID)
		return nil
	}

	if err := n.computeRingFromFiltered(reachableNodes, nil); err != nil {
		fmt.Printf("[Node %s] ERROR: Failed to compute ring from reachable nodes: %v\n", n.id[:8], err)
		n.election.Reset()
		n.unfreezeOperations()
		return fmt.Errorf("ring computation failed: %w", err)
	}

	if n.nextNode == "" {
		fmt.Printf("[Node %s] ERROR: Ring computation failed (nextNode is empty)\n", n.id[:8])
		n.election.Reset()
		n.unfreezeOperations()
		return fmt.Errorf("ring computation failed: nextNode is empty")
	}

	electionID := time.Now().UnixNano()
	n.election.StartElection(n.id, electionID)

	n.election.SetRingParticipants(reachableNodes)

	ringAddrs := make(map[string]string)
	for _, nodeID := range reachableNodes {
		if broker, ok := n.clusterState.GetBroker(nodeID); ok {
			ringAddrs[nodeID] = broker.Address
		}
	}
	fmt.Printf("[Node %s] Built ring address map: %d entries\n", n.id[:8], len(ringAddrs))

	fmt.Printf("[Node %s] Sending ELECTION ANNOUNCE to next node: %s\n", n.id[:8], n.nextNode)
	err := n.sendElectionMessage(n.id, electionID, protocol.ElectionMessage_ANNOUNCE, reachableNodes, ringAddrs)

	if err != nil {
		fmt.Printf("[Node %s] [Election-Start] UNEXPECTED: Send to %s failed after pre-filtering: %v\n",
			n.id[:8], n.nextNode, err)
		fmt.Printf("[Node %s] [Election-Start] Node may have failed between probe and send (race condition)\n", n.id[:8])
		fmt.Printf("[Node %s] [Election-Start] Falling back to dynamic node discovery...\n", n.id[:8])

		if n.tryNextAvailableNode(n.id, electionID, protocol.ElectionMessage_ANNOUNCE, reachableNodes, ringAddrs) {
			fmt.Printf("[Node %s] [Election-Start] Successfully found alternative node\n", n.id[:8])
			return nil
		}

		fmt.Printf("[Node %s] [Election-Start] No other nodes reachable - declaring victory\n", n.id[:8])
		n.election.DeclareVictory(n.id)
		n.becomeLeaderAfterElection(electionID)
		return nil
	}

	fmt.Printf("[Node %s] Election started successfully - message sent to ring\n", n.id[:8])

	// Safety timeout: if the ANNOUNCE never circles back (e.g., next node crashes
	// after receiving it, or drops it), the initiator would be stuck forever —
	// frozen, with IsInProgress=true blocking any new election.
	// After 30s, reset election state and unfreeze so the node can retry.
	go func() {
		time.Sleep(30 * time.Second)
		if n.election.IsInProgress() && n.IsFrozen() {
			log.Printf("[Node %s] Election initiator safety timeout: ANNOUNCE never returned after 30s, resetting\n", n.id[:8])
			n.election.Reset()
			n.unfreezeOperations()
		}
	}()

	return nil
}

func (n *Node) sendElectionMessage(candidateID string, electionID int64, phase protocol.ElectionMessage_Phase, ringParticipants []string, ringAddrs map[string]string) error {
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

	msg := protocol.NewElectionMsg(n.id, candidateID, electionID, phase, ringParticipants, ringAddrs)

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

	if err := protocol.WriteTCPMessage(conn, msg); err != nil {
		fmt.Printf("[Node %s] [Election-Send] ERROR: Failed to send message: %v\n", n.id[:8], err)
		return fmt.Errorf("failed to send ELECTION message: %w", err)
	}

	fmt.Printf("[Node %s] [Election-Send] Successfully sent ELECTION %s (candidate: %s) to %s\n",
		n.id[:8], phaseStr, candidateID[:8], n.nextNode)
	fmt.Printf("[Node %s] [Election-Send] ========================================\n", n.id[:8])

	return nil
}

func (n *Node) handleElection(msg *protocol.ElectionMsg, sender *net.UDPAddr) {
	candidateID := msg.CandidateId
	electionID := msg.ElectionId
	phase := msg.Phase
	ringParticipants := msg.RingParticipants
	ringAddrs := msg.RingParticipantAddrs

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
	fmt.Printf("[Node %s] [Election-Handle] Ring addresses: %d entries\n", n.id[:8], len(ringAddrs))
	fmt.Printf("[Node %s] [Election-Handle] Sender address: %s\n", n.id[:8], senderStr)
	fmt.Printf("[Node %s] [Election-Handle] My ID: %s\n", n.id[:8], n.id[:8])
	fmt.Printf("[Node %s] [Election-Handle] ========================================\n\n", n.id[:8])

	switch phase {
	case protocol.ElectionMessage_ANNOUNCE:
		fmt.Printf("[Node %s] [Election-Handle] Processing ANNOUNCE phase...\n", n.id[:8])
		n.handleElectionAnnounce(candidateID, electionID, ringParticipants, ringAddrs)

	case protocol.ElectionMessage_VICTORY:
		fmt.Printf("[Node %s] [Election-Handle] Processing VICTORY phase...\n", n.id[:8])
		n.handleElectionVictory(candidateID, electionID, ringParticipants, ringAddrs)
	}
}

func (n *Node) handleElectionAnnounce(candidateID string, electionID int64, ringParticipants []string, ringAddrs map[string]string) {
	fmt.Printf("[Node %s] [Election-Announce] ========================================\n", n.id[:8])
	fmt.Printf("[Node %s] [Election-Announce] Processing ANNOUNCE message\n", n.id[:8])
	fmt.Printf("[Node %s] [Election-Announce] Candidate: %s\n", n.id[:8], candidateID[:8])
	fmt.Printf("[Node %s] [Election-Announce] My ID: %s\n", n.id[:8], n.id[:8])
	fmt.Printf("[Node %s] [Election-Announce] Election ID: %d\n", n.id[:8], electionID)
	fmt.Printf("[Node %s] [Election-Announce] Ring participants from message: %d nodes\n", n.id[:8], len(ringParticipants))

	if !n.IsFrozen() {
		fmt.Printf("[Node %s] [Election-Announce] Freezing operations for view-synchronous election\n", n.id[:8])
		n.freezeOperations()
	}

	if len(ringParticipants) < 2 {
		fmt.Printf("[Node %s] [Election-Announce] WARNING: Ring participants too small (%d)\n", n.id[:8], len(ringParticipants))
		fmt.Printf("[Node %s] [Election-Announce] Ignoring election - invalid ring, unfreezing\n", n.id[:8])
		n.unfreezeOperations()
		return
	}

	n.election.SetRingParticipants(ringParticipants)

	// Always recompute from message's ring, not registry. Does not modify ClusterState.
	if n.nextNode == "" || true {
		fmt.Printf("[Node %s] [Election-Announce] Computing ring from message's participants...\n", n.id[:8])
		if err := n.computeRingFromFiltered(ringParticipants, ringAddrs); err != nil {
			fmt.Printf("[Node %s] [Election-Announce] ERROR: Cannot compute ring: %v, unfreezing\n", n.id[:8], err)
			log.Printf("[Election] ERROR: Cannot compute ring: %v\n", err)
			n.unfreezeOperations()
			return
		}
		fmt.Printf("[Node %s] [Election-Announce] Ring computed, next node: %s\n", n.id[:8], n.nextNode)
	}

	fmt.Printf("[Node %s] [Election-Announce] Comparing IDs: candidate=%s, mine=%s\n", n.id[:8], candidateID[:8], n.id[:8])
	if candidateID > n.id {
		fmt.Printf("[Node %s] [Election-Announce] Candidate %s is HIGHER than %s - FORWARDING\n",
			n.id[:8], candidateID[:8], n.id[:8])
		n.election.UpdateCandidate(candidateID)

		if err := n.sendElectionMessage(candidateID, electionID, protocol.ElectionMessage_ANNOUNCE, ringParticipants, ringAddrs); err != nil {
			fmt.Printf("[Node %s] [Election-Announce] ERROR: Failed to forward: %v\n", n.id[:8], err)
			log.Printf("[Election] Failed to forward: %v\n", err)

			if n.tryNextAvailableNode(candidateID, electionID, protocol.ElectionMessage_ANNOUNCE, ringParticipants, ringAddrs) {
				fmt.Printf("[Node %s] [Election-Announce] Successfully forwarded to next available node\n", n.id[:8])
			} else {
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
		fmt.Printf("[Node %s] [Election-Announce] Candidate %s is LOWER than %s - REPLACING with my ID and FORWARDING\n",
			n.id[:8], candidateID[:8], n.id[:8])

		n.election.UpdateCandidate(n.id)

		if err := n.sendElectionMessage(n.id, electionID, protocol.ElectionMessage_ANNOUNCE, ringParticipants, ringAddrs); err != nil {
			fmt.Printf("[Node %s] [Election-Announce] ERROR: Failed to forward with my ID: %v\n", n.id[:8], err)
			log.Printf("[Election] Failed to forward with my ID: %v\n", err)

			if n.tryNextAvailableNode(n.id, electionID, protocol.ElectionMessage_ANNOUNCE, ringParticipants, ringAddrs) {
				fmt.Printf("[Node %s] [Election-Announce] Successfully forwarded to next available node\n", n.id[:8])
			} else {
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
		if err := n.sendElectionMessage(n.id, electionID, protocol.ElectionMessage_VICTORY, ringParticipants, ringAddrs); err != nil {
			fmt.Printf("[Node %s] [Election-Announce] ERROR: Failed to send VICTORY: %v\n", n.id[:8], err)
			log.Printf("[Election] Failed to send VICTORY: %v\n", err)
			if len(ringParticipants) > 1 {
				fmt.Printf("[Node %s] [Election-Announce] Trying to send VICTORY to next available node...\n", n.id[:8])
				n.tryNextAvailableNode(n.id, electionID, protocol.ElectionMessage_VICTORY, ringParticipants, ringAddrs)
			}
		} else {
			fmt.Printf("[Node %s] [Election-Announce] VICTORY message sent successfully\n", n.id[:8])
		}

		fmt.Printf("[Node %s] [Election-Announce] Becoming leader (with view-sync recovery)...\n", n.id[:8])
		n.becomeLeaderAfterElection(electionID)
	}
	fmt.Printf("[Node %s] [Election-Announce] ========================================\n", n.id[:8])
}

func (n *Node) handleElectionVictory(leaderID string, electionID int64, ringParticipants []string, ringAddrs map[string]string) {
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

	if !n.IsFrozen() {
		fmt.Printf("[Node %s] [Election-Victory] Freezing operations for view-synchronous election\n", n.id[:8])
		n.freezeOperations()
	}

	if len(ringParticipants) < 2 {
		fmt.Printf("[Node %s] [Election-Victory] WARNING: Ring participants too small (%d)\n", n.id[:8], len(ringParticipants))
		fmt.Printf("[Node %s] [Election-Victory] Ignoring VICTORY message, unfreezing\n", n.id[:8])
		n.unfreezeOperations()
		return
	}

	// Always use message's ring. Does not modify ClusterState.
	if n.nextNode == "" || true {
		fmt.Printf("[Node %s] [Election-Victory] Computing ring from message's participants...\n", n.id[:8])
		if err := n.computeRingFromFiltered(ringParticipants, ringAddrs); err != nil {
			fmt.Printf("[Node %s] [Election-Victory] ERROR: Cannot compute ring: %v, unfreezing\n", n.id[:8], err)
			log.Printf("[Election] ERROR: Cannot compute ring: %v\n", err)
			n.unfreezeOperations()
			return
		}
		fmt.Printf("[Node %s] [Election-Victory] Ring computed, next node: %s\n", n.id[:8], n.nextNode)
	}

	fmt.Printf("[Election] Accepting %s as new leader\n", leaderID[:8])
	n.election.AcceptLeader(leaderID)

	// Clear stale address so VIEW_INSTALL sets the correct one.
	if broker, ok := n.clusterState.GetBroker(leaderID); ok {
		n.leaderAddress = broker.Address
		fmt.Printf("[Node %s] Updated stored leader address to: %s\n", n.id[:8], n.leaderAddress)
	} else {
		// New leader not in our registry yet - clear stale address.
		// The upcoming VIEW_INSTALL from the new leader will set the correct address.
		fmt.Printf("[Node %s] New leader %s not in registry, clearing stale leader address (%s)\n",
			n.id[:8], leaderID[:8], n.leaderAddress)
		n.leaderAddress = ""
	}

	// Forward VICTORY with ring participants and addresses
	if err := n.sendElectionMessage(leaderID, electionID, protocol.ElectionMessage_VICTORY, ringParticipants, ringAddrs); err != nil {
		log.Printf("[Election] Failed to forward VICTORY: %v\n", err)
		// Try next available node in the ring
		n.tryNextAvailableNode(leaderID, electionID, protocol.ElectionMessage_VICTORY, ringParticipants, ringAddrs)
	}

	n.becomeFollower()

	// Safety net: if VIEW_INSTALL doesn't arrive within 30s, unfreeze.
	// Normal flow: STATE_EXCHANGE + VIEW_INSTALL completes in <5s.
	// This handles edge cases where the new leader crashes or TCP fails
	// after we accepted it. The follower duties will detect the failure
	// and start a new election, but we shouldn't stay frozen while waiting.
	go func() {
		time.Sleep(30 * time.Second)
		if n.IsFrozen() && !n.IsLeader() {
			log.Printf("[Node %s] Safety unfreeze: no VIEW_INSTALL received 30s after election loss\n", n.id[:8])
			n.unfreezeOperations()
		}
	}()
}
