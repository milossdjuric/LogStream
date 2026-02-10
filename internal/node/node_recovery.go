package node

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/milossdjuric/logstream/internal/protocol"
	"github.com/milossdjuric/logstream/internal/state"
	"github.com/milossdjuric/logstream/internal/storage"
)

// View-Synchronous Recovery Protocol Implementation
// Based on the project specification:
// 1. All brokers detect failure and freeze operations
// 2. Brokers execute LCR election algorithm
// 3. New leader initiates state exchange protocol
// 4. Majority agreement on completed updates
// 5. State synchronization across brokers
// 6. Install new view with updated membership

// IsFrozen returns whether the node is frozen for recovery
func (n *Node) IsFrozen() bool {
	return n.viewState.IsFrozen()
}

// waitForUnfreeze blocks until operations are unfrozen or timeout expires.
// Returns true if unfrozen, false if timed out.
// Used by PRODUCE/CONSUME handlers to keep the TCP connection open during freeze
// instead of queuing (which would cause the defer to close the conn).
func (n *Node) waitForUnfreeze(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for n.IsFrozen() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(50 * time.Millisecond)
	}
	return true
}

// freezeOperations freezes all operations during view change
func (n *Node) freezeOperations() {
	fmt.Printf("[Node %s] Freezing operations for view change...\n", n.id[:8])
	n.viewState.Freeze()
}

// unfreezeOperations unfreezes operations after view installation
func (n *Node) unfreezeOperations() {
	fmt.Printf("[Node %s] Unfreezing operations after view installation...\n", n.id[:8])
	n.viewState.Unfreeze()

	// Process any queued messages
	n.processQueuedMessages()
}

// queueFrozenMessage adds a message to the frozen queue for later processing
func (n *Node) queueFrozenMessage(msgType string, msg protocol.Message, conn net.Conn) {
	n.frozenQueueMu.Lock()
	defer n.frozenQueueMu.Unlock()

	n.frozenQueue = append(n.frozenQueue, frozenMessage{
		msgType: msgType,
		msg:     msg,
		conn:    conn,
	})
	fmt.Printf("[Node %s] Queued %s message during freeze (queue size: %d)\n",
		n.id[:8], msgType, len(n.frozenQueue))
}

// processQueuedMessages processes all messages that were queued during freeze.
// Only DATA messages are queued now - PRODUCE/CONSUME handlers wait for unfreeze
// instead of queuing, to avoid the conn-close bug (defer would close the conn
// while the frozen queue still held a reference to it).
func (n *Node) processQueuedMessages() {
	n.frozenQueueMu.Lock()
	queue := n.frozenQueue
	n.frozenQueue = make([]frozenMessage, 0)
	n.frozenQueueMu.Unlock()

	if len(queue) == 0 {
		return
	}

	fmt.Printf("[Node %s] Processing %d queued messages after unfreeze...\n", n.id[:8], len(queue))

	for _, fm := range queue {
		switch fm.msgType {
		case "DATA":
			if dataMsg, ok := fm.msg.(*protocol.DataMsg); ok {
				// DATA messages are UDP - no connection to close
				go n.handleData(dataMsg, nil)
			}
		default:
			fmt.Printf("[Node %s] Unknown queued message type: %s\n", n.id[:8], fm.msgType)
		}
	}

	fmt.Printf("[Node %s] Finished dispatching %d queued messages\n", n.id[:8], len(queue))
}

// initiateStateExchange is called by the new leader after winning election
// It collects last applied sequence numbers and full log entries from all followers
// Uses view-synchronous log merge (UNION) to preserve all acknowledged data
func (n *Node) initiateStateExchange(electionID int64) error {
	if !n.IsLeader() {
		return fmt.Errorf("only leader can initiate state exchange")
	}

	fmt.Printf("\n[Leader %s] ========================================\n", n.id[:8])
	fmt.Printf("[Leader %s] Initiating view-synchronous state exchange\n", n.id[:8])
	fmt.Printf("[Leader %s] Election ID: %d\n", n.id[:8], electionID)
	fmt.Printf("[Leader %s] Using UNION-based log merge\n", n.id[:8])
	fmt.Printf("[Leader %s] ========================================\n\n", n.id[:8])

	// Get list of followers from registry
	brokers := n.clusterState.ListBrokers()
	var followers []string
	var followerAddresses []string

	for _, brokerID := range brokers {
		if brokerID == n.id {
			continue // Skip self
		}
		broker, ok := n.clusterState.GetBroker(brokerID)
		if ok {
			followers = append(followers, brokerID)
			followerAddresses = append(followerAddresses, broker.Address)
		}
	}

	// View-synchronous: Collect our own full log entries for merge
	myLogEntries := n.collectLogEntries()
	myLogOffsets := n.collectLogOffsets() // Keep for backward compatibility
	fmt.Printf("[Leader %s] Own log entries: %d topics\n", n.id[:8], len(myLogEntries))

	if len(followers) == 0 {
		fmt.Printf("[Leader %s] No followers to exchange state with, skipping state exchange\n", n.id[:8])
		// No merge needed - just use our own logs
		return n.installNewView(electionID, n.lastAppliedSeqNum, myLogOffsets, myLogEntries)
	}

	fmt.Printf("[Leader %s] Requesting state from %d followers\n", n.id[:8], len(followers))

	// Prepare new view
	proposedView := n.viewState.GetViewNumber() + 1

	// Start recovery manager
	n.recoveryManager.StartRecovery(electionID, proposedView, len(followers)+1) // +1 for leader

	// Add our own state to the responses
	snapshot, _ := n.clusterState.Serialize()
	n.recoveryManager.AddResponse(n.id, n.lastAppliedSeqNum, snapshot, true)

	// Track all responses for view-synchronous log merge
	allResponses := []stateExchangeResponse{
		{
			brokerID:       n.id,
			lastAppliedSeq: n.lastAppliedSeqNum,
			stateSnapshot:  snapshot,
			hasState:       true,
			logOffsets:     myLogOffsets,  // Deprecated
			topicLogs:      myLogEntries,  // View-synchronous
		},
	}

	// Send STATE_EXCHANGE requests to all followers in parallel
	var wg sync.WaitGroup
	responseChan := make(chan *stateExchangeResponse, len(followers))

	for i, followerID := range followers {
		wg.Add(1)
		go func(fID, fAddr string) {
			defer wg.Done()
			resp := n.sendStateExchangeRequest(fID, fAddr, electionID, proposedView)
			if resp != nil {
				responseChan <- resp
			}
		}(followerID, followerAddresses[i])
	}

	// Wait for responses with timeout
	go func() {
		wg.Wait()
		close(responseChan)
	}()

	// Collect responses with timeout
	timeout := time.After(10 * time.Second)

collectResponses:
	for {
		select {
		case resp, ok := <-responseChan:
			if !ok {
				break collectResponses
			}
			n.recoveryManager.AddResponse(resp.brokerID, resp.lastAppliedSeq, resp.stateSnapshot, resp.hasState)
			allResponses = append(allResponses, *resp)

			// Check if we have majority
			if n.recoveryManager.HasMajority() {
				fmt.Printf("[Leader %s] Received majority (%d responses), proceeding with view installation\n",
					n.id[:8], n.recoveryManager.GetResponseCount())
				break collectResponses
			}

		case <-timeout:
			fmt.Printf("[Leader %s] State exchange timeout, proceeding with %d responses\n",
				n.id[:8], n.recoveryManager.GetResponseCount())
			break collectResponses
		}
	}

	// Determine majority-agreed sequence
	agreedSeq := n.recoveryManager.GetAgreedSequence()
	fmt.Printf("[Leader %s] Majority agreed on sequence: %d\n", n.id[:8], agreedSeq)

	// View-synchronous: Merge logs using UNION (any entry in ANY log is preserved)
	// UNION ensures no acknowledged data is lost during view change
	mergedLogs := mergeAllTopicLogs(allResponses)
	fmt.Printf("[Leader %s] Merged logs from %d responses: %d topics\n", n.id[:8], len(allResponses), len(mergedLogs))

	// Apply merged logs to our own storage (we may have been missing some entries)
	n.applyMergedLogs(mergedLogs)

	// Compute log offsets from merged logs for backward compatibility
	agreedLogOffsets := computeOffsetsFromMergedLogs(mergedLogs)
	fmt.Printf("[Leader %s] Agreed log offsets (from merge): %v\n", n.id[:8], agreedLogOffsets)

	// Install the new view with merged logs
	return n.installNewView(electionID, agreedSeq, agreedLogOffsets, mergedLogs)
}

type stateExchangeResponse struct {
	brokerID       string
	lastAppliedSeq int64
	stateSnapshot  []byte
	hasState       bool
	logOffsets     map[string]uint64            // DEPRECATED: kept for backward compatibility
	topicLogs      []*protocol.TopicLogEntries  // View-synchronous: full log entries for merge
}

// sendStateExchangeRequest sends a state exchange request to a follower
func (n *Node) sendStateExchangeRequest(followerID, followerAddr string, electionID, viewNumber int64) *stateExchangeResponse {
	fmt.Printf("[Leader %s] Sending STATE_EXCHANGE to %s at %s\n", n.id[:8], followerID[:8], followerAddr)

	// Connect to follower
	conn, err := net.DialTimeout("tcp", followerAddr, 5*time.Second)
	if err != nil {
		fmt.Printf("[Leader %s] Failed to connect to %s: %v\n", n.id[:8], followerID[:8], err)
		return nil
	}
	defer conn.Close()

	// Set read/write deadlines
	conn.SetDeadline(time.Now().Add(10 * time.Second))

	// Send STATE_EXCHANGE message
	msg := protocol.NewStateExchangeMsg(n.id, electionID, viewNumber)
	if err := protocol.WriteTCPMessage(conn, msg); err != nil {
		fmt.Printf("[Leader %s] Failed to send STATE_EXCHANGE to %s: %v\n", n.id[:8], followerID[:8], err)
		return nil
	}

	// Wait for response
	respMsg, err := protocol.ReadTCPMessage(conn)
	if err != nil {
		fmt.Printf("[Leader %s] Failed to read STATE_EXCHANGE_RESPONSE from %s: %v\n", n.id[:8], followerID[:8], err)
		return nil
	}

	// Verify response type
	stateResp, ok := respMsg.(*protocol.StateExchangeResponseMsg)
	if !ok {
		fmt.Printf("[Leader %s] Unexpected response type from %s: %T\n", n.id[:8], followerID[:8], respMsg)
		return nil
	}

	return &stateExchangeResponse{
		brokerID:       followerID,
		lastAppliedSeq: stateResp.LastAppliedSeq,
		stateSnapshot:  stateResp.StateSnapshot,
		hasState:       stateResp.HasCompleteState,
		logOffsets:     stateResp.LogOffsets,  // Deprecated
		topicLogs:      stateResp.TopicLogs,   // View-synchronous: full entries
	}
}

// handleStateExchange handles STATE_EXCHANGE requests from the new leader
func (n *Node) handleStateExchange(msg *protocol.StateExchangeMsg, conn net.Conn) {
	senderID := protocol.GetSenderID(msg)
	electionID := msg.ElectionId
	viewNumber := msg.ViewNumber

	fmt.Printf("[Node %s] <- STATE_EXCHANGE from %s (election: %d, view: %d)\n",
		n.id[:8], senderID[:8], electionID, viewNumber)

	// Freeze our operations
	n.freezeOperations()

	// Prepare response with our last applied sequence
	var snapshot []byte
	hasState := false

	// Serialize our current state
	if s, err := n.clusterState.Serialize(); err == nil {
		snapshot = s
		hasState = true
	}

	// Collect log offsets for backward compatibility
	logOffsets := n.collectLogOffsets()

	// View-synchronous: Collect full log entries for proper merge
	// This ensures no acknowledged data is lost during view change
	topicLogs := n.collectLogEntries()
	fmt.Printf("[Node %s] Collected %d topic logs with full entries for view-sync merge\n", n.id[:8], len(topicLogs))

	// Send response with full log entries
	response := protocol.NewStateExchangeResponseMsg(
		n.id,
		electionID,
		n.lastAppliedSeqNum,
		snapshot,
		hasState,
		logOffsets,  // Deprecated, kept for compatibility
		topicLogs,   // View-synchronous: full entries for merge
	)

	if err := protocol.WriteTCPMessage(conn, response); err != nil {
		log.Printf("[Node %s] Failed to send STATE_EXCHANGE_RESPONSE: %v\n", n.id[:8], err)
	} else {
		fmt.Printf("[Node %s] -> STATE_EXCHANGE_RESPONSE (lastSeq: %d, hasState: %v, topics: %d)\n",
			n.id[:8], n.lastAppliedSeqNum, hasState, len(topicLogs))
	}
}

// installNewView installs a new view after state exchange or node join.
// This is the SINGLE code path for all view installations (post-election recovery and node joins).
// mergedLogs contains the UNION of all log entries for view-synchronous consistency (nil for node joins).
// Uses defer to guarantee unfreeze regardless of error path.
func (n *Node) installNewView(electionID, agreedSeq int64, agreedLogOffsets map[string]uint64, mergedLogs []*protocol.TopicLogEntries) error {
	// Guarantee unfreeze on ALL exit paths (success or error)
	defer n.unfreezeOperations()

	if !n.IsLeader() {
		return fmt.Errorf("only leader can install new view")
	}

	newViewNumber := n.viewState.GetViewNumber() + 1

	fmt.Printf("\n[Leader %s] ========================================\n", n.id[:8])
	fmt.Printf("[Leader %s] INSTALLING NEW VIEW %d\n", n.id[:8], newViewNumber)
	fmt.Printf("[Leader %s] Agreed Sequence: %d\n", n.id[:8], agreedSeq)
	fmt.Printf("[Leader %s] Merged Logs: %d topics\n", n.id[:8], len(mergedLogs))
	fmt.Printf("[Leader %s] ========================================\n\n", n.id[:8])

	// Get current membership from registry
	brokers := n.clusterState.ListBrokers()
	var memberIDs []string
	var memberAddresses []string

	for _, brokerID := range brokers {
		broker, ok := n.clusterState.GetBroker(brokerID)
		if ok {
			memberIDs = append(memberIDs, brokerID)
			memberAddresses = append(memberAddresses, broker.Address)
		}
	}

	// Serialize agreed state
	stateSnapshot, err := n.clusterState.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize state for view install: %w", err)
	}

	// Update our sequence number to agreed sequence
	if agreedSeq > n.lastAppliedSeqNum {
		fmt.Printf("[Leader %s] Updating lastAppliedSeqNum from %d to %d\n",
			n.id[:8], n.lastAppliedSeqNum, agreedSeq)
		n.lastAppliedSeqNum = agreedSeq
	}

	// Install view locally first
	err = n.viewState.InstallView(newViewNumber, n.id, memberIDs, memberAddresses, agreedSeq)
	if err != nil {
		return fmt.Errorf("failed to install view locally: %w", err)
	}

	// Send VIEW_INSTALL to all followers with merged logs
	var wg sync.WaitGroup
	ackChan := make(chan viewInstallAck, len(memberIDs))

	for i, memberID := range memberIDs {
		if memberID == n.id {
			continue // Skip self
		}

		wg.Add(1)
		go func(mID, mAddr string) {
			defer wg.Done()
			ack := n.sendViewInstall(mID, mAddr, newViewNumber, agreedSeq, stateSnapshot, memberIDs, memberAddresses, agreedLogOffsets, mergedLogs)
			ackChan <- ack
		}(memberID, memberAddresses[i])
	}

	// Wait for acks with timeout
	go func() {
		wg.Wait()
		close(ackChan)
	}()

	// Collect acknowledgments
	ackCount := 1 // Count ourselves
	timeout := time.After(10 * time.Second)

collectAcks:
	for {
		select {
		case ack, ok := <-ackChan:
			if !ok {
				break collectAcks
			}
			if ack.success {
				ackCount++
				fmt.Printf("[Leader %s] VIEW_INSTALL_ACK from %s (success)\n", n.id[:8], ack.brokerID[:8])
			} else {
				fmt.Printf("[Leader %s] VIEW_INSTALL_ACK from %s (failed: %s)\n",
					n.id[:8], ack.brokerID[:8], ack.errorMsg)
			}

		case <-timeout:
			fmt.Printf("[Leader %s] VIEW_INSTALL timeout, received %d acks\n", n.id[:8], ackCount)
			break collectAcks
		}
	}

	// Check if we have majority
	requiredAcks := (len(memberIDs) / 2) + 1
	if ackCount >= requiredAcks {
		fmt.Printf("[Leader %s] View %d installed successfully with %d/%d acks\n",
			n.id[:8], newViewNumber, ackCount, len(memberIDs))
	} else {
		fmt.Printf("[Leader %s] Warning: View %d installed with only %d/%d acks (need %d)\n",
			n.id[:8], newViewNumber, ackCount, len(memberIDs), requiredAcks)
	}

	// End recovery (no-op if no recovery was in progress, e.g. node join)
	n.recoveryManager.EndRecovery()

	// Reset replication sequence to agreed sequence
	// This ensures new replication starts from clean boundary
	n.lastReplicatedSeq = agreedSeq
	fmt.Printf("[Leader %s] Reset lastReplicatedSeq to %d for new view\n", n.id[:8], agreedSeq)

	// Note: unfreeze happens via defer at the top of this function

	fmt.Printf("[Leader %s] New view %d is now active\n", n.id[:8], newViewNumber)
	return nil
}

type viewInstallAck struct {
	brokerID string
	success  bool
	errorMsg string
}

// sendViewInstall sends a VIEW_INSTALL message to a follower
// mergedLogs contains the UNION of all log entries for view-synchronous consistency
func (n *Node) sendViewInstall(brokerID, brokerAddr string, viewNumber, agreedSeq int64, stateSnapshot []byte, memberIDs, memberAddresses []string, agreedLogOffsets map[string]uint64, mergedLogs []*protocol.TopicLogEntries) viewInstallAck {
	fmt.Printf("[Leader %s] Sending VIEW_INSTALL to %s at %s (merged logs: %d topics)\n", n.id[:8], brokerID[:8], brokerAddr, len(mergedLogs))

	// Connect to broker
	conn, err := net.DialTimeout("tcp", brokerAddr, 5*time.Second)
	if err != nil {
		return viewInstallAck{brokerID: brokerID, success: false, errorMsg: err.Error()}
	}
	defer conn.Close()

	// Set deadline
	conn.SetDeadline(time.Now().Add(10 * time.Second))

	// Send VIEW_INSTALL message with merged logs for view-synchronous consistency
	msg := protocol.NewViewInstallMsg(n.id, viewNumber, agreedSeq, stateSnapshot, memberIDs, memberAddresses, agreedLogOffsets, mergedLogs)
	if err := protocol.WriteTCPMessage(conn, msg); err != nil {
		return viewInstallAck{brokerID: brokerID, success: false, errorMsg: err.Error()}
	}

	// Wait for acknowledgment
	respMsg, err := protocol.ReadTCPMessage(conn)
	if err != nil {
		return viewInstallAck{brokerID: brokerID, success: false, errorMsg: err.Error()}
	}

	// Verify response type
	ackMsg, ok := respMsg.(*protocol.ViewInstallAckMsg)
	if !ok {
		return viewInstallAck{brokerID: brokerID, success: false, errorMsg: "unexpected response type"}
	}

	return viewInstallAck{
		brokerID: brokerID,
		success:  ackMsg.Success,
		errorMsg: ackMsg.ErrorMessage,
	}
}

// handleViewInstall handles VIEW_INSTALL messages from the leader
func (n *Node) handleViewInstall(msg *protocol.ViewInstallMsg, conn net.Conn) {
	senderID := protocol.GetSenderID(msg)
	viewNumber := msg.ViewNumber
	agreedSeq := msg.AgreedSeq
	mergedLogs := msg.MergedLogs // View-synchronous: full entries for merge

	fmt.Printf("\n[Node %s] ========================================\n", n.id[:8])
	fmt.Printf("[Node %s] <- VIEW_INSTALL from %s\n", n.id[:8], senderID[:8])
	fmt.Printf("[Node %s] View Number: %d\n", n.id[:8], viewNumber)
	fmt.Printf("[Node %s] Agreed Seq: %d\n", n.id[:8], agreedSeq)
	fmt.Printf("[Node %s] Merged Logs: %d topics\n", n.id[:8], len(mergedLogs))
	fmt.Printf("[Node %s] Members: %d\n", n.id[:8], len(msg.MemberIds))
	fmt.Printf("[Node %s] ========================================\n\n", n.id[:8])

	var success bool
	var errorMsg string

	// Verify view number
	currentView := n.viewState.GetViewNumber()
	isViewChange := viewNumber > currentView
	isStateUpdate := viewNumber == currentView

	if viewNumber < currentView {
		// Reject stale view numbers
		errorMsg = fmt.Sprintf("stale view number %d (current: %d)", viewNumber, currentView)
		success = false
	} else {
		// Apply the state (for both view changes and state updates)
		if len(msg.StateSnapshot) > 0 {
			if err := n.clusterState.Deserialize(msg.StateSnapshot); err != nil {
				errorMsg = fmt.Sprintf("failed to apply state: %v", err)
				success = false
			} else {
				success = true
				// Sync broker ring after state application
				n.syncBrokerRing()
			}
		} else {
			success = true
		}

		if success {
			// Update our sequence number
			if agreedSeq > n.lastAppliedSeqNum {
				fmt.Printf("[Node %s] Updating lastAppliedSeqNum from %d to %d\n",
					n.id[:8], n.lastAppliedSeqNum, agreedSeq)
				n.lastAppliedSeqNum = agreedSeq
			}

			if isViewChange {
				// Full view change - install new view
				err := n.viewState.InstallView(viewNumber, senderID, msg.MemberIds, msg.MemberAddresses, agreedSeq)
				if err != nil {
					errorMsg = fmt.Sprintf("failed to install view: %v", err)
					success = false
				} else {
					fmt.Printf("[Node %s] Successfully installed view %d\n", n.id[:8], viewNumber)

					// Reset holdback queue to agreed sequence + 1
					// This ensures we start fresh in the new view
					n.holdbackQueue.Reset(agreedSeq + 1)
					fmt.Printf("[Node %s] Reset holdback queue to seq %d for new view\n", n.id[:8], agreedSeq+1)

					// Update leader information
					n.leaderAddress = ""
					for i, id := range msg.MemberIds {
						if id == senderID && i < len(msg.MemberAddresses) {
							n.leaderAddress = msg.MemberAddresses[i]
							break
						}
					}

					// CRITICAL: Reset lastLeaderHeartbeat to prevent false failure detection
					// Without this, the follower would immediately detect "leader failure" after
					// installing the new view because the old heartbeat timestamp is stale
					n.lastLeaderHeartbeatMu.Lock()
					n.lastLeaderHeartbeat = time.Now()
					n.lastLeaderHeartbeatMu.Unlock()
					fmt.Printf("[Node %s] Reset lastLeaderHeartbeat for new leader %s\n", n.id[:8], senderID[:8])

					// View-synchronous: Apply merged logs (UNION of all entries)
					// This preserves all acknowledged data
					if len(mergedLogs) > 0 {
						n.applyMergedLogs(mergedLogs)
					}
				}
			} else if isStateUpdate {
				// State update within same view - just update leader address if needed
				fmt.Printf("[Node %s] Applied state update (seq=%d) within view %d\n",
					n.id[:8], agreedSeq, viewNumber)

				// Update leader address if not set
				if n.leaderAddress == "" {
					for i, id := range msg.MemberIds {
						if id == senderID && i < len(msg.MemberAddresses) {
							n.leaderAddress = msg.MemberAddresses[i]
							break
						}
					}
				}
			}
		}
	}

	// Send acknowledgment
	ack := protocol.NewViewInstallAckMsg(n.id, viewNumber, success, errorMsg)
	if err := protocol.WriteTCPMessage(conn, ack); err != nil {
		log.Printf("[Node %s] Failed to send VIEW_INSTALL_ACK: %v\n", n.id[:8], err)
	} else {
		fmt.Printf("[Node %s] -> VIEW_INSTALL_ACK (success: %v)\n", n.id[:8], success)
	}

	// Unfreeze operations if successful AND this was a view change
	// State updates don't freeze operations, so no unfreeze needed
	if success && isViewChange && n.IsFrozen() {
		n.unfreezeOperations()
	}
}

// GetViewState returns the current view state
func (n *Node) GetViewState() *state.ViewState {
	return n.viewState
}

// GetRecoveryManager returns the recovery manager
func (n *Node) GetRecoveryManager() *state.ViewRecoveryManager {
	return n.recoveryManager
}

// PrintViewStatus prints the current view status
func (n *Node) PrintViewStatus() {
	n.viewState.PrintStatus()
}

// performViewChangeForNodeJoin performs a view change when a new node joins.
// Uses the same installNewView path as post-election recovery for consistency.
// Steps: Freeze -> Register broker -> installNewView (which defers unfreeze)
func (n *Node) performViewChangeForNodeJoin(newNodeID, newNodeAddr string) {
	if !n.IsLeader() {
		return
	}

	fmt.Printf("\n[Leader %s] ========================================\n", n.id[:8])
	fmt.Printf("[Leader %s] VIEW CHANGE - Node Join\n", n.id[:8])
	fmt.Printf("[Leader %s] New Node: %s (%s)\n", n.id[:8], newNodeID[:8], newNodeAddr)
	fmt.Printf("[Leader %s] ========================================\n\n", n.id[:8])

	// 1. Freeze operations
	n.freezeOperations()

	// 2. Register new broker in cluster state
	n.clusterState.RegisterBroker(newNodeID, newNodeAddr, false)

	// 3. Sync the broker ring so the new node is included in topic assignment
	n.syncBrokerRing()

	// 4. Rebalance: move streams assigned to leader to followers now that we have more nodes
	leaderStreams := n.clusterState.GetStreamsByBroker(n.id)
	fmt.Printf("[Leader %s] [ViewChange-Rebalance] Ring nodes=%d, streams on leader=%d\n",
		n.id[:8], n.brokerRing.NodeCount(), len(leaderStreams))
	reassignments := n.rebalanceLeaderStreams()

	// 5. Collect current log offsets for new node synchronization
	agreedLogOffsets := n.collectLogOffsets()

	// 6. Install new view (handles view increment, VIEW_INSTALL to followers, and defers unfreeze)
	// The updated stream assignments are included in the replicated state.
	currentSeq := n.lastAppliedSeqNum
	if err := n.installNewView(0, currentSeq, agreedLogOffsets, nil); err != nil {
		log.Printf("[Leader %s] View change for node join failed: %v\n", n.id[:8], err)
		// installNewView defers unfreeze, so no manual unfreeze needed
	}

	// 7. Notify clients AFTER view install so the follower has the stream assignments
	// before clients try to reconnect
	n.notifyStreamReassignments(reassignments)

	fmt.Printf("[Leader %s] View change for node join complete\n", n.id[:8])
}

// streamReassignment holds info about a reassigned stream for deferred client notification
type streamReassignment struct {
	topic      string
	producerID string
	consumerID string
	newBrokerID   string
	newBrokerAddr string
}

// rebalanceLeaderStreams reassigns streams currently assigned to the leader to followers.
// Called when a new broker joins and the leader can delegate data processing.
// Returns reassignment info so callers can notify clients AFTER state is replicated.
func (n *Node) rebalanceLeaderStreams() []streamReassignment {
	if n.brokerRing.NodeCount() <= 1 {
		return nil
	}

	streams := n.clusterState.GetStreamsByBroker(n.id)
	if len(streams) == 0 {
		return nil
	}

	fmt.Printf("[Leader %s] [Rebalance] Found %d streams assigned to leader, reassigning to followers\n",
		n.id[:8], len(streams))

	var reassignments []streamReassignment

	for _, stream := range streams {
		newBrokerID := n.brokerRing.GetNodeExcluding(stream.Topic, n.id)
		if newBrokerID == "" || newBrokerID == n.id {
			continue
		}

		broker, ok := n.clusterState.GetBroker(newBrokerID)
		if !ok {
			continue
		}

		if err := n.clusterState.ReassignStreamBroker(stream.Topic, newBrokerID, broker.Address); err != nil {
			log.Printf("[Leader %s] [Rebalance] Failed to reassign topic %s: %v\n", n.id[:8], stream.Topic, err)
			continue
		}

		fmt.Printf("[Leader %s] [Rebalance] Reassigned topic %s: %s -> %s @ %s\n",
			n.id[:8], stream.Topic, n.id[:8], newBrokerID[:8], broker.Address)

		reassignments = append(reassignments, streamReassignment{
			topic:         stream.Topic,
			producerID:    stream.ProducerId,
			consumerID:    stream.ConsumerId,
			newBrokerID:   newBrokerID,
			newBrokerAddr: broker.Address,
		})
	}

	return reassignments
}

// notifyStreamReassignments sends REASSIGN_BROKER to affected clients.
// Must be called AFTER installNewView so the follower has the updated stream assignments.
func (n *Node) notifyStreamReassignments(reassignments []streamReassignment) {
	for _, r := range reassignments {
		if r.producerID != "" {
			go n.notifyClientOfReassignment(r.producerID, protocol.NodeType_PRODUCER, r.topic, r.newBrokerAddr, r.newBrokerID)
		}
		if r.consumerID != "" {
			go n.notifyClientOfReassignment(r.consumerID, protocol.NodeType_CONSUMER, r.topic, r.newBrokerAddr, r.newBrokerID)
		}
	}
}

// collectLogOffsets returns a map of topic -> highest offset for all local logs
// DEPRECATED: Used in old offset-based sync during state exchange
// Kept for backward compatibility
func (n *Node) collectLogOffsets() map[string]uint64 {
	n.dataLogsMu.RLock()
	defer n.dataLogsMu.RUnlock()

	offsets := make(map[string]uint64)
	for topic, topicLog := range n.dataLogs {
		highest, err := topicLog.HighestOffset()
		if err == nil {
			offsets[topic] = highest
		}
	}
	return offsets
}

// collectLogEntries returns full log entries for all topics
// Used in view-synchronous state exchange to collect complete log state
// This enables union-based merge that preserves all acknowledged data
func (n *Node) collectLogEntries() []*protocol.TopicLogEntries {
	n.dataLogsMu.RLock()
	defer n.dataLogsMu.RUnlock()

	var result []*protocol.TopicLogEntries
	for topic, topicLog := range n.dataLogs {
		entries := topicLog.GetAllEntries()
		if len(entries) == 0 {
			continue
		}

		// Convert storage.LogEntry to protocol.LogEntry
		protoEntries := make([]*protocol.LogEntry, len(entries))
		for i, entry := range entries {
			protoEntries[i] = &protocol.LogEntry{
				Offset:    entry.Offset,
				Data:      entry.Data,
				Timestamp: entry.Timestamp,
			}
		}

		result = append(result, &protocol.TopicLogEntries{
			Topic:   topic,
			Entries: protoEntries,
		})
	}
	return result
}

// mergeAllTopicLogs performs view-synchronous log merge using UNION semantics
// Any entry that exists in ANY log from ANY member is preserved
// This ensures no acknowledged data is lost during view change
func mergeAllTopicLogs(responses []stateExchangeResponse) []*protocol.TopicLogEntries {
	// Map: topic -> (offset -> entry)
	// Using offset as key ensures deduplication of same entries
	merged := make(map[string]map[uint64]*protocol.LogEntry)

	for _, resp := range responses {
		for _, topicLog := range resp.topicLogs {
			if topicLog == nil {
				continue
			}
			topic := topicLog.Topic
			if merged[topic] == nil {
				merged[topic] = make(map[uint64]*protocol.LogEntry)
			}

			for _, entry := range topicLog.Entries {
				if entry == nil {
					continue
				}
				// UNION: Keep entry if we don't have it, or replace if newer timestamp
				existing, exists := merged[topic][entry.Offset]
				if !exists || (existing != nil && entry.Timestamp > existing.Timestamp) {
					merged[topic][entry.Offset] = entry
				}
			}
		}
	}

	// Convert map back to sorted TopicLogEntries
	var result []*protocol.TopicLogEntries
	for topic, entryMap := range merged {
		// Collect and sort entries by offset
		var entries []*protocol.LogEntry
		for _, entry := range entryMap {
			if entry != nil {
				entries = append(entries, entry)
			}
		}

		// Sort by offset
		sortLogEntries(entries)

		result = append(result, &protocol.TopicLogEntries{
			Topic:   topic,
			Entries: entries,
		})
	}

	return result
}

// sortLogEntries sorts log entries by offset in ascending order
func sortLogEntries(entries []*protocol.LogEntry) {
	for i := 0; i < len(entries)-1; i++ {
		for j := i + 1; j < len(entries); j++ {
			if entries[i].Offset > entries[j].Offset {
				entries[i], entries[j] = entries[j], entries[i]
			}
		}
	}
}

// computeOffsetsFromMergedLogs computes highest offset per topic from merged logs
// Used for backward compatibility with offset-based systems
func computeOffsetsFromMergedLogs(mergedLogs []*protocol.TopicLogEntries) map[string]uint64 {
	offsets := make(map[string]uint64)
	for _, topicLog := range mergedLogs {
		if topicLog == nil || len(topicLog.Entries) == 0 {
			continue
		}
		// Find highest offset
		var highest uint64
		for _, entry := range topicLog.Entries {
			if entry != nil && entry.Offset > highest {
				highest = entry.Offset
			}
		}
		offsets[topicLog.Topic] = highest
	}
	return offsets
}

// applyMergedLogs applies merged log entries to local storage
// This fills in any gaps in our local log with entries from other members
func (n *Node) applyMergedLogs(mergedLogs []*protocol.TopicLogEntries) {
	n.dataLogsMu.Lock()
	defer n.dataLogsMu.Unlock()

	for _, topicLog := range mergedLogs {
		if topicLog == nil {
			continue
		}

		topicStorage, exists := n.dataLogs[topicLog.Topic]
		if !exists {
			// Create new log for this topic
			topicStorage = storage.NewMemoryLog()
			n.dataLogs[topicLog.Topic] = topicStorage
		}

		// Apply each entry
		for _, entry := range topicLog.Entries {
			if entry == nil {
				continue
			}
			if err := topicStorage.AppendAtOffset(entry.Offset, entry.Data); err != nil {
				fmt.Printf("[Node %s] Warning: Failed to apply merged entry at offset %d for topic %s: %v\n",
					n.id[:8], entry.Offset, topicLog.Topic, err)
			}
		}
	}

	fmt.Printf("[Node %s] Applied merged logs for %d topics\n", n.id[:8], len(mergedLogs))
}

