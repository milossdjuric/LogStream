package node

import (
	"fmt"
	"log"
	"net"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/milossdjuric/logstream/internal/protocol"
)

func (n *Node) runLeaderDuties() {
	fmt.Printf("[Leader %s] Starting leader duties\n", n.id[:8])

	if err := n.sendHeartbeat(); err != nil {
		log.Printf("[Leader %s] Failed to send initial heartbeat: %v\n", n.id[:8], err)
	}

	heartbeatTicker := time.NewTicker(5 * time.Second)
	clientHeartbeatTicker := time.NewTicker(30 * time.Second)
	timeoutTicker := time.NewTicker(5 * time.Second) // Check more frequently for faster cleanup
	defer heartbeatTicker.Stop()
	defer clientHeartbeatTicker.Stop()
	defer timeoutTicker.Stop()

	for {
		select {
		case <-heartbeatTicker.C:
			if err := n.sendHeartbeat(); err != nil {
				log.Printf("[Leader %s] Failed to send heartbeat: %v\n", n.id[:8], err)
			}

		case <-clientHeartbeatTicker.C:
			n.sendHeartbeatsToProducers()
			n.sendHeartbeatsToConsumers()

		case <-timeoutTicker.C:
			removedBrokers := n.clusterState.CheckBrokerTimeouts(30 * time.Second)
			if len(removedBrokers) > 0 {
				fmt.Printf("[Leader %s] Removed %d dead brokers\n", n.id[:8], len(removedBrokers))
				for _, brokerID := range removedBrokers {
					n.handleBrokerFailure(brokerID)
				}
				n.syncBrokerRing()
				n.replicateAllState()
			}

			deadProducers := n.clusterState.CheckProducerTimeouts(30 * time.Second)
			if len(deadProducers) > 0 {
				for _, producerID := range deadProducers {
					n.cleanupProducer(producerID)
				}
				n.replicateAllState()
			}

			deadConsumers := n.clusterState.CheckConsumerTimeouts(30 * time.Second)
			if len(deadConsumers) > 0 {
				for _, consumerID := range deadConsumers {
					n.cleanupConsumer(consumerID)
				}
				n.replicateAllState()
			}

		case <-n.stopLeaderDuties:
			return
		}
	}
}

// sendHeartbeat sends a heartbeat message to all brokers via multicast
func (n *Node) sendHeartbeat() error {
	if !n.IsLeader() {
		return fmt.Errorf("only leader can send heartbeat")
	}

	// protocol.SendHeartbeatMulticast() provided by protocol package
	viewNumber := n.viewState.GetViewNumber()
	err := protocol.SendHeartbeatMulticast(
		n.multicastSender,
		n.id,
		protocol.NodeType_LEADER,
		viewNumber,
		n.config.MulticastGroup,
	)

	if err == nil {
		fmt.Printf("[Leader %s] -> HEARTBEAT\n", n.id[:8])
	}

	return err
}

// replicateStateToFollowers performs TCP-based state replication to all followers
// This replaces UDP multicast REPLICATE with reliable TCP VIEW_INSTALL messages
// Key benefits:
//  1. Reliable delivery with ACK confirmation
//  2. No race conditions with UDP listener readiness
//  3. Consistent with view-synchronous protocol (same mechanism as VIEW_INSTALL)
//
// Called event-driven on every state change (brokers, producers, consumers)
func (n *Node) replicateStateToFollowers() error {
	if !n.IsLeader() {
		return fmt.Errorf("only leader can replicate state")
	}

	// Check if state has changed since last replication
	currentSeq := n.clusterState.GetSequenceNum()
	if currentSeq == n.lastReplicatedSeq {
		// No state change, skip replication
		return nil
	}

	fmt.Printf("[Leader %s] State changed (seq: %d -> %d), replicating via TCP...\n",
		n.id[:8], n.lastReplicatedSeq, currentSeq)

	// Serialize cluster state
	snapshot, err := n.clusterState.Serialize()
	if err != nil {
		return err
	}

	fmt.Printf("[Leader %s] Serialized cluster state: %d bytes, %d brokers, %d producers, %d consumers\n",
		n.id[:8], len(snapshot), n.clusterState.GetBrokerCount(), n.clusterState.CountProducers(), n.clusterState.CountConsumers())

	// Get list of followers
	brokers := n.clusterState.ListBrokers()
	var followerIDs []string
	var followerAddresses []string

	for _, brokerID := range brokers {
		if brokerID == n.id {
			continue // Skip self
		}
		broker, ok := n.clusterState.GetBroker(brokerID)
		if ok {
			followerIDs = append(followerIDs, brokerID)
			followerAddresses = append(followerAddresses, broker.Address)
		}
	}

	if len(followerIDs) == 0 {
		// No followers to replicate to, just update our sequence
		n.lastReplicatedSeq = currentSeq
		fmt.Printf("[Leader %s] No followers to replicate to, state updated locally (seq=%d)\n",
			n.id[:8], currentSeq)
		return nil
	}

	// Get current view info
	viewNumber := n.viewState.GetViewNumber()

	// Get membership info for VIEW_INSTALL
	var memberIDs []string
	var memberAddresses []string
	for _, brokerID := range brokers {
		broker, ok := n.clusterState.GetBroker(brokerID)
		if ok {
			memberIDs = append(memberIDs, brokerID)
			memberAddresses = append(memberAddresses, broker.Address)
		}
	}

	// Collect current log offsets
	logOffsets := n.collectLogOffsets()

	// Send state to all followers via TCP VIEW_INSTALL (parallel)
	var wg sync.WaitGroup
	ackCount := 0
	var ackMu sync.Mutex

	for i, followerID := range followerIDs {
		wg.Add(1)
		go func(fID, fAddr string) {
			defer wg.Done()
			// Use sendStateUpdate which sends VIEW_INSTALL with current view (not incremented)
			ack := n.sendStateUpdate(fID, fAddr, viewNumber, currentSeq, snapshot, memberIDs, memberAddresses, logOffsets)
			if ack.success {
				ackMu.Lock()
				ackCount++
				ackMu.Unlock()
				fmt.Printf("[Leader %s] REPLICATE_ACK from %s (TCP, success)\n", n.id[:8], ack.brokerID[:8])
			} else {
				fmt.Printf("[Leader %s] REPLICATE_ACK from %s (TCP, failed: %s)\n",
					n.id[:8], ack.brokerID[:8], ack.errorMsg)
			}
		}(followerID, followerAddresses[i])
	}

	// Wait for all responses with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All responses received
	case <-time.After(5 * time.Second):
		fmt.Printf("[Leader %s] State replication timeout - some followers may not have received update\n", n.id[:8])
	}

	// Update last replicated sequence
	n.lastReplicatedSeq = currentSeq
	fmt.Printf("[Leader %s] -> REPLICATE seq=%d (TCP) to %d followers (%d acked)\n",
		n.id[:8], currentSeq, len(followerIDs), ackCount)

	return nil
}

// sendStateUpdate sends a state update (VIEW_INSTALL without view increment) to a follower
func (n *Node) sendStateUpdate(brokerID, brokerAddr string, viewNumber, seqNum int64, stateSnapshot []byte, memberIDs, memberAddresses []string, logOffsets map[string]uint64) viewInstallAck {
	// Connect to broker
	conn, err := net.DialTimeout("tcp", brokerAddr, 3*time.Second)
	if err != nil {
		return viewInstallAck{brokerID: brokerID, success: false, errorMsg: err.Error()}
	}
	defer conn.Close()

	// Set deadline
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	// Send VIEW_INSTALL message (reusing existing protocol message)
	// Note: We use the same view number (not incremented) for state updates
	// View increments only happen during membership changes or elections
	// For state updates (not recovery), we don't need merged logs - pass nil
	msg := protocol.NewViewInstallMsg(n.id, viewNumber, seqNum, stateSnapshot, memberIDs, memberAddresses, logOffsets, nil)
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

// replicateAllState is deprecated - use replicateStateToFollowers instead
// Kept for backward compatibility during transition
func (n *Node) replicateAllState() error {
	return n.replicateStateToFollowers()
}


// sendHeartbeatsToProducers sends TCP unicast heartbeats to all registered producers
// Proposal requirement: "the leader node sends heartbeat messages to the consumers/producers"
func (n *Node) sendHeartbeatsToProducers() {
	if !n.IsLeader() {
		return
	}

	producers := n.clusterState.ListProducers()
	if len(producers) == 0 {
		return
	}

	fmt.Printf("[Leader %s] Sending heartbeats to %d producer(s)...\n", n.id[:8], len(producers))

	for _, producerID := range producers {
		producer, ok := n.clusterState.GetProducer(producerID)
		if !ok {
			continue
		}

		// Send heartbeat via TCP in goroutine to avoid blocking
		go func(pid string, addr string) {
			conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
			if err != nil {
				// Producer might be down - will be detected by timeout check
				return
			}
			defer conn.Close()

		// Create heartbeat message with current view number
		viewNumber := n.viewState.GetViewNumber()
		heartbeat := protocol.NewHeartbeatMsg(n.id, protocol.NodeType_LEADER, 0, viewNumber)

		// Send heartbeat
		if err := protocol.WriteTCPMessage(conn, heartbeat); err != nil {
			return // Producer might be down
		}

		// Update producer heartbeat timestamp on success
		n.clusterState.UpdateProducerHeartbeat(pid)
		fmt.Printf("[Leader %s] -> HEARTBEAT (TCP) to producer %s\n", n.id[:8], pid[:8])
		}(producerID, producer.Address)
	}
}

// sendHeartbeatsToConsumers sends TCP unicast heartbeats to all registered consumers
// Proposal requirement: "the leader node sends heartbeat messages to the consumers/producers"
func (n *Node) sendHeartbeatsToConsumers() {
	if !n.IsLeader() {
		return
	}

	consumers := n.clusterState.ListConsumers()
	if len(consumers) == 0 {
		return
	}

	fmt.Printf("[Leader %s] Sending heartbeats to %d consumer(s)...\n", n.id[:8], len(consumers))

	for _, consumerID := range consumers {
		consumer, ok := n.clusterState.GetConsumer(consumerID)
		if !ok {
			continue
		}

		// Send heartbeat via TCP in goroutine to avoid blocking
		go func(cid string, addr string) {
			conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
			if err != nil {
				// Consumer might be down - will be detected by timeout check
				return
			}
			defer conn.Close()

		// Create heartbeat message with current view number
		viewNumber := n.viewState.GetViewNumber()
		heartbeat := protocol.NewHeartbeatMsg(n.id, protocol.NodeType_LEADER, 0, viewNumber)

		// Send heartbeat
		if err := protocol.WriteTCPMessage(conn, heartbeat); err != nil {
			return // Consumer might be down
		}

		// Update consumer heartbeat timestamp on success
		n.clusterState.UpdateConsumerHeartbeat(cid)
		fmt.Printf("[Leader %s] -> HEARTBEAT (TCP) to consumer %s\n", n.id[:8], cid[:8])
		}(consumerID, consumer.Address)
	}
}

// listenForBroadcastJoins listens for JOIN requests from new nodes
func (n *Node) listenForBroadcastJoins() {
	for {
		// protocol.BroadcastConnection.ReceiveMessage() provided by protocol package
		msg, sender, err := n.broadcastListener.ReceiveMessage()
		if err != nil {
			return // Listener closed
		}

		if joinMsg, ok := msg.(*protocol.JoinMsg); ok {
			senderID := protocol.GetSenderID(msg)
			senderType := protocol.GetSenderType(msg)
			fmt.Printf("[Node %s] <- JOIN from %s (addr=%s, type=%s)\n",
				n.id[:8], senderID[:8], joinMsg.Address, senderType)

			// CRITICAL FIX: Ignore JOIN messages from ourselves!
			// This prevents self-discovery where a node discovers itself and becomes a follower
			if senderID == n.id {
				fmt.Printf("[Node %s] IGNORING JOIN from self (self-discovery prevention)\n", n.id[:8])
				continue
			}

			// CRITICAL FIX: Only leader should respond to JOIN messages
			if !n.IsLeader() {
				fmt.Printf("[Node %s] IGNORING JOIN from %s - not leader (current role: follower)\n",
					n.id[:8], senderID[:8])
				continue
			}

			// Client discovery: producers/consumers only need the leader address.
			// They are NOT broker nodes - skip network checks, TCP verification, and view changes.
			if senderType == protocol.NodeType_PRODUCER || senderType == protocol.NodeType_CONSUMER {
				fmt.Printf("[Leader %s] Client discovery from %s (type=%s, addr=%s)\n",
					n.id[:8], senderID[:8], senderType, joinMsg.Address)

				// Send JOIN_RESPONSE to the UDP source address (sender).
				// The client uses the SAME socket for sending broadcasts and receiving responses,
				// matching the proven broker-to-broker discovery pattern.
				responseAddr := fmt.Sprintf("%s", sender)
				err := n.broadcastListener.SendJoinResponse(
					n.id,
					n.address,
					n.config.MulticastGroup,
					[]string{n.address},
					responseAddr,
				)
				if err != nil {
					log.Printf("[Leader %s] Failed to send JOIN_RESPONSE to client: %v\n", n.id[:8], err)
				} else {
					fmt.Printf("[Leader %s] -> JOIN_RESPONSE to client %s at %s\n", n.id[:8], senderID[:8], responseAddr)
				}
				continue
			}

			// --- Broker JOIN handling below ---

			// Filter out JOIN messages from wrong network interfaces (e.g., Vagrant NAT 192.168.121.x)
			// Only accept JOINs from the same network as the leader (192.168.100.x)
			// We check the JOIN message address (which is what the node claims to be) rather than
			// the UDP sender IP (which might be NAT'd through Vagrant)
			leaderIP := strings.Split(n.address, ":")[0]
			leaderNetwork := strings.Join(strings.Split(leaderIP, ".")[:3], ".")

			// Check the address in the JOIN message itself (this is what matters)
			joinIP := strings.Split(joinMsg.Address, ":")[0]
			joinNetwork := strings.Join(strings.Split(joinIP, ".")[:3], ".")

			if joinNetwork != leaderNetwork {
				fmt.Printf("[Leader %s] REJECTING JOIN from %s: wrong network (join=%s, expected=%s)\n",
					n.id[:8], senderID[:8], joinNetwork, leaderNetwork)
				continue // Ignore JOINs from wrong network
			}

			// Split-brain detection: if sender is also claiming to be leader, trigger election
			// Check if sender is already in our registry as a leader
			if broker, ok := n.clusterState.GetBroker(senderID); ok && broker.IsLeader {
				fmt.Printf("[Leader %s] SPLIT-BRAIN DETECTED: Node %s is also claiming to be leader!\n",
					n.id[:8], senderID[:8])
				fmt.Printf("[Leader %s] Triggering election to resolve split-brain...\n", n.id[:8])
				// Trigger election to resolve split-brain
				go func() {
					time.Sleep(1 * time.Second) // Small delay to ensure network is ready
					if err := n.StartElection(); err != nil {
						log.Printf("[Leader %s] Failed to start election for split-brain resolution: %v\n", n.id[:8], err)
					}
				}()
				continue // Don't register the duplicate leader
			}

			// Rate limit JOIN rejections to prevent flooding
			// If we recently rejected this node, skip processing entirely
			n.joinRejectionsMu.Lock()
			lastRejection, wasRejected := n.joinRejections[senderID]
			n.joinRejectionsMu.Unlock()

			if wasRejected && time.Since(lastRejection) < 30*time.Second {
				// Silently ignore - we already rejected this node recently
				continue
			}

			// CRITICAL: Verify TCP connectivity BEFORE registering the node
			// UDP broadcast may work even when TCP doesn't (different networks, NAT, firewall)
			// Without this check, we register nodes we can't actually communicate with via TCP
			if !n.verifyTCPConnectivity(joinMsg.Address, senderID) {
				fmt.Printf("[Leader %s] REJECTING JOIN from %s: TCP connectivity verification failed to %s\n",
					n.id[:8], senderID[:8], joinMsg.Address)
				fmt.Printf("[Leader %s] UDP broadcast works but TCP doesn't - nodes may be on different networks\n", n.id[:8])

				// Record rejection to rate-limit future attempts
				n.joinRejectionsMu.Lock()
				n.joinRejections[senderID] = time.Now()
				n.joinRejectionsMu.Unlock()
				continue
			}

			// Clear any previous rejection record since TCP now works
			n.joinRejectionsMu.Lock()
			delete(n.joinRejections, senderID)
			n.joinRejectionsMu.Unlock()

			fmt.Printf("[Leader %s] TCP connectivity verified to %s at %s\n", n.id[:8], senderID[:8], joinMsg.Address)

			// Register new node in state.Registry
			newNodeID := senderID

			// View-synchronous: Perform lightweight view change on node join
			// This ensures membership changes are delivered atomically with view change
			// performLightweightViewChange already sends VIEW_INSTALL to all followers via TCP
			n.performLightweightViewChange(newNodeID, joinMsg.Address)

			// Send response using protocol.BroadcastConnection
			responseAddr := fmt.Sprintf("%s", sender)
			err := n.broadcastListener.SendJoinResponse(
				n.id,
				n.address,
				n.config.MulticastGroup,
				[]string{n.address},
				responseAddr,
			)

			if err != nil {
				log.Printf("[Leader %s] Failed to send JOIN_RESPONSE: %v\n", n.id[:8], err)
			} else {
				fmt.Printf("[Leader %s] -> JOIN_RESPONSE to %s\n", n.id[:8], sender)
			}

			// Note: State replication is handled by performLightweightViewChange via TCP VIEW_INSTALL
			// No additional UDP multicast sync needed
		}
	}
}

// startBroadcastListener starts the broadcast listener for incoming JOIN requests
func (n *Node) startBroadcastListener() {
	if n.broadcastListener == nil {
		fmt.Printf("[Node %s] [becomeLeader] Creating broadcast listener on port %d...\n", n.id[:8], n.config.BroadcastPort)
		listener, err := protocol.CreateBroadcastListener(n.config.BroadcastPort)
		if err != nil {
			fmt.Printf("[Node %s] [becomeLeader] ERROR: Failed to start broadcast listener: %v\n", n.id[:8], err)
			log.Printf("[Node %s] Failed to start broadcast listener: %v\n", n.id[:8], err)
		} else {
			n.broadcastListener = listener
			fmt.Printf("[Node %s] [becomeLeader] Starting broadcast join listener goroutine...\n", n.id[:8])
			go func() {
				defer func() {
					if r := recover(); r != nil {
						buf := make([]byte, 4096)
						stackLen := runtime.Stack(buf, false)
						fmt.Printf("[Node %s] [Broadcast-Listener] PANIC RECOVERED: %v\nStack:\n%s\n", n.id[:8], r, buf[:stackLen])
						log.Printf("[Node %s] [Broadcast-Listener] PANIC: %v\nStack:\n%s\n", n.id[:8], r, buf[:stackLen])
					}
				}()
				n.listenForBroadcastJoins()
			}()
			fmt.Printf("[Leader %s] Broadcast listener ready on port %d\n", n.id[:8], n.config.BroadcastPort)
		}
	} else {
		fmt.Printf("[Node %s] [becomeLeader] Broadcast listener already exists\n", n.id[:8])
	}
}

// verifyTCPConnectivity tests if we can establish a TCP connection to a node
// This is critical because UDP broadcast may work even when TCP doesn't
// (e.g., different networks, NAT, firewall rules)
func (n *Node) verifyTCPConnectivity(address, nodeID string) bool {
	fmt.Printf("[Leader %s] [TCP-Verify] Testing TCP connectivity to %s at %s...\n",
		n.id[:8], nodeID[:8], address)

	// Try to establish TCP connection with short timeout
	conn, err := net.DialTimeout("tcp", address, 3*time.Second)
	if err != nil {
		fmt.Printf("[Leader %s] [TCP-Verify] FAILED to connect to %s: %v\n",
			n.id[:8], nodeID[:8], err)
		return false
	}
	defer conn.Close()

	// Set a deadline for the verification handshake
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	// Send a heartbeat as a connectivity test
	// The node should accept the connection and we can verify it's responsive
	heartbeat := protocol.NewHeartbeatMsg(n.id, protocol.NodeType_LEADER, 0, n.viewState.GetViewNumber())
	if err := protocol.WriteTCPMessage(conn, heartbeat); err != nil {
		fmt.Printf("[Leader %s] [TCP-Verify] FAILED to write to %s: %v\n",
			n.id[:8], nodeID[:8], err)
		return false
	}

	fmt.Printf("[Leader %s] [TCP-Verify] SUCCESS - TCP connection to %s verified\n",
		n.id[:8], nodeID[:8])
	return true
}

// ============== Stream Failover ==============

// handleBrokerFailure handles stream failover when a broker fails
// Reassigns all streams from the failed broker to healthy brokers
func (n *Node) handleBrokerFailure(failedBrokerID string) {
	if !n.IsLeader() {
		return
	}

	fmt.Printf("[Leader %s] [Failover] Handling failure of broker %s\n", n.id[:8], failedBrokerID[:8])

	// Get all streams assigned to the failed broker
	streams := n.clusterState.GetStreamsByBroker(failedBrokerID)
	if len(streams) == 0 {
		fmt.Printf("[Leader %s] [Failover] No streams to reassign for broker %s\n", n.id[:8], failedBrokerID[:8])
		return
	}

	fmt.Printf("[Leader %s] [Failover] Found %d streams to reassign from broker %s\n",
		n.id[:8], len(streams), failedBrokerID[:8])

	// Reassign each stream to a new broker using consistent hashing
	for _, stream := range streams {
		// Get new broker for this topic
		newBrokerID, newBrokerAddr, err := n.GetBrokerForTopic(stream.Topic)
		if err != nil {
			// If consistent hash returns the failed broker (only one in ring), fall back to leader
			if newBrokerID == failedBrokerID {
				newBrokerID = n.id
				newBrokerAddr = n.address
				fmt.Printf("[Leader %s] [Failover] Falling back to self for topic %s\n", n.id[:8], stream.Topic)
			} else {
				log.Printf("[Leader %s] [Failover] Failed to find new broker for topic %s: %v\n",
					n.id[:8], stream.Topic, err)
				continue
			}
		}

		// Skip if new broker is the same as failed broker
		if newBrokerID == failedBrokerID {
			newBrokerID = n.id
			newBrokerAddr = n.address
			fmt.Printf("[Leader %s] [Failover] Falling back to self for topic %s (hash returned failed broker)\n",
				n.id[:8], stream.Topic)
		}

		// Reassign the stream
		if err := n.clusterState.ReassignStreamBroker(stream.Topic, newBrokerID, newBrokerAddr); err != nil {
			log.Printf("[Leader %s] [Failover] Failed to reassign stream for topic %s: %v\n",
				n.id[:8], stream.Topic, err)
			continue
		}

		fmt.Printf("[Leader %s] [Failover] Reassigned topic %s: %s -> %s @ %s\n",
			n.id[:8], stream.Topic, failedBrokerID[:8], newBrokerID[:8], newBrokerAddr)

		// Notify producer of new broker assignment
		if stream.ProducerId != "" {
			go n.notifyClientOfReassignment(stream.ProducerId, protocol.NodeType_PRODUCER, stream.Topic, newBrokerAddr, newBrokerID)
		}

		// Notify consumer of new broker assignment
		if stream.ConsumerId != "" {
			go n.notifyClientOfReassignment(stream.ConsumerId, protocol.NodeType_CONSUMER, stream.Topic, newBrokerAddr, newBrokerID)
		}
	}

	fmt.Printf("[Leader %s] [Failover] Completed stream failover for broker %s\n", n.id[:8], failedBrokerID[:8])
}

// notifyClientOfReassignment sends a REASSIGN_BROKER message to a client
// This notifies producers and consumers when their assigned broker changes
func (n *Node) notifyClientOfReassignment(clientID string, clientType protocol.NodeType, topic, newBrokerAddr, newBrokerID string) {
	if !n.IsLeader() {
		return
	}

	// Get client address
	var clientAddr string
	switch clientType {
	case protocol.NodeType_PRODUCER:
		producer, ok := n.clusterState.GetProducer(clientID)
		if !ok {
			log.Printf("[Leader %s] [Reassign] Producer %s not found in registry\n", n.id[:8], clientID[:8])
			return
		}
		clientAddr = producer.Address
	case protocol.NodeType_CONSUMER:
		consumer, ok := n.clusterState.GetConsumer(clientID)
		if !ok {
			log.Printf("[Leader %s] [Reassign] Consumer %s not found in registry\n", n.id[:8], clientID[:8])
			return
		}
		clientAddr = consumer.Address
	default:
		log.Printf("[Leader %s] [Reassign] Unknown client type: %v\n", n.id[:8], clientType)
		return
	}

	clientTypeName := "producer"
	if clientType == protocol.NodeType_CONSUMER {
		clientTypeName = "consumer"
	}

	fmt.Printf("[Leader %s] [Reassign] Notifying %s %s of new broker %s @ %s for topic %s\n",
		n.id[:8], clientTypeName, clientID[:8], newBrokerID[:8], newBrokerAddr, topic)

	// Connect to client
	conn, err := net.DialTimeout("tcp", clientAddr, 5*time.Second)
	if err != nil {
		log.Printf("[Leader %s] [Reassign] Failed to connect to %s %s: %v\n",
			n.id[:8], clientTypeName, clientID[:8], err)
		return
	}
	defer conn.Close()

	// Set deadline
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	// Create and send REASSIGN_BROKER message
	msg := protocol.NewReassignBrokerMsg(n.id, clientID, clientType, topic, newBrokerAddr, newBrokerID)
	if err := protocol.WriteTCPMessage(conn, msg); err != nil {
		log.Printf("[Leader %s] [Reassign] Failed to send REASSIGN_BROKER to %s %s: %v\n",
			n.id[:8], clientTypeName, clientID[:8], err)
		return
	}

	fmt.Printf("[Leader %s] -> REASSIGN_BROKER to %s %s (topic: %s, broker: %s)\n",
		n.id[:8], clientTypeName, clientID[:8], topic, newBrokerAddr)
}

// ============== Client Cleanup ==============

// cleanupProducer cleans up state for a disconnected/dead producer
func (n *Node) cleanupProducer(producerID string) {
	if !n.IsLeader() {
		return
	}

	fmt.Printf("[Leader %s] [Cleanup] Cleaning up producer %s\n", n.id[:8], producerID[:8])

	// Get the producer's topic
	producer, ok := n.clusterState.GetProducer(producerID)
	if !ok {
		// Producer already removed from registry
		return
	}

	topic := producer.Topic

	// Remove the stream assignment (this removes the whole stream since producer owns it)
	if err := n.clusterState.RemoveStream(topic); err != nil {
		// Stream might not exist, that's OK
		fmt.Printf("[Leader %s] [Cleanup] Stream for topic %s already removed or not found\n", n.id[:8], topic)
	} else {
		fmt.Printf("[Leader %s] [Cleanup] Removed stream for topic %s (producer disconnected)\n", n.id[:8], topic)
	}

	// Clean up data holdback queue for this producer
	n.dataHoldbackQueue.RemoveProducer(producerID)

	fmt.Printf("[Leader %s] [Cleanup] Producer %s cleanup complete\n", n.id[:8], producerID[:8])
}

// cleanupConsumer cleans up state for a disconnected/dead consumer
func (n *Node) cleanupConsumer(consumerID string) {
	if !n.IsLeader() {
		return
	}

	fmt.Printf("[Leader %s] [Cleanup] Cleaning up consumer %s\n", n.id[:8], consumerID[:8])

	// Get the consumer's subscribed topics
	consumer, ok := n.clusterState.GetConsumer(consumerID)
	if !ok {
		// Consumer already removed from registry
		return
	}

	// Remove consumer from each subscribed stream (but keep the stream for the producer)
	for _, topic := range consumer.Topics {
		if err := n.clusterState.RemoveConsumerFromStream(topic, consumerID); err != nil {
			// Stream might not exist or consumer not assigned, that's OK
			fmt.Printf("[Leader %s] [Cleanup] Could not remove consumer from stream %s: %v\n",
				n.id[:8], topic, err)
		} else {
			fmt.Printf("[Leader %s] [Cleanup] Removed consumer %s from stream %s\n",
				n.id[:8], consumerID[:8], topic)
		}
	}

	// Clean up consumer offset tracking
	n.removeConsumerOffsets(consumerID)

	fmt.Printf("[Leader %s] [Cleanup] Consumer %s cleanup complete\n", n.id[:8], consumerID[:8])
}
