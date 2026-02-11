package node

import (
	"fmt"
	"log"
	"net"
	"runtime"
	"time"

	"github.com/milossdjuric/logstream/internal/protocol"
	"github.com/milossdjuric/logstream/internal/state"
)

func (n *Node) handleTCPConnection(conn net.Conn) {
	keepConnectionOpen := false
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackLen := runtime.Stack(buf, false)
			log.Printf("[Node %s] PANIC in TCP handler: %v\n%s\n", n.id[:8], r, buf[:stackLen])
		}
		if !keepConnectionOpen {
			conn.Close()
		}
	}()

	msg, err := protocol.ReadTCPMessage(conn)
	if err != nil {
		log.Printf("[Node %s] Failed to read TCP message: %v\n", n.id[:8], err)
		return
	}

	msgType := msg.GetHeader().Type
	senderID := msg.GetHeader().SenderId
	fmt.Printf("[Node %s] <- %s from %s\n", n.id[:8], msgType, senderID[:8])

	switch m := msg.(type) {
	case *protocol.ProduceMsg:
		n.handleProduceRequest(m, conn)

	case *protocol.ConsumeMsg:
		n.handleConsumeRequest(m, conn)

	case *protocol.SubscribeMsg:
		n.handleSubscribe(m, conn)
		keepConnectionOpen = true

	case *protocol.HeartbeatMsg:
		n.handleTCPHeartbeat(m, conn)

	case *protocol.ElectionMsg:
		n.handleElection(m, nil)

	case *protocol.StateExchangeMsg:
		n.handleStateExchange(m, conn)

	case *protocol.ViewInstallMsg:
		n.handleViewInstall(m, conn)

	case *protocol.ReplicateAckMsg:
		n.handleReplicateAck(m, conn)

	case *protocol.JoinMsg:
		n.handleTCPJoin(m, conn)

	default:
		log.Printf("[Node %s] Unknown TCP message type: %T\n", n.id[:8], m)
	}
}

func (n *Node) handleHeartbeat(msg protocol.Message, sender *net.UDPAddr) {
	senderID := protocol.GetSenderID(msg)
	senderType := protocol.GetSenderType(msg)

	// Extract view number from heartbeat
	hbMsg, ok := msg.(*protocol.HeartbeatMsg)
	if !ok {
		log.Printf("[Node %s] Invalid heartbeat message type\n", n.id[:8])
		return
	}
	senderViewNumber := hbMsg.ViewNumber
	myViewNumber := n.viewState.GetViewNumber()

	// Reject heartbeats from old views (zombie leaders!)
	if senderViewNumber < myViewNumber {
		fmt.Printf("[Node %s] Ignoring heartbeat from %s with stale view %d (current: %d) - zombie leader?\n",
			n.id[:8], senderID[:8], senderViewNumber, myViewNumber)
		return
	}

	// Split-brain detection: two leaders in SAME view
	if n.IsLeader() && senderType == protocol.NodeType_LEADER && senderID != n.id {
		// Only trigger election if views are equal (genuine split-brain)
		if senderViewNumber == myViewNumber {
			senderAddress := hbMsg.SenderAddress

			fmt.Printf("[Leader %s] SPLIT-BRAIN DETECTED from %s (same view %d, addr=%s)\n",
				n.id[:8], senderID[:8], myViewNumber, senderAddress)

			// Register the other leader so the election includes both nodes.
			// Without this, each node only has itself in registry (1 broker)
			// and the election takes the "sole survivor" path, creating an infinite loop.
			if senderAddress != "" {
				if _, exists := n.clusterState.GetBroker(senderID); !exists {
					fmt.Printf("[Leader %s] Registering other leader %s at %s for election\n",
						n.id[:8], senderID[:8], senderAddress)
					n.clusterState.RegisterBroker(senderID, senderAddress, true)
				}
			}

			if !n.election.IsInProgress() {
				go func() {
					fmt.Printf("[Leader %s] Starting snap election to resolve split-brain\n", n.id[:8])
					if err := n.StartElection(); err != nil {
						log.Printf("[Leader %s] Snap election failed: %v\n", n.id[:8], err)
					}
				}()
			}
		} else if senderViewNumber > myViewNumber {
			// Other leader has higher view - we're the zombie!
			fmt.Printf("[Node %s] ZOMBIE DETECTED: Received heartbeat from leader %s with higher view %d (mine: %d)\n",
				n.id[:8], senderID[:8], senderViewNumber, myViewNumber)
			
			fmt.Printf("[Node %s] Stepping down from stale leader role and attempting to rejoin cluster\n", n.id[:8])
			
			// Step down from leader role immediately
			n.becomeFollower()
			
			// Attempt to rejoin cluster via discovery
			go func() {
				// Brief delay to let new cluster stabilize
				time.Sleep(2 * time.Second)
				
				fmt.Printf("[Node %s] Attempting to rejoin cluster via discovery broadcast...\n", n.id[:8])
				
				if err := n.discoverCluster(); err != nil {
					log.Printf("[Node %s] Failed to rejoin cluster: %v\n", n.id[:8], err)
					log.Printf("[Node %s] Will retry on next heartbeat detection\n", n.id[:8])
					return
				}
				
				fmt.Printf("[Node %s] Successfully rejoined cluster as follower (new view: %d)\n", 
					n.id[:8], n.viewState.GetViewNumber())
			}()
		}
		return
	}

	// Check if we (as follower) have a stale view
	if !n.IsLeader() && senderType == protocol.NodeType_LEADER && senderViewNumber > myViewNumber {
		fmt.Printf("[Node %s] STALE FOLLOWER DETECTED: Leader has view %d, mine is %d - rejoining cluster\n",
			n.id[:8], senderViewNumber, myViewNumber)
		
		// Attempt to rejoin cluster via discovery to get updated state
		go func() {
			time.Sleep(1 * time.Second)
			
			fmt.Printf("[Node %s] Follower attempting to rejoin cluster via discovery...\n", n.id[:8])
			
			if err := n.discoverCluster(); err != nil {
				log.Printf("[Node %s] Follower failed to rejoin cluster: %v\n", n.id[:8], err)
				return
			}
			
			fmt.Printf("[Node %s] Follower successfully rejoined cluster (new view: %d)\n", 
				n.id[:8], n.viewState.GetViewNumber())
		}()
	}
	
	n.clusterState.UpdateBrokerHeartbeat(senderID)

	if !n.IsLeader() && senderType == protocol.NodeType_LEADER {
		now := time.Now()
		// Phi accrual detector disabled - using simple timeout-based detection
		// n.failureDetector.Update(senderID, now)
		
		// Track last leader heartbeat time for timeout-based failure detection
		n.lastLeaderHeartbeatMu.Lock()
		n.lastLeaderHeartbeat = now
		n.lastLeaderHeartbeatMu.Unlock()
		
		fmt.Printf("[Node %s] <- HEARTBEAT from %s (type: %s, view: %d)\n", n.id[:8], senderID[:8], senderType, senderViewNumber)
	}
}

func (n *Node) handleReplication(msg *protocol.ReplicateMsg, sender *net.UDPAddr) {
	seqNum := protocol.GetSequenceNum(msg)
	senderID := protocol.GetSenderID(msg)

	if n.IsFrozen() {
		return
	}

	msgViewNumber := msg.ViewNumber
	msgLeaderID := msg.LeaderId
	currentViewNumber := n.viewState.GetViewNumber()
	currentLeaderID := n.viewState.GetLeaderID()

	if msgViewNumber > 0 && msgViewNumber < currentViewNumber {
		return
	}

	if msgViewNumber > currentViewNumber {
		fmt.Printf("[Node %s] Message from future view %d (current: %d)\n",
			n.id[:8], msgViewNumber, currentViewNumber)
	}

	if !n.IsLeader() && msgViewNumber > 0 && msgLeaderID != "" && currentLeaderID != "" && msgLeaderID != currentLeaderID {
		fmt.Printf("[Node %s] Leader mismatch: expected %s, got %s\n",
			n.id[:8], safeIDStr(currentLeaderID), safeIDStr(msgLeaderID))
		return
	}

	// Leaders should NEVER process REPLICATE messages
	// - Own messages: would corrupt state by deserializing our own snapshot
	// - Other leader's messages: indicates split-brain, trigger election
	if n.IsLeader() {
		if senderID == n.id {
			// Ignore our own REPLICATE messages (multicast loopback)
			return
		}

		// Split-brain: received REPLICATE from another leader
		fmt.Printf("[Node %s] SPLIT-BRAIN: Received REPLICATE from %s\n", n.id[:8], senderID[:8])

		otherBrokerAddr := ""
		tempState := state.NewClusterState()
		if err := tempState.Deserialize(msg.StateSnapshot); err == nil {
			if broker, ok := tempState.GetBroker(senderID); ok {
				otherBrokerAddr = broker.Address
			}
		}

		if otherBrokerAddr != "" {
			if _, exists := n.clusterState.GetBroker(senderID); !exists {
				n.clusterState.RegisterBroker(senderID, otherBrokerAddr, true)
			}
		}

		if !n.election.IsInProgress() {
			go func() {
				if err := n.StartElection(); err != nil {
					log.Printf("[Node %s] Snap election failed: %v\n", n.id[:8], err)
				}
			}()
		}
		return
	}

	fmt.Printf("[Node %s] <- REPLICATE seq=%d from %s\n", n.id[:8], seqNum, senderID[:8])

	holdbackMsg := &state.HoldbackMessage{
		SequenceNum:   seqNum,
		StateSnapshot: msg.StateSnapshot,
		UpdateType:    msg.UpdateType,
	}

	if err := n.holdbackQueue.Enqueue(holdbackMsg); err != nil {
		log.Printf("[Node %s] Holdback enqueue failed: %v\n", n.id[:8], err)
	}
}

func (n *Node) handleNack(msg *protocol.NackMsg, sender *net.UDPAddr) {
	fmt.Printf("[%s] <- NACK seq=%d-%d from %s\n",
		n.id[:8], msg.FromSeq, msg.ToSeq, protocol.GetSenderID(msg)[:8])
}

func (n *Node) handleReplicateAck(msg *protocol.ReplicateAckMsg, conn net.Conn) {
	senderID := protocol.GetSenderID(msg)
	ackedSeq := msg.AckedSeq
	success := msg.Success

	if !n.IsLeader() {
		return
	}

	n.trackReplicateAck(senderID, ackedSeq, success)
}

func (n *Node) trackReplicateAck(followerID string, seqNum int64, success bool) {
	n.replicateAckMu.Lock()
	defer n.replicateAckMu.Unlock()

	if n.replicateAcks == nil {
		n.replicateAcks = make(map[int64]map[string]bool)
	}
	if n.replicateAcks[seqNum] == nil {
		n.replicateAcks[seqNum] = make(map[string]bool)
	}

	n.replicateAcks[seqNum][followerID] = success

	if len(n.replicateAcks) > 100 {
		minSeq := seqNum - 100
		for seq := range n.replicateAcks {
			if seq < minSeq {
				delete(n.replicateAcks, seq)
			}
		}
	}
}

// handleProduceRequest handles PRODUCE requests from producers
// Implements one-to-one producer-to-topic mapping and broker assignment via consistent hashing
func (n *Node) handleProduceRequest(msg *protocol.ProduceMsg, conn net.Conn) {
	producerID := protocol.GetSenderID(msg)
	topic := msg.Topic
	address := msg.ProducerAddress

	fmt.Printf("[Leader %s] <- PRODUCE from %s (topic: %s, addr: %s)\n",
		n.id[:8], producerID[:8], topic, address)

	// Only the leader handles PRODUCE registration
	if !n.IsLeader() {
		fmt.Printf("[Node %s] PRODUCE from %s REJECTED - not the leader (client will re-discover)\n",
			n.id[:8], producerID[:8])
		ack := &protocol.ProduceMessage{
			Header: &protocol.MessageHeader{
				Type:        protocol.MessageType_PRODUCE,
				Timestamp:   time.Now().UnixNano(),
				SenderId:    n.id,
				SequenceNum: 0,
				SenderType:  protocol.NodeType_BROKER,
			},
			Topic:           "",
			ProducerAddress: "",
		}
		protocol.WriteTCPMessage(conn, &protocol.ProduceMsg{ProduceMessage: ack})
		return
	}

	// View-synchronous check: send immediate failure during freeze.
	// Client will retry with backoff (circuit breaker pattern).
	if n.IsFrozen() {
		fmt.Printf("[Leader %s] PRODUCE from %s REJECTED - operations frozen for view change (client will retry)\n",
			n.id[:8], producerID[:8])
		ack := &protocol.ProduceMessage{
			Header: &protocol.MessageHeader{
				Type:        protocol.MessageType_PRODUCE,
				Timestamp:   time.Now().UnixNano(),
				SenderId:    n.id,
				SequenceNum: 0,
				SenderType:  protocol.NodeType_LEADER,
			},
			Topic:           "",
			ProducerAddress: "",
		}
		protocol.WriteTCPMessage(conn, &protocol.ProduceMsg{ProduceMessage: ack})
		return
	}

	// One-to-one check: verify topic doesn't already have a DIFFERENT producer.
	// Allow re-registration from the same producer ID (reconnect after heartbeat drift).
	if stream, exists := n.clusterState.GetStreamAssignment(topic); exists && stream.ProducerId != "" && stream.ProducerId != producerID {
		log.Printf("[Leader %s] PRODUCE from %s REJECTED - topic %s already has producer %s (one-to-one mapping)\n",
			n.id[:8], producerID[:8], topic, stream.ProducerId[:8])
		// Send failure response
		ack := &protocol.ProduceMessage{
			Header: &protocol.MessageHeader{
				Type:        protocol.MessageType_PRODUCE,
				Timestamp:   time.Now().UnixNano(),
				SenderId:    n.id,
				SequenceNum: 0,
				SenderType:  protocol.NodeType_LEADER,
			},
			Topic:           "",
			ProducerAddress: "", // Empty indicates failure
		}
		protocol.WriteTCPMessage(conn, &protocol.ProduceMsg{ProduceMessage: ack})
		return
	}

	// Register producer
	if err := n.clusterState.RegisterProducer(producerID, address, topic); err != nil {
		log.Printf("[Leader %s] Failed to register producer: %v\n", n.id[:8], err)
		// Send failure response
		ack := &protocol.ProduceMessage{
			Header: &protocol.MessageHeader{
				Type:        protocol.MessageType_PRODUCE,
				Timestamp:   time.Now().UnixNano(),
				SenderId:    n.id,
				SequenceNum: 0,
				SenderType:  protocol.NodeType_LEADER,
			},
			Topic:           topic,
			ProducerAddress: "",
		}
		protocol.WriteTCPMessage(conn, &protocol.ProduceMsg{ProduceMessage: ack})
		return
	}

	// Check if this is a re-registration (same producer reconnecting for same topic)
	var brokerID, brokerAddress string
	if existingStream, exists := n.clusterState.GetStreamAssignment(topic); exists && existingStream.ProducerId == producerID {
		// Re-registration: reuse existing stream assignment
		brokerID = existingStream.AssignedBrokerId
		brokerAddress = existingStream.BrokerAddress
		fmt.Printf("[Leader %s] Producer %s re-registering for topic %s (reusing broker %s)\n",
			n.id[:8], producerID[:8], topic, brokerID[:8])
	} else {
		// New registration: select broker using consistent hashing
		var err error
		brokerID, brokerAddress, err = n.GetBrokerForTopic(topic)
		if err != nil {
			log.Printf("[Leader %s] Failed to assign broker for topic %s: %v\n", n.id[:8], topic, err)
			// Fall back to self (leader) if no brokers available
			brokerID = n.id
			brokerAddress = n.address
			fmt.Printf("[Leader %s] Falling back to self as broker for topic %s\n", n.id[:8], topic)
		}

		// Create stream assignment (tracks one-to-one producer-broker mapping)
		if err := n.clusterState.AssignStream(topic, producerID, brokerID, brokerAddress); err != nil {
			log.Printf("[Leader %s] Failed to create stream assignment: %v\n", n.id[:8], err)
			// Send failure response
			ack := &protocol.ProduceMessage{
				Header: &protocol.MessageHeader{
					Type:        protocol.MessageType_PRODUCE,
					Timestamp:   time.Now().UnixNano(),
					SenderId:    n.id,
					SequenceNum: 0,
					SenderType:  protocol.NodeType_LEADER,
				},
				Topic:           topic,
				ProducerAddress: "",
			}
			protocol.WriteTCPMessage(conn, &protocol.ProduceMsg{ProduceMessage: ack})
			return
		}
	}

	// Replicate state change immediately
	if err := n.replicateAllState(); err != nil {
		log.Printf("[Leader %s] Failed to replicate after producer registration: %v\n", n.id[:8], err)
	}

	// Send success response with assigned broker address
	ack := &protocol.ProduceMessage{
		Header: &protocol.MessageHeader{
			Type:        protocol.MessageType_PRODUCE,
			Timestamp:   time.Now().UnixNano(),
			SenderId:    n.id,
			SequenceNum: 0,
			SenderType:  protocol.NodeType_LEADER,
		},
		Topic:           topic,
		ProducerAddress: brokerAddress, // Producer will send DATA to this address
	}

	if err := protocol.WriteTCPMessage(conn, &protocol.ProduceMsg{ProduceMessage: ack}); err != nil {
		log.Printf("[Leader %s] Failed to send PRODUCE_ACK: %v\n", n.id[:8], err)
		return
	}

	fmt.Printf("[Leader %s] -> PRODUCE_ACK to %s (topic: %s, assigned broker: %s @ %s)\n",
		n.id[:8], producerID[:8], topic, brokerID[:8], brokerAddress)
}

// handleTCPHeartbeat handles TCP heartbeats from producers, consumers, or brokers
func (n *Node) handleTCPHeartbeat(msg *protocol.HeartbeatMsg, conn net.Conn) {
	senderID := protocol.GetSenderID(msg)
	senderType := protocol.GetSenderType(msg)

	fmt.Printf("[%s] <- HEARTBEAT (TCP) from %s (type: %s)\n",
		n.id[:8], senderID[:8], senderType)

	switch senderType {
	case protocol.NodeType_PRODUCER:
		// Producer heartbeat - update timestamp
		if n.IsLeader() {
			n.clusterState.UpdateProducerHeartbeat(senderID)
			fmt.Printf("[Leader %s] Updated producer %s heartbeat\n", n.id[:8], senderID[:8])
		}

	case protocol.NodeType_CONSUMER:
		// Consumer heartbeat - update timestamp
		if n.IsLeader() {
			n.clusterState.UpdateConsumerHeartbeat(senderID)
			fmt.Printf("[Leader %s] Updated consumer %s heartbeat\n", n.id[:8], senderID[:8])
		}

	case protocol.NodeType_BROKER:
		// Broker heartbeat to leader - update timestamp
		if n.IsLeader() {
			n.clusterState.UpdateBrokerHeartbeat(senderID)
			fmt.Printf("[Leader %s] Updated broker %s heartbeat (TCP)\n", n.id[:8], senderID[:8])
		}

	case protocol.NodeType_LEADER:
		// Leader heartbeat to client - just acknowledge (clients track this)
		fmt.Printf("[%s] Received leader heartbeat (TCP)\n", n.id[:8])
	}
}

// applyRegistryUpdate applies a registry update from the holdback queue
func (n *Node) applyRegistryUpdate(msg *state.HoldbackMessage) error {
	fmt.Printf("\n[Node %s] ========================================\n", n.id[:8])
	fmt.Printf("[Node %s] Applying registry update seq=%d type=%s\n",
		n.id[:8], msg.SequenceNum, msg.UpdateType)

	currentLeaderID := n.getLeaderIDFromRegistry()
	leaderStr := "(none)"
	if currentLeaderID != "" && len(currentLeaderID) >= 8 {
		leaderStr = currentLeaderID[:8]
	}
	fmt.Printf("[Node %s] Before update: IsLeader=%v, CurrentLeader='%s'\n",
		n.id[:8], n.IsLeader(), leaderStr)

	// Deserialize cluster state
	if err := n.clusterState.Deserialize(msg.StateSnapshot); err != nil {
		return fmt.Errorf("failed to apply update: %w", err)
	}

	// Sync broker ring for stream assignment (keeps ring in sync with cluster state)
	n.syncBrokerRing()

	// Update last applied sequence number
	if msg.SequenceNum > n.lastAppliedSeqNum {
		n.lastAppliedSeqNum = msg.SequenceNum
		fmt.Printf("[Node %s] Updated lastAppliedSeqNum to %d\n", n.id[:8], n.lastAppliedSeqNum)
	}

	// Print cluster state status after applying update
	n.clusterState.PrintStatus()

	// Update stored leader address from registry if we're a follower
	if !n.IsLeader() {
		brokers := n.clusterState.ListBrokers()
		for _, brokerID := range brokers {
			if broker, ok := n.clusterState.GetBroker(brokerID); ok && broker.IsLeader {
				if n.leaderAddress != broker.Address {
					fmt.Printf("[Node %s] Updated stored leader address: %s -> %s\n",
						n.id[:8], n.leaderAddress, broker.Address)
					n.leaderAddress = broker.Address
				}
				break
			}
		}
	}

	// Phi accrual detector automatically tracks leader heartbeats - no explicit initialization needed
	if !n.IsLeader() {
		newLeaderID := n.getLeaderIDFromRegistry()
		if newLeaderID != "" {
			fmt.Printf("[Node %s] Phi accrual detector will track leader: %s\n",
				n.id[:8], newLeaderID[:8])
		}
	} else {
		fmt.Printf("[Node %s] I'm the leader - phi detector inactive\n", n.id[:8])
	}

	fmt.Printf("[Node %s] ========================================\n\n", n.id[:8])

	// Send REPLICATE_ACK to leader (passive replication - backups send acknowledgement)
	// Only followers send ACKs (leaders don't ACK their own replication)
	if !n.IsLeader() && n.leaderAddress != "" {
		go n.sendReplicateAck(msg.SequenceNum, true, "")
	}

	return nil
}

// sendReplicateAck sends REPLICATE_ACK to the leader
// Called after successfully applying a REPLICATE message
func (n *Node) sendReplicateAck(seqNum int64, success bool, errorMessage string) {
	if n.leaderAddress == "" {
		fmt.Printf("[Node %s] Cannot send REPLICATE_ACK - no leader address known\n", n.id[:8])
		return
	}

	viewNumber := n.viewState.GetViewNumber()
	ackMsg := protocol.NewReplicateAckMsg(n.id, seqNum, viewNumber, success, errorMessage)

	// Connect to leader via TCP
	conn, err := net.DialTimeout("tcp", n.leaderAddress, 5*time.Second)
	if err != nil {
		fmt.Printf("[Node %s] Failed to connect to leader for REPLICATE_ACK: %v\n", n.id[:8], err)
		return
	}
	defer conn.Close()

	if err := protocol.WriteTCPMessage(conn, ackMsg); err != nil {
		fmt.Printf("[Node %s] Failed to send REPLICATE_ACK: %v\n", n.id[:8], err)
		return
	}

	fmt.Printf("[Node %s] -> REPLICATE_ACK seq=%d to leader %s\n", n.id[:8], seqNum, n.leaderAddress)
}

// safeIDStr safely formats a node ID string for logging
// Returns first 8 characters if long enough, or "(empty)" if empty
func safeIDStr(id string) string {
	if id == "" {
		return "(empty)"
	}
	if len(id) >= 8 {
		return id[:8]
	}
	return id
}

// handleTCPJoin handles JOIN messages received via TCP (for explicit leader mode)
// This allows nodes to join when broadcast discovery doesn't work (e.g., WiFi AP isolation)
func (n *Node) handleTCPJoin(msg *protocol.JoinMsg, conn net.Conn) {
	senderID := protocol.GetSenderID(msg)
	fmt.Printf("[Node %s] <- JOIN (TCP) from %s (addr=%s)\n", n.id[:8], senderID[:8], msg.Address)

	// Only leader should handle JOIN requests
	if !n.IsLeader() {
		fmt.Printf("[Node %s] REJECTING TCP JOIN from %s - not leader\n", n.id[:8], senderID[:8])
		// Send error response
		errorResp := protocol.NewJoinResponseMsg(n.id, "", "", nil)
		protocol.WriteTCPMessage(conn, errorResp)
		return
	}

	// Ignore JOIN from self
	if senderID == n.id {
		fmt.Printf("[Leader %s] IGNORING TCP JOIN from self\n", n.id[:8])
		return
	}

	// Register new broker via view change
	newNodeID := senderID
	n.performViewChangeForNodeJoin(newNodeID, msg.Address)

	// Send JOIN_RESPONSE
	brokers := n.clusterState.ListBrokers()
	var brokerAddrs []string
	for _, bid := range brokers {
		if b, ok := n.clusterState.GetBroker(bid); ok {
			brokerAddrs = append(brokerAddrs, b.Address)
		}
	}

	response := protocol.NewJoinResponseMsg(
		n.id,
		n.address,
		n.config.MulticastGroup,
		brokerAddrs,
	)

	if err := protocol.WriteTCPMessage(conn, response); err != nil {
		log.Printf("[Leader %s] Failed to send JOIN_RESPONSE (TCP): %v\n", n.id[:8], err)
		return
	}

	fmt.Printf("[Leader %s] -> JOIN_RESPONSE (TCP) to %s (multicast: %s)\n",
		n.id[:8], senderID[:8], n.config.MulticastGroup)
}
