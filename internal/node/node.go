package node

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/milossdjuric/logstream/internal/config"
	"github.com/milossdjuric/logstream/internal/protocol"
	"github.com/milossdjuric/logstream/internal/state"
)

// Node represents a node in the cluster (can be leader or follower)
type Node struct {
	// Id, address, and config
	id      string
	address string
	config  *config.Config

	// Role in cluster, can change during runtime via election
	mu       sync.RWMutex
	isLeader bool

	// State, as in registry and holdback queue for FIFO ordering
	registry      *state.Registry
	holdbackQueue *state.RegistryHoldbackQueue

	// Client registries (only leader uses these)
	producers *state.ProducerRegistry
	consumers *state.ConsumerRegistry

	// Track last replicated sequence to avoid sending duplicates
	lastReplicatedSeq int64

	// Network connections
	multicastReceiver *protocol.MulticastConnection
	multicastSender   *protocol.MulticastConnection
	broadcastListener *protocol.BroadcastConnection
	tcpListener       net.Listener

	// Control channels for role-specific goroutines
	stopLeaderDuties   chan struct{}
	stopFollowerDuties chan struct{}
}

// NewNode creates a new node
func NewNode(cfg *config.Config) *Node {
	node := &Node{
		id:                 protocol.GenerateNodeID(cfg.NodeAddress),
		address:            cfg.NodeAddress,
		config:             cfg,
		isLeader:           false, // Default to follower, determined during discovery
		registry:           state.NewRegistry(),
		producers:          state.NewProducerRegistry(),
		consumers:          state.NewConsumerRegistry(),
		lastReplicatedSeq:  0,
		stopLeaderDuties:   make(chan struct{}),
		stopFollowerDuties: make(chan struct{}),
	}

	// Initialize holdback queue for FIFO registry updates
	node.holdbackQueue = state.NewRegistryHoldbackQueue(node.applyRegistryUpdate)

	return node
}

// Initialize the node (discovers cluster and starts appropriate role)
func (n *Node) Start() error {
	fmt.Printf("[Node %s] Starting...\n", n.id[:8])

	// Discover existing cluster
	if err := n.discoverCluster(); err != nil {
		// If no cluster found, become leader
		fmt.Printf("[Node %s] No existing cluster, becoming leader\n", n.id[:8])
		n.becomeLeader()
	} else {
		// If cluster found, become follower
		fmt.Printf("[Node %s] Joined existing cluster as follower\n", n.id[:8])
		n.becomeFollower()

	}

	// Setup network multicast
	if err := n.setupMulticast(); err != nil {
		return err
	}

	// If leader, start TCP listener
	if n.IsLeader() {
		if err := n.startTCPListener(); err != nil {
			return fmt.Errorf("failed to start TCP listener: %w", err)
		}
	}

	// Start multicast listener
	go n.listenMulticast()

	// Start role-specific duties, either leader or follower
	if n.IsLeader() {
		go n.runLeaderDuties()
	} else {
		go n.runFollowerDuties()
	}

	fmt.Printf("[Node %s] Started at %s (leader=%v)\n", n.id[:8], n.address, n.IsLeader())
	return nil
}

// IsLeader returns current role
func (n *Node) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.isLeader
}

// becomeLeader transitions this node to leader role
func (n *Node) becomeLeader() {
	n.mu.Lock()
	wasLeader := n.isLeader
	n.isLeader = true
	n.mu.Unlock()

	if !wasLeader {
		fmt.Printf("\n[Node %s] ========================================\n", n.id[:8])
		fmt.Printf("[Node %s] Becoming LEADER\n", n.id[:8])
		fmt.Printf("[Node %s] ========================================\n\n", n.id[:8])

		// Stop goroutine for follower duties
		select {
		case n.stopFollowerDuties <- struct{}{}:
		default:
		}

		// Update registry
		n.registry.RegisterBroker(n.id, n.address, true)

		// Initialize last replicated sequence
		n.lastReplicatedSeq = n.registry.GetSequenceNum()

		// Start broadcast listener for new nodes joining
		if n.broadcastListener == nil {
			listener, err := protocol.CreateBroadcastListener(n.config.BroadcastPort)
			if err != nil {
				log.Printf("[Node %s] Failed to start broadcast listener: %v\n", n.id[:8], err)
			} else {
				n.broadcastListener = listener
				go n.listenForBroadcastJoins()
				fmt.Printf("[Leader %s] Broadcast listener ready on port %d\n", n.id[:8], n.config.BroadcastPort)
			}
		}
	}
}

// becomeFollower transitions this node to follower role
func (n *Node) becomeFollower() {

	// Stop leader duties if running
	n.mu.Lock()
	wasLeader := n.isLeader
	n.isLeader = false
	n.mu.Unlock()

	if wasLeader {
		fmt.Printf("[Node %s] Becoming FOLLOWER\n", n.id[:8])

		// Stop leader duties
		select {
		case n.stopLeaderDuties <- struct{}{}:
		default:
		}

		// Update registry
		n.registry.RegisterBroker(n.id, n.address, false)

		// Stop broadcast listener
		if n.broadcastListener != nil {
			n.broadcastListener.Close()
			n.broadcastListener = nil
		}

		// Stop TCP listener
		if n.tcpListener != nil {
			n.tcpListener.Close()
			n.tcpListener = nil
		}
	}
}

func (n *Node) discoverCluster() error {
	fmt.Printf("[Node %s] Discovering cluster...\n", n.id[:8])

	// Create broadcast config
	config := protocol.DefaultBroadcastConfig()
	config.MaxRetries = 1
	config.Timeout = 2 * time.Second

	// Discover cluster
	response, err := protocol.DiscoverClusterWithRetry(
		n.id,
		protocol.NodeType_BROKER,
		n.address,
		config,
	)

	// Check for errors
	if err != nil {
		return err
	}

	//  Update configuration
	fmt.Printf("[Node %s] Found cluster, Leader: %s\n", n.id[:8], response.LeaderAddress)
	n.config.MulticastGroup = response.MulticastGroup

	return nil
}

func (n *Node) setupMulticast() error {
	// protocol.JoinMulticastGroup provided by protocol package
	receiver, err := protocol.JoinMulticastGroup(
		n.config.MulticastGroup,
		n.config.NetworkInterface,
	)
	if err != nil {
		return fmt.Errorf("failed to join multicast: %w", err)
	}
	n.multicastReceiver = receiver

	// protocol.CreateMulticastSender provided by protocol package
	senderAddr := fmt.Sprintf("%s:0", n.config.NetworkInterface)
	sender, err := protocol.CreateMulticastSender(senderAddr)
	if err != nil {
		n.multicastReceiver.Close()
		return fmt.Errorf("failed to create multicast sender: %w", err)
	}
	n.multicastSender = sender

	return nil
}

// Start TCP listener for producer/consumer connections
func (n *Node) startTCPListener() error {
	// Extract port from node address and use port+1000 for TCP
	host, port, err := net.SplitHostPort(n.address)
	if err != nil {
		return fmt.Errorf("invalid address: %w", err)
	}

	// Use same port for TCP (port 8001 for both UDP discovery and TCP clients)
	tcpAddr := fmt.Sprintf("%s:%s", host, port)

	// Listen for TCP connections
	listener, err := net.Listen("tcp", tcpAddr)
	if err != nil {
		return fmt.Errorf("failed to start TCP listener: %w", err)
	}

	n.tcpListener = listener
	fmt.Printf("[Leader %s] TCP listener started on %s\n", n.id[:8], listener.Addr())

	// Accept connections in background
	go n.acceptTCPConnections()

	return nil
}

// Accept incoming TCP connections
func (n *Node) acceptTCPConnections() {
	for {
		conn, err := n.tcpListener.Accept()
		if err != nil {
			// Check if we've been shut down
			if n.tcpListener != nil {
				log.Printf("[Leader %s] TCP accept error: %v\n", n.id[:8], err)
			}
			return
		}

		// Handle connection in goroutine
		go n.handleTCPConnection(conn)
	}
}

// Handle incoming TCP connections
func (n *Node) handleTCPConnection(conn net.Conn) {
	defer conn.Close()

	// Read message
	msg, err := protocol.ReadTCPMessage(conn)
	if err != nil {
		log.Printf("[Leader %s] Failed to read TCP message: %v\n", n.id[:8], err)
		return
	}

	// Route based on message type
	switch m := msg.(type) {
	case *protocol.ProduceMsg:
		n.handleProduceRequest(m, conn)

	case *protocol.ConsumeMsg:
		n.handleConsumeRequest(m, conn)

	default:
		log.Printf("[Leader %s] Unknown TCP message type: %T\n", n.id[:8], m)
	}
}

// Handle PRODUCE request from producer
func (n *Node) handleProduceRequest(msg *protocol.ProduceMsg, conn net.Conn) {
	producerID := protocol.GetSenderID(msg)
	topic := msg.Topic
	address := msg.ProducerAddress

	fmt.Printf("[Leader %s] <- PRODUCE from %s (topic: %s, addr: %s)\n",
		n.id[:8], producerID[:8], topic, address)

	// Register producer
	if err := n.producers.Register(producerID, address, topic); err != nil {
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

	// TODO: Select broker using rendezvous hashing
	// For now, assign to self (leader)
	assignedBroker := n.address

	// Send success response with assigned broker
	ack := &protocol.ProduceMessage{
		Header: &protocol.MessageHeader{
			Type:        protocol.MessageType_PRODUCE,
			Timestamp:   time.Now().UnixNano(),
			SenderId:    n.id,
			SequenceNum: 0,
			SenderType:  protocol.NodeType_LEADER,
		},
		Topic:           topic,
		ProducerAddress: assignedBroker,
	}

	if err := protocol.WriteTCPMessage(conn, &protocol.ProduceMsg{ProduceMessage: ack}); err != nil {
		log.Printf("[Leader %s] Failed to send PRODUCE_ACK: %v\n", n.id[:8], err)
		return
	}

	fmt.Printf("[Leader %s] -> PRODUCE_ACK to %s (assigned broker: %s)\n",
		n.id[:8], producerID[:8], assignedBroker)
}

// Handle CONSUME request from consumer
func (n *Node) handleConsumeRequest(msg *protocol.ConsumeMsg, conn net.Conn) {
	consumerID := protocol.GetSenderID(msg)
	topic := msg.Topic
	address := msg.ConsumerAddress

	fmt.Printf("[Leader %s] <- CONSUME from %s (topic: %s, addr: %s)\n",
		n.id[:8], consumerID[:8], topic, address)

	// Register consumer
	if err := n.consumers.Register(consumerID, address); err != nil {
		log.Printf("[Leader %s] Failed to register consumer: %v\n", n.id[:8], err)

		// Send failure response if registration fails
		ack := &protocol.ConsumeMessage{
			Header: &protocol.MessageHeader{
				Type:        protocol.MessageType_CONSUME,
				Timestamp:   time.Now().UnixNano(),
				SenderId:    n.id,
				SequenceNum: 0,
				SenderType:  protocol.NodeType_LEADER,
			},
			Topic:           "",
			ConsumerAddress: "",
		}
		protocol.WriteTCPMessage(conn, &protocol.ConsumeMsg{ConsumeMessage: ack})
		return
	}

	// Subscribe consumer to topic
	if err := n.consumers.Subscribe(consumerID, topic); err != nil {
		log.Printf("[Leader %s] Failed to subscribe consumer: %v\n", n.id[:8], err)
		return
	}

	// Send success response
	ack := &protocol.ConsumeMessage{
		Header: &protocol.MessageHeader{
			Type:        protocol.MessageType_CONSUME,
			Timestamp:   time.Now().UnixNano(),
			SenderId:    n.id,
			SequenceNum: 0,
			SenderType:  protocol.NodeType_LEADER,
		},
		Topic:           topic,
		ConsumerAddress: n.address,
	}

	if err := protocol.WriteTCPMessage(conn, &protocol.ConsumeMsg{ConsumeMessage: ack}); err != nil {
		log.Printf("[Leader %s] Failed to send CONSUME_ACK: %v\n", n.id[:8], err)
		return
	}

	fmt.Printf("[Leader %s] -> CONSUME_ACK to %s\n", n.id[:8], consumerID[:8])

	// Keep connection open and start sending results
	go n.streamResultsToConsumer(consumerID, topic, conn)
}

// Stream results to consumer (placeholder for now)
func (n *Node) streamResultsToConsumer(consumerID, topic string, conn net.Conn) {
	fmt.Printf("[Leader %s] Started result stream to consumer %s (topic: %s)\n",
		n.id[:8], consumerID[:8], topic)

	// TODO: Implement actual result streaming
	// For now, just keep connection alive and send heartbeats
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Update consumer heartbeat
			n.consumers.UpdateHeartbeat(consumerID)
		}
	}
}

// Listen for incoming multicast messages
func (n *Node) listenMulticast() {
	for {
		// protocol.MulticastConnection.ReadMessage() provided by protocol package
		msg, sender, err := n.multicastReceiver.ReadMessage()
		if err != nil {
			// Only log actual errors, not "not a protobuf message"
			if err.Error() != "not a protobuf message" {
				log.Printf("[Node %s] Multicast read error: %v\n", n.id[:8], err)
			}
			continue
		}

		// Route message based on type
		switch m := msg.(type) {
		case *protocol.HeartbeatMsg:
			n.handleHeartbeat(msg, sender)

		case *protocol.ReplicateMsg:
			n.handleReplication(m, sender)

		case *protocol.ElectionMsg:
			n.handleElection(m, sender)

		case *protocol.NackMsg:
			n.handleNack(m, sender)

		case *protocol.DataMsg:
			n.handleData(m, sender)
		}
	}
}

// Handle heartbeat messages
func (n *Node) handleHeartbeat(msg protocol.Message, sender *net.UDPAddr) {
	// Get sender ID from message
	fmt.Printf("[%s] <- HEARTBEAT from %s (sender: %s)\n",
		n.id[:8], protocol.GetSenderID(msg)[:8], sender)

	// Update heartbeat timestamp in state.Registry
	n.registry.UpdateHeartbeat(protocol.GetSenderID(msg))
}

// Handle replication messages
func (n *Node) handleReplication(msg *protocol.ReplicateMsg, sender *net.UDPAddr) {
	// Get sequence number from message
	seqNum := protocol.GetSequenceNum(msg)
	fmt.Printf("[%s] <- REPLICATE seq=%d type=%s from %s\n",
		n.id[:8], seqNum, msg.UpdateType, protocol.GetSenderID(msg)[:8])

	// Enqueue for FIFO delivery via state.HoldbackQueue
	holdbackMsg := &state.HoldbackMessage{
		SequenceNum:   seqNum,
		StateSnapshot: msg.StateSnapshot,
		UpdateType:    msg.UpdateType,
	}

	if err := n.holdbackQueue.Enqueue(holdbackMsg); err != nil {
		log.Printf("[Node %s] Holdback error: %v\n", n.id[:8], err)
	}
}

func (n *Node) handleElection(msg *protocol.ElectionMsg, sender *net.UDPAddr) {
	fmt.Printf("[%s] <- ELECTION candidate=%s phase=%v\n",
		n.id[:8], msg.CandidateId[:8], msg.Phase)
	// TODO: Implement LCR election algorithm
}

func (n *Node) handleNack(msg *protocol.NackMsg, sender *net.UDPAddr) {
	fmt.Printf("[%s] <- NACK seq=%d-%d from %s\n",
		n.id[:8], msg.FromSeq, msg.ToSeq, protocol.GetSenderID(msg)[:8])
	// TODO: Resend missing messages
}

// Handle DATA from producer
func (n *Node) handleData(msg *protocol.DataMsg, sender *net.UDPAddr) {
	// Get producer ID and topic from message
	producerID := protocol.GetSenderID(msg)
	topic := msg.Topic

	// Log
	fmt.Printf("[%s] <- DATA from %s (topic: %s, size: %d bytes)\n",
		n.id[:8], producerID[:8], topic, len(msg.Data))

	// Update producer heartbeat
	n.producers.UpdateHeartbeat(producerID)

	// Get consumers subscribed to this topic
	subscribers := n.consumers.GetSubscribers(topic)
	if len(subscribers) == 0 {
		fmt.Printf("[%s] No consumers subscribed to topic: %s\n", n.id[:8], topic)
		return
	}

	fmt.Printf("[%s] Forwarding to %d consumer(s) on topic: %s\n",
		n.id[:8], len(subscribers), topic)

	// Send RESULT message to each subscribed consumer
	for _, consumerID := range subscribers {
		// Get consumer info
		consumer, ok := n.consumers.Get(consumerID)
		if !ok {
			continue
		}

		// Create RESULT message
		resultMsg := protocol.NewResultMsg(
			n.id,     // sender ID (this node)
			topic,    // topic
			msg.Data, // data payload
			0,        // offset (TODO: track per-topic offset)
			0,        // sequence (TODO: track per-topic sequence)
		)

		// Send via TCP in a goroutine to avoid blocking threads
		go func(addr string, result protocol.Message) {
			conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				log.Printf("[%s] Failed to connect to consumer %s: %v\n",
					n.id[:8], addr, err)
				return
			}
			defer conn.Close()

			if err := protocol.WriteTCPMessage(conn, result); err != nil {
				log.Printf("[%s] Failed to send RESULT to %s: %v\n",
					n.id[:8], addr, err)
				return
			}

			fmt.Printf("[%s] -> RESULT to %s (topic: %s, size: %d bytes)\n",
				n.id[:8], addr, topic, len(msg.Data))
		}(consumer.Address, resultMsg)
	}
}

// Apply registry update
func (n *Node) applyRegistryUpdate(msg *state.HoldbackMessage) error {
	fmt.Printf("\n[Node %s] ========================================\n", n.id[:8])
	fmt.Printf("[Node %s] Applying registry update seq=%d type=%s\n",
		n.id[:8], msg.SequenceNum, msg.UpdateType)

	// Get current registry state
	if err := n.registry.Deserialize(msg.StateSnapshot); err != nil {
		return fmt.Errorf("failed to apply update: %w", err)
	}

	// Print registry status after applying update
	n.registry.PrintStatus()
	fmt.Printf("[Node %s] ========================================\n\n", n.id[:8])

	return nil
}

// Leader specific duties to be run
func (n *Node) runLeaderDuties() {
	fmt.Printf("[Leader %s] Starting leader duties\n", n.id[:8])

	// Start heartbeating and timeout checking
	heartbeatTicker := time.NewTicker(5 * time.Second)
	timeoutTicker := time.NewTicker(10 * time.Second)
	defer heartbeatTicker.Stop()
	defer timeoutTicker.Stop()

	for {
		select {
		case <-heartbeatTicker.C:
			// Send periodic heartbeat
			if err := n.sendHeartbeat(); err != nil {
				log.Printf("[Leader %s] Failed to send heartbeat: %v\n", n.id[:8], err)
			}

			// Replicate registry state
			if err := n.replicateRegistry(); err != nil {
				log.Printf("[Leader %s] Failed to replicate registry: %v\n", n.id[:8], err)
			}

		case <-timeoutTicker.C:
			// Check for dead brokers
			removedBrokers := n.registry.CheckTimeouts(30 * time.Second)
			if len(removedBrokers) > 0 {
				fmt.Printf("[Leader %s] Removed %d dead brokers\n", n.id[:8], len(removedBrokers))
				// Replicate immediately to propagate removal
				n.replicateRegistry()
			}

			// Check for dead producers
			deadProducers := n.producers.CheckTimeouts(60 * time.Second)
			if len(deadProducers) > 0 {
				fmt.Printf("[Leader %s] Removed %d dead producers\n", n.id[:8], len(deadProducers))
			}

			// Check for dead consumers
			deadConsumers := n.consumers.CheckTimeouts(60 * time.Second)
			if len(deadConsumers) > 0 {
				fmt.Printf("[Leader %s] Removed %d dead consumers\n", n.id[:8], len(deadConsumers))
			}

		// Stop leader duties get out of for loop
		case <-n.stopLeaderDuties:
			fmt.Printf("[Leader %s] Stopping leader duties\n", n.id[:8])
			return
		}
	}
}

func (n *Node) sendHeartbeat() error {
	if !n.IsLeader() {
		return fmt.Errorf("only leader can send heartbeat")
	}

	// Send heartbeat message on multicast
	err := protocol.SendHeartbeatMulticast(
		n.multicastSender,
		n.id,
		protocol.NodeType_LEADER,
		n.config.MulticastGroup,
	)

	if err == nil {
		fmt.Printf("[Leader %s] -> HEARTBEAT\n", n.id[:8])
	}
	return err
}

// Replicate the current registry state
func (n *Node) replicateRegistry() error {
	if !n.IsLeader() {
		return fmt.Errorf("only leader can replicate registry")
	}

	// Get current sequence number
	currentSeq := n.registry.GetSequenceNum()
	if currentSeq == n.lastReplicatedSeq {
		// No state change, skip replication
		return nil
	}

	// Get current registry state
	snapshot, err := n.registry.Serialize()
	if err != nil {
		return err
	}

	// Send replication message
	err = protocol.SendReplicationMulticast(
		n.multicastSender,
		n.id,
		snapshot,
		"REGISTRY",
		currentSeq,
		n.config.MulticastGroup,
	)

	if err == nil {
		// Update last replicated sequence
		n.lastReplicatedSeq = currentSeq
		fmt.Printf("[Leader %s] -> REPLICATE seq=%d type=REGISTRY\n", n.id[:8], currentSeq)
	}
	return err
}

func (n *Node) listenForBroadcastJoins() {
	for {
		// Wait for incoming broadcast messages
		msg, sender, err := n.broadcastListener.ReceiveMessage()
		if err != nil {
			return // Listener closed
		}

		if joinMsg, ok := msg.(*protocol.JoinMsg); ok {
			fmt.Printf("[Leader %s] <- JOIN from %s (addr=%s)\n",
				n.id[:8], protocol.GetSenderID(msg)[:8], joinMsg.Address)

			// Register new node in registry
			newNodeID := protocol.GetSenderID(msg)
			n.registry.RegisterBroker(newNodeID, joinMsg.Address, false)

			// Send response on broadcast
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

			// Send immediate state sync
			fmt.Printf("[Leader %s] Sending initial state sync to %s\n",
				n.id[:8], newNodeID[:8])

			if err := n.replicateRegistry(); err != nil {
				log.Printf("[Leader %s] Failed initial sync: %v\n", n.id[:8], err)
			}
		}
	}
}

// Run follower duties
func (n *Node) runFollowerDuties() {
	fmt.Printf("[Follower %s] Starting follower duties\n", n.id[:8])

	// For now set up a heartbeat ticker for 5 seconds
	heartbeatTicker := time.NewTicker(5 * time.Second)
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-heartbeatTicker.C:
			// Send periodic heartbeat
			if err := n.sendFollowerHeartbeat(); err != nil {
				log.Printf("[Follower %s] Failed to send heartbeat: %v\n", n.id[:8], err)
			}

		// Stop follower duties get out of for loop
		case <-n.stopFollowerDuties:
			fmt.Printf("[Follower %s] Stopping follower duties\n", n.id[:8])
			return
		}
	}
}

// Send heartbeat message
func (n *Node) sendFollowerHeartbeat() error {
	if n.IsLeader() {
		return fmt.Errorf("only followers should call this")
	}

	// Send heartbeat via multicast so leader receives it
	err := protocol.SendHeartbeatMulticast(
		n.multicastSender,
		n.id,
		protocol.NodeType_BROKER,
		n.config.MulticastGroup,
	)

	if err == nil {
		fmt.Printf("[Follower %s] -> HEARTBEAT\n", n.id[:8])
	}
	return err
}

// Shutdown the node
func (n *Node) Shutdown() {
	fmt.Printf("\n[Node %s] Shutting down...\n", n.id[:8])

	// If we're the leader, remove self from registry and replicate
	if n.IsLeader() {
		fmt.Printf("[Leader %s] Performing graceful shutdown...\n", n.id[:8])
		n.registry.RemoveBroker(n.id)
		n.replicateRegistry()
		time.Sleep(1 * time.Second) // Let message propagate
	}

	// Stop role-specific goroutines
	close(n.stopLeaderDuties)
	close(n.stopFollowerDuties)

	// Close network connections
	if n.multicastReceiver != nil {
		n.multicastReceiver.Close()
	}
	if n.multicastSender != nil {
		n.multicastSender.Close()
	}
	if n.broadcastListener != nil {
		n.broadcastListener.Close()
	}
	if n.tcpListener != nil {
		n.tcpListener.Close()
	}

	fmt.Printf("[Node %s] Shutdown complete\n", n.id[:8])
}
