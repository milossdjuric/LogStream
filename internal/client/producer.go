package client

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/milossdjuric/logstream/internal/protocol"
)

// Producer handles producing data to LogStream
type Producer struct {
	id            string
	topic         string
	leaderAddr    string
	brokerAddr    string
	brokerAddrMu  sync.RWMutex   // Protects brokerAddr, udpRemoteAddr, and udpConn
	udpConn       *net.UDPConn
	udpRemoteAddr *net.UDPAddr
	tcpListener   net.Listener   // TCP listener for incoming messages from leader
	stopHeartbeat chan struct{}
	stopListener  chan struct{}
	seqNum        int64          // Monotonically increasing sequence number for FIFO ordering
	wg            sync.WaitGroup // Synchronize goroutine lifecycle
}

// NewProducer creates a new producer
// If leaderAddr is empty, auto-discovery via broadcast will be used during Connect()
func NewProducer(topic, leaderAddr string) *Producer {
	// Generate ID based on topic if no leader address (will discover later)
	idSeed := leaderAddr
	if idSeed == "" {
		idSeed = topic
	}
	return &Producer{
		id:            protocol.GenerateClientID("producer", idSeed),
		topic:         topic,
		leaderAddr:    leaderAddr,
		stopHeartbeat: make(chan struct{}),
		stopListener:  make(chan struct{}),
		seqNum:        1, // Start at 1 (0 means unset)
	}
}

// Connect registers with the leader via TCP
// If no leader address was provided, auto-discovers the cluster via broadcast first
func (p *Producer) Connect() error {
	const (
		maxAttempts      = 10
		initialDelay     = 500 * time.Millisecond
		retryDelay       = 1 * time.Second
		halfOpenDelay    = 2 * time.Second
		failureThreshold = 5
	)

	// Auto-discover leader if not provided
	if p.leaderAddr == "" {
		fmt.Printf("[Producer %s] No leader address provided, discovering via broadcast...\n", p.id[:8])
		leaderAddr, err := protocol.DiscoverLeader(nil)
		if err != nil {
			return fmt.Errorf("failed to discover cluster: %w", err)
		}
		p.leaderAddr = leaderAddr
		fmt.Printf("[Producer %s] Discovered leader at %s\n", p.id[:8], p.leaderAddr)
	}

	failureCount := 0
	var conn net.Conn
	var err error

	time.Sleep(initialDelay)

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		conn, err = net.DialTimeout("tcp", p.leaderAddr, 5*time.Second)

		if err != nil {
			failureCount++
			if failureCount >= failureThreshold {
				time.Sleep(halfOpenDelay)
				conn, err = net.DialTimeout("tcp", p.leaderAddr, 5*time.Second)
				if err == nil {
					break
				}
				failureCount++
			}
		} else {
			break
		}

		if attempt < maxAttempts {
			time.Sleep(retryDelay)
		}
	}

	if err != nil {
		return fmt.Errorf("failed to connect to leader after %d attempts: %w", maxAttempts, err)
	}

	defer conn.Close()

	// Get local address for registration
	localAddr := conn.LocalAddr().String()

	// Make PRODUCE message
	produceMsg := protocol.NewProduceMsg(p.id, p.topic, localAddr, 0)

	// Send PRODUCE request
	fmt.Printf("[Producer %s] -> PRODUCE (topic: %s)\n", p.id[:8], p.topic)
	if err := protocol.WriteTCPMessage(conn, produceMsg); err != nil {
		return fmt.Errorf("failed to send PRODUCE: %w", err)
	}

	// Read PRODUCE_ACK response
	msg, err := protocol.ReadTCPMessage(conn)
	if err != nil {
		return fmt.Errorf("failed to read PRODUCE_ACK: %w", err)
	}

	ack, ok := msg.(*protocol.ProduceMsg)
	if !ok {
		return fmt.Errorf("unexpected response type: %T", msg)
	}

	// Extract assigned broker address
	p.brokerAddr = ack.ProducerAddress
	if p.brokerAddr == "" {
		return fmt.Errorf("no broker assigned")
	}

	fmt.Printf("[Producer %s] <- PRODUCE_ACK (assigned broker: %s)\n", p.id[:8], p.brokerAddr)

	// Create UDP connection for sending data
	p.udpConn, err = net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		return fmt.Errorf("failed to create UDP socket: %w", err)
	}

	// Resolve broker address for sending
	p.udpRemoteAddr, err = net.ResolveUDPAddr("udp", p.brokerAddr)
	if err != nil {
		p.udpConn.Close()
		return fmt.Errorf("failed to resolve broker address: %w", err)
	}

	// Start TCP listener for incoming messages (like REASSIGN_BROKER)
	// Use the same local address that was used for registration
	localTCPAddr, err := net.ResolveTCPAddr("tcp", localAddr)
	if err != nil {
		p.udpConn.Close()
		return fmt.Errorf("failed to resolve local TCP address: %w", err)
	}

	p.tcpListener, err = net.ListenTCP("tcp", localTCPAddr)
	if err != nil {
		// Try with any available port if the original port is in use
		p.tcpListener, err = net.Listen("tcp", ":0")
		if err != nil {
			p.udpConn.Close()
			return fmt.Errorf("failed to start TCP listener: %w", err)
		}
	}

	fmt.Printf("[Producer %s] TCP listener started on %s\n", p.id[:8], p.tcpListener.Addr().String())

	// Start listener routine for incoming messages
	p.wg.Add(1)
	go p.listenForMessages()

	// Start heartbeat routine to send periodic heartbeats
	p.wg.Add(1) // Register goroutine before starting
	go p.sendHeartbeats()

	return nil
}

// SendData sends data to the assigned broker via UDP
// Uses monotonically increasing sequence numbers for FIFO ordering
func (p *Producer) SendData(data []byte) error {
	// Get current broker connection (protected by mutex for concurrent access)
	p.brokerAddrMu.RLock()
	conn := p.udpConn
	remoteAddr := p.udpRemoteAddr
	p.brokerAddrMu.RUnlock()

	if conn == nil {
		return fmt.Errorf("not connected")
	}

	// Get current sequence number and increment for next message
	currentSeq := p.seqNum
	p.seqNum++

	// Create DATA message with sequence number for FIFO ordering
	dataMsg := protocol.NewDataMsg(p.id, p.topic, data, currentSeq)

	// Send DATA message
	if err := protocol.WriteUDPMessage(conn, dataMsg, remoteAddr); err != nil {
		return fmt.Errorf("failed to send data: %w", err)
	}

	fmt.Printf("[Producer %s] -> DATA seq=%d (topic: %s, size: %d bytes)\n",
		p.id[:8], currentSeq, p.topic, len(data))

	return nil
}

// GetSequenceNum returns the current sequence number (for debugging/testing)
func (p *Producer) GetSequenceNum() int64 {
	return p.seqNum
}

// sendHeartbeats sends periodic heartbeats to leader
// Detects leader death via consecutive failures and triggers cluster re-discovery
func (p *Producer) sendHeartbeats() {
	defer p.wg.Done() // Signal completion when goroutine exits
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	consecutiveFailures := 0

	for {
		select {
		case <-ticker.C:
			// Send heartbeat via TCP to leader
			conn, err := net.DialTimeout("tcp", p.leaderAddr, 5*time.Second)
			if err != nil {
				consecutiveFailures++
				log.Printf("[Producer %s] Heartbeat failed (%d consecutive): %v", p.id[:8], consecutiveFailures, err)

				if consecutiveFailures >= 3 {
					fmt.Printf("[Producer %s] Leader appears down after %d heartbeat failures, attempting re-discovery...\n",
						p.id[:8], consecutiveFailures)
					if err := p.reconnectToCluster(); err != nil {
						log.Printf("[Producer %s] Cluster re-discovery failed: %v", p.id[:8], err)
					} else {
						consecutiveFailures = 0
						fmt.Printf("[Producer %s] Successfully reconnected to cluster\n", p.id[:8])
					}
				}
				continue
			}

			// Create heartbeat message (producers don't have view numbers, use 0)
			heartbeat := protocol.NewHeartbeatMsg(p.id, protocol.NodeType_PRODUCER, 0, 0)

			// Send heartbeat message
			if err := protocol.WriteTCPMessage(conn, heartbeat); err != nil {
				log.Printf("[Producer %s] Failed to write heartbeat: %v", p.id[:8], err)
			} else {
				consecutiveFailures = 0
			}

			conn.Close()

		case <-p.stopHeartbeat:
			return
		}
	}
}

// reconnectToCluster re-discovers the cluster leader and re-registers the producer.
// Called when the leader appears to be down (consecutive heartbeat failures).
func (p *Producer) reconnectToCluster() error {
	const maxAttempts = 5
	const attemptDelay = 5 * time.Second

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		fmt.Printf("[Producer %s] Reconnection attempt %d/%d...\n", p.id[:8], attempt, maxAttempts)

		// Wait before retrying to give cluster time to elect new leader
		if attempt > 1 {
			time.Sleep(attemptDelay)
		}

		// Try broadcast discovery first
		leaderAddr, err := protocol.DiscoverLeader(nil)
		if err != nil {
			log.Printf("[Producer %s] Discovery failed: %v", p.id[:8], err)
			continue
		}

		fmt.Printf("[Producer %s] Discovered new leader at %s\n", p.id[:8], leaderAddr)

		// Register with new leader
		conn, err := net.DialTimeout("tcp", leaderAddr, 5*time.Second)
		if err != nil {
			log.Printf("[Producer %s] Failed to connect to new leader: %v", p.id[:8], err)
			continue
		}

		localAddr := conn.LocalAddr().String()
		produceMsg := protocol.NewProduceMsg(p.id, p.topic, localAddr, 0)

		fmt.Printf("[Producer %s] -> PRODUCE to new leader (topic: %s)\n", p.id[:8], p.topic)
		if err := protocol.WriteTCPMessage(conn, produceMsg); err != nil {
			conn.Close()
			log.Printf("[Producer %s] Failed to send PRODUCE: %v", p.id[:8], err)
			continue
		}

		msg, err := protocol.ReadTCPMessage(conn)
		conn.Close()
		if err != nil {
			log.Printf("[Producer %s] Failed to read PRODUCE_ACK: %v", p.id[:8], err)
			continue
		}

		ack, ok := msg.(*protocol.ProduceMsg)
		if !ok {
			log.Printf("[Producer %s] Unexpected response type: %T", p.id[:8], msg)
			continue
		}

		newBrokerAddr := ack.ProducerAddress
		if newBrokerAddr == "" {
			log.Printf("[Producer %s] No broker assigned by new leader", p.id[:8])
			continue
		}

		fmt.Printf("[Producer %s] <- PRODUCE_ACK (new broker: %s)\n", p.id[:8], newBrokerAddr)

		// Create new UDP connection before swapping
		newUDPConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
		if err != nil {
			log.Printf("[Producer %s] Failed to create new UDP socket: %v", p.id[:8], err)
			continue
		}

		newRemoteAddr, err := net.ResolveUDPAddr("udp", newBrokerAddr)
		if err != nil {
			newUDPConn.Close()
			log.Printf("[Producer %s] Failed to resolve new broker address: %v", p.id[:8], err)
			continue
		}

		// Atomically swap connection state
		p.brokerAddrMu.Lock()
		oldConn := p.udpConn
		p.udpConn = newUDPConn
		p.udpRemoteAddr = newRemoteAddr
		p.brokerAddr = newBrokerAddr
		p.brokerAddrMu.Unlock()

		// Update leader address
		p.leaderAddr = leaderAddr

		// Close old UDP connection
		if oldConn != nil {
			oldConn.Close()
		}

		fmt.Printf("[Producer %s] Reconnected: leader=%s, broker=%s\n", p.id[:8], leaderAddr, newBrokerAddr)
		return nil
	}

	return fmt.Errorf("failed to reconnect after %d attempts", maxAttempts)
}

// listenForMessages listens for incoming TCP messages from the leader
// Handles REASSIGN_BROKER messages for broker failover
func (p *Producer) listenForMessages() {
	defer p.wg.Done()

	for {
		select {
		case <-p.stopListener:
			return
		default:
			if p.tcpListener == nil {
				return
			}

			// Set accept timeout to allow checking stop signal
			if tcpListener, ok := p.tcpListener.(*net.TCPListener); ok {
				tcpListener.SetDeadline(time.Now().Add(1 * time.Second))
			}

			conn, err := p.tcpListener.Accept()
			if err != nil {
				// Check if it's a timeout (expected) or actual error
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				// Check if listener was closed
				select {
				case <-p.stopListener:
					return
				default:
					log.Printf("[Producer %s] Accept error: %v", p.id[:8], err)
					continue
				}
			}

			// Handle the connection in a goroutine
			go p.handleIncomingConnection(conn)
		}
	}
}

// handleIncomingConnection handles an incoming TCP connection
func (p *Producer) handleIncomingConnection(conn net.Conn) {
	defer conn.Close()

	// Set read deadline
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	msg, err := protocol.ReadTCPMessage(conn)
	if err != nil {
		log.Printf("[Producer %s] Failed to read incoming message: %v", p.id[:8], err)
		return
	}

	switch m := msg.(type) {
	case *protocol.ReassignBrokerMsg:
		fmt.Printf("[Producer %s] <- REASSIGN_BROKER (topic: %s, new broker: %s)\n",
			p.id[:8], m.Topic, m.NewBrokerAddress)

		// Update broker address
		if err := p.UpdateBrokerAddress(m.NewBrokerAddress); err != nil {
			log.Printf("[Producer %s] Failed to update broker address: %v", p.id[:8], err)
		} else {
			fmt.Printf("[Producer %s] Successfully updated broker to %s\n", p.id[:8], m.NewBrokerAddress)
		}

	case *protocol.HeartbeatMsg:
		// Heartbeat from leader - just acknowledge
		fmt.Printf("[Producer %s] <- HEARTBEAT from leader\n", p.id[:8])

	default:
		log.Printf("[Producer %s] Received unexpected message type: %T", p.id[:8], msg)
	}
}

// UpdateBrokerAddress updates the broker address for sending data
// Called when receiving REASSIGN_BROKER from leader
func (p *Producer) UpdateBrokerAddress(newAddr string) error {
	p.brokerAddrMu.Lock()
	defer p.brokerAddrMu.Unlock()

	// Resolve new broker address
	newRemoteAddr, err := net.ResolveUDPAddr("udp", newAddr)
	if err != nil {
		return fmt.Errorf("failed to resolve new broker address: %w", err)
	}

	oldAddr := p.brokerAddr
	p.brokerAddr = newAddr
	p.udpRemoteAddr = newRemoteAddr

	fmt.Printf("[Producer %s] Broker address updated: %s -> %s\n", p.id[:8], oldAddr, newAddr)
	return nil
}

// GetBrokerAddress returns the current broker address
func (p *Producer) GetBrokerAddress() string {
	p.brokerAddrMu.RLock()
	defer p.brokerAddrMu.RUnlock()
	return p.brokerAddr
}

// Close shuts down the producer
func (p *Producer) Close() {
	close(p.stopHeartbeat)
	close(p.stopListener)

	// Close TCP listener to unblock Accept()
	if p.tcpListener != nil {
		p.tcpListener.Close()
	}

	// Wait for goroutines to actually exit (proper synchronization)
	p.wg.Wait()

	p.brokerAddrMu.Lock()
	if p.udpConn != nil {
		p.udpConn.Close()
	}
	p.brokerAddrMu.Unlock()
}
