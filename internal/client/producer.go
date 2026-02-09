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
	clientPort    int            // Fixed TCP listener port (0 = OS picks)
	advertiseAddr string         // Override advertised IP (e.g. Windows host IP for WSL)
	stopHeartbeat chan struct{}
	stopListener  chan struct{}
	seqNum        int64          // Monotonically increasing sequence number for FIFO ordering
	wg            sync.WaitGroup // Synchronize goroutine lifecycle

	// UDP heartbeat state
	leaderAddrUDP         *net.UDPAddr   // Resolved leader UDP address for heartbeats
	lastLeaderHeartbeat   time.Time
	lastLeaderHeartbeatMu sync.RWMutex
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

// NewProducerWithPort creates a new producer with a fixed TCP listener port.
// If port is 0, the OS picks an ephemeral port (same as NewProducer).
func NewProducerWithPort(topic, leaderAddr string, port int, advertiseAddr string) *Producer {
	p := NewProducer(topic, leaderAddr)
	p.clientPort = port
	p.advertiseAddr = advertiseAddr
	return p
}

// Connect registers with the leader via TCP
// If no leader address was provided, auto-discovers the cluster via broadcast first
func (p *Producer) Connect() error {
	const (
		maxAttempts      = 15
		initialDelay     = 500 * time.Millisecond
		retryDelay       = 2 * time.Second
		halfOpenDelay    = 5 * time.Second
		failureThreshold = 5
	)

	// Auto-discover leader if not provided
	if p.leaderAddr == "" {
		fmt.Printf("[Producer %s] No leader address provided, discovering via broadcast...\n", p.id[:8])
		discoveryConfig := protocol.DefaultBroadcastConfig()
		discoveryConfig.DiscoveryPort = p.clientPort
		leaderAddr, err := protocol.DiscoverLeader(discoveryConfig)
		if err != nil {
			return fmt.Errorf("failed to discover cluster: %w", err)
		}
		p.leaderAddr = leaderAddr
		fmt.Printf("[Producer %s] Discovered leader at %s\n", p.id[:8], p.leaderAddr)
	}

	time.Sleep(initialDelay)

	// Registration with circuit breaker retry.
	// Handles transient failures like leader frozen during view change.
	regDelay := retryDelay
	var lastErr error

	for regAttempt := 1; regAttempt <= maxAttempts; regAttempt++ {
		lastErr = p.attemptRegistration()
		if lastErr == nil {
			break
		}
		fmt.Printf("[Producer %s] Registration attempt %d/%d failed: %v\n",
			p.id[:8], regAttempt, maxAttempts, lastErr)
		if regAttempt < maxAttempts {
			fmt.Printf("[Producer %s] Retrying in %v...\n", p.id[:8], regDelay)
			time.Sleep(regDelay)
			regDelay = time.Duration(float64(regDelay) * 1.5)
			if regDelay > halfOpenDelay {
				regDelay = halfOpenDelay
			}
		}
	}

	if lastErr != nil {
		return fmt.Errorf("registration failed after %d attempts: %w", maxAttempts, lastErr)
	}

	fmt.Printf("[Producer %s] <- PRODUCE_ACK (assigned broker: %s)\n", p.id[:8], p.brokerAddr)

	// Create UDP connection for sending data (bind to clientPort so leader can send heartbeats back)
	var err error
	udpPort := 0
	if p.clientPort > 0 {
		udpPort = p.clientPort
	}
	p.udpConn, err = net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: udpPort})
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
	// Always bind to 0.0.0.0:port (not the advertised IP which may not be local, e.g. WSL)
	if p.clientPort > 0 {
		// Use fixed port for the listener
		listenAddr := fmt.Sprintf("0.0.0.0:%d", p.clientPort)
		p.tcpListener, err = net.Listen("tcp", listenAddr)
		if err != nil {
			p.udpConn.Close()
			return fmt.Errorf("failed to start TCP listener on %s: %w", listenAddr, err)
		}
	} else {
		// Use any available port for the listener
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

	// Initialize UDP heartbeat state
	p.leaderAddrUDP, _ = net.ResolveUDPAddr("udp", p.leaderAddr)
	p.lastLeaderHeartbeatMu.Lock()
	p.lastLeaderHeartbeat = time.Now()
	p.lastLeaderHeartbeatMu.Unlock()

	// Start heartbeat routine to send periodic heartbeats via UDP
	p.wg.Add(1)
	go p.sendHeartbeats()

	// Start UDP receive loop for incoming heartbeats from leader
	p.wg.Add(1)
	go p.receiveUDPMessages()

	return nil
}

// attemptRegistration performs a single registration attempt with the leader.
// Connects via TCP, sends PRODUCE, reads PRODUCE_ACK, and sets p.brokerAddr on success.
func (p *Producer) attemptRegistration() error {
	conn, err := net.DialTimeout("tcp", p.leaderAddr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to leader: %w", err)
	}
	defer conn.Close()

	// Determine local address for registration
	localIP := conn.LocalAddr().(*net.TCPAddr).IP.String()
	if p.advertiseAddr != "" {
		localIP = p.advertiseAddr
	}

	var localAddr string
	if p.clientPort > 0 {
		localAddr = fmt.Sprintf("%s:%d", localIP, p.clientPort)
	} else if p.advertiseAddr != "" {
		localPort := conn.LocalAddr().(*net.TCPAddr).Port
		localAddr = fmt.Sprintf("%s:%d", localIP, localPort)
	} else {
		localAddr = conn.LocalAddr().String()
	}

	produceMsg := protocol.NewProduceMsg(p.id, p.topic, localAddr, 0)

	fmt.Printf("[Producer %s] -> PRODUCE (topic: %s, addr: %s)\n", p.id[:8], p.topic, localAddr)
	if err := protocol.WriteTCPMessage(conn, produceMsg); err != nil {
		return fmt.Errorf("failed to send PRODUCE: %w", err)
	}

	msg, err := protocol.ReadTCPMessage(conn)
	if err != nil {
		return fmt.Errorf("failed to read PRODUCE_ACK: %w", err)
	}

	ack, ok := msg.(*protocol.ProduceMsg)
	if !ok {
		return fmt.Errorf("unexpected response type: %T", msg)
	}

	if ack.ProducerAddress == "" {
		return fmt.Errorf("no broker assigned (leader may be temporarily unavailable)")
	}

	p.brokerAddrMu.Lock()
	p.brokerAddr = ack.ProducerAddress
	p.brokerAddrMu.Unlock()
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

// sendHeartbeats sends periodic heartbeats to leader via UDP
// Detects leader death via missing incoming heartbeats and triggers cluster re-discovery
func (p *Producer) sendHeartbeats() {
	defer p.wg.Done()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Send heartbeat via UDP to leader
			heartbeat := protocol.NewHeartbeatMsg(p.id, protocol.NodeType_PRODUCER, 0, 0, "")
			p.brokerAddrMu.RLock()
			conn := p.udpConn
			p.brokerAddrMu.RUnlock()
			if conn != nil && p.leaderAddrUDP != nil {
				protocol.WriteUDPMessage(conn, heartbeat, p.leaderAddrUDP)
			}

			// Check for leader failure (no heartbeat received for 60s)
			p.lastLeaderHeartbeatMu.RLock()
			lastHB := p.lastLeaderHeartbeat
			p.lastLeaderHeartbeatMu.RUnlock()
			if !lastHB.IsZero() && time.Since(lastHB) > 60*time.Second {
				fmt.Printf("[Producer %s] Leader appears down (no heartbeat for %v), attempting re-discovery...\n",
					p.id[:8], time.Since(lastHB).Round(time.Second))
				if err := p.reconnectToCluster(); err != nil {
					log.Printf("[Producer %s] Cluster re-discovery failed: %v", p.id[:8], err)
				} else {
					fmt.Printf("[Producer %s] Successfully reconnected to cluster\n", p.id[:8])
				}
			}

		case <-p.stopHeartbeat:
			return
		}
	}
}

// receiveUDPMessages receives incoming UDP messages (heartbeats from leader)
func (p *Producer) receiveUDPMessages() {
	defer p.wg.Done()
	for {
		select {
		case <-p.stopHeartbeat:
			return
		default:
		}

		p.brokerAddrMu.RLock()
		conn := p.udpConn
		p.brokerAddrMu.RUnlock()
		if conn == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		msg, _, err := protocol.ReadUDPMessage(conn)
		if err != nil {
			continue // timeout or error, retry
		}

		if hb, ok := msg.(*protocol.HeartbeatMsg); ok {
			senderType := protocol.GetSenderType(hb)
			if senderType == protocol.NodeType_LEADER {
				p.lastLeaderHeartbeatMu.Lock()
				p.lastLeaderHeartbeat = time.Now()
				p.lastLeaderHeartbeatMu.Unlock()
			}
		}
	}
}

// reconnectToCluster re-discovers the cluster leader and re-registers the producer.
// Called when the leader appears to be down (consecutive heartbeat failures).
func (p *Producer) reconnectToCluster() error {
	const maxAttempts = 10
	const attemptDelay = 5 * time.Second

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		fmt.Printf("[Producer %s] Reconnection attempt %d/%d...\n", p.id[:8], attempt, maxAttempts)

		// Wait before retrying to give cluster time to elect new leader
		if attempt > 1 {
			time.Sleep(attemptDelay)
		}

		// Try broadcast discovery first
		discoveryConfig := protocol.DefaultBroadcastConfig()
		discoveryConfig.DiscoveryPort = p.clientPort
		leaderAddr, err := protocol.DiscoverLeader(discoveryConfig)
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

		var localAddr string
		reconnIP := conn.LocalAddr().(*net.TCPAddr).IP.String()
		if p.advertiseAddr != "" {
			reconnIP = p.advertiseAddr
		}
		if p.clientPort > 0 {
			localAddr = fmt.Sprintf("%s:%d", reconnIP, p.clientPort)
		} else {
			localAddr = conn.LocalAddr().String()
		}
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

		// Resolve new broker address (reuse existing UDP socket)
		newRemoteAddr, err := net.ResolveUDPAddr("udp", newBrokerAddr)
		if err != nil {
			log.Printf("[Producer %s] Failed to resolve new broker address: %v", p.id[:8], err)
			continue
		}

		// Update broker connection state (reuse UDP socket)
		p.brokerAddrMu.Lock()
		p.udpRemoteAddr = newRemoteAddr
		p.brokerAddr = newBrokerAddr
		p.brokerAddrMu.Unlock()

		// Update leader address and UDP target
		p.leaderAddr = leaderAddr
		p.leaderAddrUDP, _ = net.ResolveUDPAddr("udp", leaderAddr)
		p.lastLeaderHeartbeatMu.Lock()
		p.lastLeaderHeartbeat = time.Now()
		p.lastLeaderHeartbeatMu.Unlock()

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
