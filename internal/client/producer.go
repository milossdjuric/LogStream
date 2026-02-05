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
	udpConn       *net.UDPConn
	udpRemoteAddr *net.UDPAddr
	stopHeartbeat chan struct{}
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

	// Start heartbeat routine to send periodic heartbeats
	p.wg.Add(1) // Register goroutine before starting
	go p.sendHeartbeats()

	return nil
}

// SendData sends data to the assigned broker via UDP
// Uses monotonically increasing sequence numbers for FIFO ordering
func (p *Producer) SendData(data []byte) error {
	if p.udpConn == nil {
		return fmt.Errorf("not connected")
	}

	// Get current sequence number and increment for next message
	currentSeq := p.seqNum
	p.seqNum++

	// Create DATA message with sequence number for FIFO ordering
	dataMsg := protocol.NewDataMsg(p.id, p.topic, data, currentSeq)

	// Send DATA message
	if err := protocol.WriteUDPMessage(p.udpConn, dataMsg, p.udpRemoteAddr); err != nil {
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
func (p *Producer) sendHeartbeats() {
	defer p.wg.Done() // Signal completion when goroutine exits
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Send heartbeat via TCP to leader
			conn, err := net.DialTimeout("tcp", p.leaderAddr, 5*time.Second)
			if err != nil {
				log.Printf("[Producer %s] Failed to send heartbeat: %v", p.id[:8], err)
				continue
			}

		// Create heartbeat message (producers don't have view numbers, use 0)
		heartbeat := protocol.NewHeartbeatMsg(p.id, protocol.NodeType_PRODUCER, 0, 0)

		// Send heartbeat message
		if err := protocol.WriteTCPMessage(conn, heartbeat); err != nil {
			log.Printf("[Producer %s] Failed to write heartbeat: %v", p.id[:8], err)
		}

			conn.Close()

		case <-p.stopHeartbeat:
			return
		}
	}
}

// Close shuts down the producer
func (p *Producer) Close() {
	close(p.stopHeartbeat)
	
	// Wait for goroutine to actually exit (proper synchronization)
	p.wg.Wait()
	
	if p.udpConn != nil {
		p.udpConn.Close()
	}
}
