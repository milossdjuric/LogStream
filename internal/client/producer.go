package client

import (
	"fmt"
	"log"
	"net"
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
}

// NewProducer creates a new producer
func NewProducer(topic, leaderAddr string) *Producer {
	return &Producer{
		id:            protocol.GenerateNodeID(fmt.Sprintf("producer-%d", time.Now().UnixNano())),
		topic:         topic,
		leaderAddr:    leaderAddr,
		stopHeartbeat: make(chan struct{}),
	}
}

// Connect registers with the leader via TCP
func (p *Producer) Connect() error {
	// Connect to leader
	conn, err := net.DialTimeout("tcp", p.leaderAddr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to leader: %w", err)
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
	go p.sendHeartbeats()

	return nil
}

// SendData sends data to the assigned broker via UDP
func (p *Producer) SendData(data []byte) error {
	if p.udpConn == nil {
		return fmt.Errorf("not connected")
	}

	// Create DATA message
	dataMsg := protocol.NewDataMsg(p.id, p.topic, data, 0)

	// Send DATA message
	if err := protocol.WriteUDPMessage(p.udpConn, dataMsg, p.udpRemoteAddr); err != nil {
		return fmt.Errorf("failed to send data: %w", err)
	}

	fmt.Printf("[Producer %s] -> DATA (topic: %s, size: %d bytes)\n",
		p.id[:8], p.topic, len(data))

	return nil
}

// sendHeartbeats sends periodic heartbeats to leader
func (p *Producer) sendHeartbeats() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Send heartbeat via TCP to leader
			conn, err := net.DialTimeout("tcp", p.leaderAddr, 5*time.Second)
			if err != nil {
				log.Printf("[Producer %s] Failed to send heartbeat: %v\n", p.id[:8], err)
				continue
			}

			// Create heartbeat message
			heartbeat := protocol.NewHeartbeatMsg(p.id, protocol.NodeType_PRODUCER, 0)

			// Send heartbeat message
			if err := protocol.WriteTCPMessage(conn, heartbeat); err != nil {
				log.Printf("[Producer %s] Failed to write heartbeat: %v\n", p.id[:8], err)
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
	if p.udpConn != nil {
		p.udpConn.Close()
	}
}
