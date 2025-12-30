package protocol

import (
	"fmt"
	"net"
	"time"
)

// BroadcastConfig holds broadcast configuration
type BroadcastConfig struct {
	Port       int           // Port to broadcast on
	Timeout    time.Duration // How long to wait for responses
	MaxRetries int           // Number of broadcast retries
	RetryDelay time.Duration // Delay between retries
}

func DefaultBroadcastConfig() *BroadcastConfig {
	return &BroadcastConfig{
		Port:       8888,
		Timeout:    5 * time.Second,
		MaxRetries: 3,
		RetryDelay: 1 * time.Second,
	}
}

// Wrapper for UDP connection used for broadcasting
type BroadcastConnection struct {
	conn *net.UDPConn
}

func CreateBroadcastSender() (*BroadcastConnection, error) {
	// Create unconnected UDP socket
	conn, err := net.ListenUDP("udp", &net.UDPAddr{
		IP:   net.IPv4zero,
		Port: 0,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create broadcast socket: %w", err)
	}

	// Enable broadcast permission, setup write buffer
	if err := conn.SetWriteBuffer(2048 * 1024); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to set write buffer: %w", err)
	}

	return &BroadcastConnection{
		conn: conn,
	}, nil
}

func CreateBroadcastListener(port int) (*BroadcastConnection, error) {
	// Listen on all interfaces for broadcast messages
	addr := &net.UDPAddr{
		IP:   net.IPv4zero,
		Port: port,
	}

	// Create UDP socket
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on broadcast port %d: %w", port, err)
	}

	// Set read buffer
	if err := conn.SetReadBuffer(2048 * 1024); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to set read buffer: %w", err)
	}

	// Return wrapped connection
	return &BroadcastConnection{
		conn: conn,
	}, nil
}

// BroadcastMessage sends a message via broadcast
func (bc *BroadcastConnection) BroadcastMessage(msg Message, broadcastAddr string) error {
	// Resolve broadcast address
	addr, err := net.ResolveUDPAddr("udp", broadcastAddr)
	if err != nil {
		return fmt.Errorf("failed to resolve broadcast address: %w", err)
	}

	// Send the message
	return WriteUDPMessage(bc.conn, msg, addr)
}

// ReceiveMessage receives a broadcast message
func (bc *BroadcastConnection) ReceiveMessage() (Message, *net.UDPAddr, error) {
	// Read the message
	return ReadUDPMessage(bc.conn)
}

// ReceiveMessageWithTimeout receives a broadcast message with timeout
func (bc *BroadcastConnection) ReceiveMessageWithTimeout(timeout time.Duration) (Message, *net.UDPAddr, error) {
	// Set read deadline
	bc.conn.SetReadDeadline(time.Now().Add(timeout))

	// Ensure deadline is cleared after read
	defer bc.conn.SetReadDeadline(time.Time{})

	// Read the message
	return ReadUDPMessage(bc.conn)
}

// Close broadcast connection
func (bc *BroadcastConnection) Close() error {
	if bc.conn != nil {
		return bc.conn.Close()
	}
	return nil
}

// GetLocalAddr returns the local address of the connection
func (bc *BroadcastConnection) GetLocalAddr() net.Addr {
	if bc.conn != nil {
		return bc.conn.LocalAddr()
	}
	return nil
}

// BroadcastJoin sends a JOIN broadcast message
func (bc *BroadcastConnection) BroadcastJoin(senderID string, senderType NodeType, address string, broadcastAddr string) error {
	// Create JOIN message
	msg := NewJoinMsg(senderID, senderType, address)

	// Broadcast the message
	return bc.BroadcastMessage(msg, broadcastAddr)
}

// SendJoinResponse sends a JOIN_RESPONSE to a specific node

func (bc *BroadcastConnection) SendJoinResponse(leaderID, leaderAddr, multicastGroup string, brokers []string, targetAddr string) error {
	// Create JOIN_RESPONSE message
	msg := NewJoinResponseMsg(leaderID, leaderAddr, multicastGroup, brokers)

	// Resolve target address to send response to
	addr, err := net.ResolveUDPAddr("udp", targetAddr)
	if err != nil {
		return fmt.Errorf("failed to resolve target address: %w", err)
	}

	// Send the message
	return WriteUDPMessage(bc.conn, msg, addr)
}

// DiscoverClusterWithRetry broadcasts JOIN and waits for JOIN_RESPONSE
func DiscoverClusterWithRetry(senderID string, senderType NodeType, address string, config *BroadcastConfig) (*JoinResponseMsg, error) {

	// Use default config if none provided
	if config == nil {
		config = DefaultBroadcastConfig()
	}

	// Create broadcast sender
	sender, err := CreateBroadcastSender()
	if err != nil {
		return nil, fmt.Errorf("failed to create broadcast sender: %w", err)
	}
	// Once done, close the sender
	defer sender.Close()

	broadcastAddr := fmt.Sprintf("255.255.255.255:%d", config.Port)

	// Try multiple times to discover the cluster
	for attempt := 0; attempt < config.MaxRetries; attempt++ {

		// Send JOIN broadcast
		if err := sender.BroadcastJoin(senderID, senderType, address, broadcastAddr); err != nil {
			return nil, fmt.Errorf("broadcast failed: %w", err)
		}

		// Wait for response with timeout
		msg, _, err := sender.ReceiveMessageWithTimeout(config.Timeout)
		if err != nil {
			// If we have timeout or other error, retry
			if attempt < config.MaxRetries-1 {

				// Sleep for some time before retrying
				time.Sleep(config.RetryDelay)
				continue
			}
			return nil, fmt.Errorf("no response after %d attempts: %w", config.MaxRetries, err)
		}

		// If we got a JOIN_RESPONSE, return it
		if response, ok := msg.(*JoinResponseMsg); ok {
			return response, nil
		}

		// Sleep before next attempt and retry
		if attempt < config.MaxRetries-1 {
			time.Sleep(config.RetryDelay)
		}
	}

	// If we run out of retries, return error
	return nil, fmt.Errorf("failed to discover cluster after %d attempts", config.MaxRetries)
}
