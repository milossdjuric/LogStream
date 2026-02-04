package protocol

import (
	"fmt"
	"net"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// BroadcastConfig holds broadcast configuration with circuit breaker settings
type BroadcastConfig struct {
	Port       int           // Port to broadcast on
	Timeout    time.Duration // How long to wait for responses per attempt
	MaxRetries int           // Number of broadcast retries

	// Circuit breaker settings
	RetryDelay           time.Duration // Initial delay between retries
	MaxRetryDelay        time.Duration // Maximum delay (cap for exponential backoff)
	BackoffMultiplier    float64       // Multiplier for exponential backoff
	SplitBrainDelay      time.Duration // Extra delay when split-brain detected
	ResponseCollectTime  time.Duration // Time to collect multiple responses (for split-brain detection)
}

func DefaultBroadcastConfig() *BroadcastConfig {
	return &BroadcastConfig{
		Port:                 8888,
		Timeout:              5 * time.Second,
		MaxRetries:           5,                    // Increased from 3 for circuit breaker
		RetryDelay:           1 * time.Second,      // Initial delay
		MaxRetryDelay:        10 * time.Second,     // Cap exponential backoff at 10s
		BackoffMultiplier:    1.5,                  // Exponential backoff multiplier
		SplitBrainDelay:      5 * time.Second,      // Extra delay when split-brain detected
		ResponseCollectTime:  500 * time.Millisecond, // Time to collect multiple responses
	}
}

type BroadcastConnection struct {
	conn *net.UDPConn
}

func CreateBroadcastSender() (*BroadcastConnection, error) {
	// Use ListenConfig to set socket options BEFORE binding to force IPv4
	lc := &net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var opErr error
			err := c.Control(func(fd uintptr) {
				// Force IPv4-only by disabling IPv6 (IPV6_V6ONLY = 0 means IPv4 can use IPv6 socket, but we want IPv4-only)
				// Actually, for IPv4 sockets, we don't need IPV6_V6ONLY, but we can set SO_BROADCAST
				opErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_BROADCAST, 1)
			})
			if err != nil {
				return err
			}
			return opErr
		},
	}

	// Create unconnected UDP socket - force IPv4 to avoid IPv6 issues in network namespaces
	listenAddr := &net.UDPAddr{
		IP:   net.IPv4zero,
		Port: 0,
	}

	packetConn, err := lc.ListenPacket(nil, "udp4", listenAddr.String())
	if err != nil {
		return nil, fmt.Errorf("failed to create broadcast socket: %w", err)
	}

	conn, ok := packetConn.(*net.UDPConn)
	if !ok {
		packetConn.Close()
		return nil, fmt.Errorf("failed to convert to UDPConn")
	}

	// Broadcast is already enabled via SO_BROADCAST socket option above
	// Setup write buffer
	if err := conn.SetWriteBuffer(2048 * 1024); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to set write buffer: %w", err)
	}

	return &BroadcastConnection{
		conn: conn,
	}, nil
}

func CreateBroadcastListener(port int) (*BroadcastConnection, error) {
	// Use ListenConfig to set socket options BEFORE binding to force IPv4
	lc := &net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var opErr error
			err := c.Control(func(fd uintptr) {
				// Enable SO_REUSEADDR for immediate port reuse
				opErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
			})
			if err != nil {
				return err
			}
			return opErr
		},
	}

	// Listen on all interfaces for broadcast messages - force IPv4 to avoid IPv6 issues in network namespaces
	listenAddr := &net.UDPAddr{
		IP:   net.IPv4zero,
		Port: port,
	}

	// Create UDP socket - force IPv4 with socket options
	packetConn, err := lc.ListenPacket(nil, "udp4", listenAddr.String())
	if err != nil {
		return nil, fmt.Errorf("failed to listen on broadcast port %d: %w", port, err)
	}

	conn, ok := packetConn.(*net.UDPConn)
	if !ok {
		packetConn.Close()
		return nil, fmt.Errorf("failed to convert to UDPConn")
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

func (bc *BroadcastConnection) BroadcastMessage(msg Message, broadcastAddr string) error {
	addr, err := net.ResolveUDPAddr("udp4", broadcastAddr)
	if err != nil {
		return fmt.Errorf("failed to resolve broadcast address: %w", err)
	}
	return WriteUDPMessage(bc.conn, msg, addr)
}

func (bc *BroadcastConnection) ReceiveMessage() (Message, *net.UDPAddr, error) {
	return ReadUDPMessage(bc.conn)
}

func (bc *BroadcastConnection) ReceiveMessageWithTimeout(timeout time.Duration) (Message, *net.UDPAddr, error) {
	bc.conn.SetReadDeadline(time.Now().Add(timeout))
	defer bc.conn.SetReadDeadline(time.Time{})
	return ReadUDPMessage(bc.conn)
}

func (bc *BroadcastConnection) Close() error {
	if bc.conn != nil {
		return bc.conn.Close()
	}
	return nil
}

func (bc *BroadcastConnection) GetLocalAddr() net.Addr {
	if bc.conn != nil {
		return bc.conn.LocalAddr()
	}
	return nil
}

func (bc *BroadcastConnection) BroadcastJoin(senderID string, senderType NodeType, address string, broadcastAddr string) error {
	msg := NewJoinMsg(senderID, senderType, address)
	return bc.BroadcastMessage(msg, broadcastAddr)
}

func (bc *BroadcastConnection) SendJoinResponse(leaderID, leaderAddr, multicastGroup string, brokers []string, targetAddr string) error {
	msg := NewJoinResponseMsg(leaderID, leaderAddr, multicastGroup, brokers)
	addr, err := net.ResolveUDPAddr("udp4", targetAddr)
	if err != nil {
		return fmt.Errorf("failed to resolve target address: %w", err)
	}
	return WriteUDPMessage(bc.conn, msg, addr)
}

// calculateBroadcastAddress calculates the subnet-specific broadcast address
// from the node's IP address. This is required for network namespaces where
// 255.255.255.255 doesn't work.
func calculateBroadcastAddress(nodeAddress string, port int) (string, error) {
	// Extract IP from address (format: "IP:PORT")
	host, _, err := net.SplitHostPort(nodeAddress)
	if err != nil {
		// If no port, assume it's just an IP
		host = nodeAddress
	}

	nodeIP := net.ParseIP(host)
	if nodeIP == nil {
		return fmt.Sprintf("255.255.255.255:%d", port), nil // Fallback to global broadcast
	}

	// Convert to IPv4 if it's IPv6-mapped IPv4 (::ffff:192.168.1.1 format)
	// or ensure we have IPv4
	nodeIPv4 := nodeIP.To4()
	if nodeIPv4 == nil {
		// Not an IPv4 address - fallback to global broadcast
		return fmt.Sprintf("255.255.255.255:%d", port), nil
	}
	nodeIP = nodeIPv4 // Use IPv4 version

	// Find the interface that has this IP
	ifaces, err := net.Interfaces()
	if err != nil {
		return fmt.Sprintf("255.255.255.255:%d", port), nil // Fallback
	}

	for _, iface := range ifaces {
		// Only skip if interface is down
		// Allow loopback interfaces for localhost testing (127.0.0.1)
		if iface.Flags&net.FlagUp == 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			if ipnet, ok := addr.(*net.IPNet); ok {
				// Convert to IPv4 for comparison
				ipnetIPv4 := ipnet.IP.To4()
				if ipnetIPv4 == nil {
					continue
				}
				if ipnetIPv4.Equal(nodeIP) {
					// Found the interface - calculate broadcast address
					// broadcast = (IP | ~mask)
					mask := ipnet.Mask
					// Ensure mask length matches IP length (IPv4 = 4 bytes)
					if len(mask) != 4 {
						// Mask length mismatch, skip this interface
						continue
					}
					broadcast := make(net.IP, 4) // IPv4 is always 4 bytes
					for i := 0; i < 4; i++ {
						broadcast[i] = nodeIP[i] | ^mask[i]
					}
					return fmt.Sprintf("%s:%d", broadcast.String(), port), nil
				}
			}
		}
	}

	// If we can't find the interface, try to infer from common subnets
	// Network Namespaces: 172.20.0.x/24
	if nodeIP[0] == 172 && nodeIP[1] == 20 {
		return fmt.Sprintf("172.20.0.255:%d", port), nil
	}
	// Docker subnets: 172.25.x.x, 172.26.x.x, 172.28.x.x, 172.29.x.x (all /24)
	if nodeIP[0] == 172 && (nodeIP[1] == 25 || nodeIP[1] == 26 || nodeIP[1] == 28 || nodeIP[1] == 29) {
		return fmt.Sprintf("172.%d.0.255:%d", nodeIP[1], port), nil
	}
	// Vagrant VMs: 192.168.100.x/24
	if nodeIP[0] == 192 && nodeIP[1] == 168 && nodeIP[2] == 100 {
		return fmt.Sprintf("192.168.100.255:%d", port), nil
	}
	// Vagrant VMs (backup): 192.168.56.x/24
	if nodeIP[0] == 192 && nodeIP[1] == 168 && nodeIP[2] == 56 {
		return fmt.Sprintf("192.168.56.255:%d", port), nil
	}

	// Fallback: calculate from /24 assumption (most common for private networks)
	// This works for most Docker, VM, and netns scenarios
	// nodeIP is guaranteed to be IPv4 (4 bytes) at this point
	broadcast := make(net.IP, 4)
	copy(broadcast, nodeIP)
	broadcast[3] = 255
	return fmt.Sprintf("%s:%d", broadcast.String(), port), nil
}

// DiscoverClusterWithRetry broadcasts JOIN and waits for JOIN_RESPONSE
// Implements circuit breaker pattern with exponential backoff and split-brain detection
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

	// Calculate subnet-specific broadcast address (required for network namespaces)
	broadcastAddr, err := calculateBroadcastAddress(address, config.Port)
	if err != nil {
		// Fallback to global broadcast if calculation fails
		broadcastAddr = fmt.Sprintf("255.255.255.255:%d", config.Port)
	}

	// Circuit breaker: track current delay for exponential backoff
	currentDelay := config.RetryDelay
	consecutiveFailures := 0

	// Try multiple times to discover the cluster with circuit breaker pattern
	for attempt := 0; attempt < config.MaxRetries; attempt++ {
		fmt.Printf("[Discovery] Attempt %d/%d (delay: %v)\n", attempt+1, config.MaxRetries, currentDelay)

		// Send JOIN broadcast
		if err := sender.BroadcastJoin(senderID, senderType, address, broadcastAddr); err != nil {
			return nil, fmt.Errorf("broadcast failed: %w", err)
		}

		// Collect ALL responses within a collection window (for split-brain detection)
		responses := collectJoinResponses(sender, config.Timeout, config.ResponseCollectTime)

		if len(responses) == 0 {
			// No responses - increment failure counter and apply backoff
			consecutiveFailures++
			fmt.Printf("[Discovery] No response received (failure #%d)\n", consecutiveFailures)

			if attempt < config.MaxRetries-1 {
				// Exponential backoff with cap
				time.Sleep(currentDelay)
				currentDelay = time.Duration(float64(currentDelay) * config.BackoffMultiplier)
				if currentDelay > config.MaxRetryDelay {
					currentDelay = config.MaxRetryDelay
				}
				continue
			}
			return nil, fmt.Errorf("no response after %d attempts", config.MaxRetries)
		}

		// Reset failure counter on successful response
		consecutiveFailures = 0

		// Check for split-brain: multiple different leaders responded
		uniqueLeaders := getUniqueLeaders(responses)
		if len(uniqueLeaders) > 1 {
			fmt.Printf("[Discovery] SPLIT-BRAIN DETECTED: %d different leaders responded\n", len(uniqueLeaders))
			for leaderAddr := range uniqueLeaders {
				fmt.Printf("[Discovery]   - Leader at: %s\n", leaderAddr)
			}

			// Sleep to allow cluster to resolve split-brain via election
			fmt.Printf("[Discovery] Sleeping %v to allow election to resolve split-brain...\n", config.SplitBrainDelay)
			time.Sleep(config.SplitBrainDelay)

			// Apply additional backoff for split-brain scenario
			currentDelay = time.Duration(float64(currentDelay) * config.BackoffMultiplier)
			if currentDelay > config.MaxRetryDelay {
				currentDelay = config.MaxRetryDelay
			}

			// Retry discovery - do NOT make a deterministic choice
			// Let the cluster resolve split-brain via election
			if attempt < config.MaxRetries-1 {
				continue
			}

			// If still seeing split-brain after all retries, return error
			// The caller should handle this (e.g., become leader themselves or fail)
			return nil, fmt.Errorf("split-brain detected after %d attempts: %d different leaders responding",
				config.MaxRetries, len(uniqueLeaders))
		}

		// Single leader - cluster is healthy, join it
		fmt.Printf("[Discovery] Found cluster with leader at %s\n", responses[0].LeaderAddress)
		return responses[0], nil
	}

	// If we run out of retries, return error
	return nil, fmt.Errorf("failed to discover cluster after %d attempts", config.MaxRetries)
}

// collectJoinResponses collects multiple JOIN_RESPONSE messages within a time window
// This allows detecting split-brain where multiple leaders respond
func collectJoinResponses(sender *BroadcastConnection, timeout, collectTime time.Duration) []*JoinResponseMsg {
	var responses []*JoinResponseMsg
	deadline := time.Now().Add(timeout)
	collectDeadline := time.Now().Add(collectTime)

	for time.Now().Before(deadline) {
		// Calculate remaining time for this read
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}

		// Use shorter timeout for individual reads to allow collecting multiple responses
		readTimeout := remaining
		if readTimeout > 200*time.Millisecond {
			readTimeout = 200 * time.Millisecond
		}

		msg, _, err := sender.ReceiveMessageWithTimeout(readTimeout)
		if err != nil {
			// Timeout on this read - check if we should continue collecting
			if len(responses) > 0 && time.Now().After(collectDeadline) {
				// We have at least one response and collection window expired
				break
			}
			// Keep trying until main timeout
			continue
		}

		// Check if it's a JOIN_RESPONSE
		if response, ok := msg.(*JoinResponseMsg); ok {
			responses = append(responses, response)
			fmt.Printf("[Discovery] Received JOIN_RESPONSE from leader %s\n", response.LeaderAddress)

			// If collection window expired and we have responses, we're done
			if time.Now().After(collectDeadline) {
				break
			}
		}
	}

	return responses
}

// getUniqueLeaders extracts unique leader addresses from responses
func getUniqueLeaders(responses []*JoinResponseMsg) map[string]bool {
	leaders := make(map[string]bool)
	for _, r := range responses {
		leaders[r.LeaderAddress] = true
	}
	return leaders
}
