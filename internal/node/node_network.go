package node

import (
	"context"
	"fmt"
	"log"
	"net"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/milossdjuric/logstream/internal/protocol"
	"golang.org/x/sys/unix"
)

// Network setup and multicast listener functions for broker node

// setupMulticast initializes multicast connections for the node
func (n *Node) setupMulticast() error {
	fmt.Printf("[Node %s] [Multicast-Setup] Joining multicast group: %s\n", n.id[:8], n.config.MulticastGroup)
	fmt.Printf("[Node %s] [Multicast-Setup] Network interface: %s\n", n.id[:8], n.config.NetworkInterface)

	// protocol.JoinMulticastGroup provided by protocol package
	receiver, err := protocol.JoinMulticastGroup(
		n.config.MulticastGroup,
		n.config.NetworkInterface,
	)
	if err != nil {
		fmt.Printf("[Node %s] [Multicast-Setup] ERROR: Failed to join multicast group: %v\n", n.id[:8], err)
		return fmt.Errorf("failed to join multicast: %w", err)
	}
	n.multicastReceiver = receiver
	fmt.Printf("[Node %s] [Multicast-Setup] Successfully joined multicast group\n", n.id[:8])
	fmt.Printf("[Node %s] [Multicast-Setup] Receiver local address: %s\n", n.id[:8], receiver.GetLocalAddr())

	// protocol.CreateMulticastSender provided by protocol package
	senderAddr := fmt.Sprintf("%s:0", n.config.NetworkInterface)
	fmt.Printf("[Node %s] [Multicast-Setup] Creating multicast sender on: %s\n", n.id[:8], senderAddr)
	sender, err := protocol.CreateMulticastSender(senderAddr)
	if err != nil {
		fmt.Printf("[Node %s] [Multicast-Setup] ERROR: Failed to create multicast sender: %v\n", n.id[:8], err)
		n.multicastReceiver.Close()
		return fmt.Errorf("failed to create multicast sender: %w", err)
	}
	n.multicastSender = sender
	fmt.Printf("[Node %s] [Multicast-Setup] Successfully created multicast sender\n", n.id[:8])
	fmt.Printf("[Node %s] [Multicast-Setup] Sender local address: %s\n", n.id[:8], sender.GetLocalAddr())

	return nil
}

// startTCPListener starts the TCP listener for producer/consumer connections AND election messages
func (n *Node) startTCPListener() error {
	// Extract port from node address
	host, port, err := net.SplitHostPort(n.address)
	if err != nil {
		return fmt.Errorf("invalid address: %w", err)
	}

	// Use same port for TCP
	tcpAddr := fmt.Sprintf("%s:%s", host, port)

	// Use ListenConfig to set SO_REUSEADDR for immediate port reuse after cleanup
	lc := &net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var opErr error
			err := c.Control(func(fd uintptr) {
				// SO_REUSEADDR: Allows reusing addresses in TIME_WAIT state
				// This is critical for VMs where cleanup might leave ports in TIME_WAIT
				opErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
			})
			if err != nil {
				return err
			}
			return opErr
		},
	}

	// Listen for TCP connections with reuse option
	listener, err := lc.Listen(context.Background(), "tcp", tcpAddr)
	if err != nil {
		return fmt.Errorf("failed to start TCP listener: %w", err)
	}

	n.tcpListener = listener
	fmt.Printf("[Node %s] TCP listener started on %s (SO_REUSEADDR enabled)\n", n.id[:8], listener.Addr())

	// Accept connections in background
	go func() {
		defer func() {
			if r := recover(); r != nil {
				buf := make([]byte, 4096)
				stackLen := runtime.Stack(buf, false)
				fmt.Printf("[Node %s] [TCP-Listener] PANIC RECOVERED: %v\nStack:\n%s\n", n.id[:8], r, buf[:stackLen])
				log.Printf("[Node %s] [TCP-Listener] PANIC: %v\nStack:\n%s\n", n.id[:8], r, buf[:stackLen])
			}
		}()
		n.acceptTCPConnections()
	}()

	return nil
}

// startUDPDataListener starts the UDP listener for DATA messages from producers
// Producers send DATA via UDP unicast to the broker's address
func (n *Node) startUDPDataListener() error {
	// Extract host and port from node address
	host, port, err := net.SplitHostPort(n.address)
	if err != nil {
		return fmt.Errorf("invalid address: %w", err)
	}

	// Create UDP address for same port as TCP
	udpAddr := fmt.Sprintf("%s:%s", host, port)
	
	// Resolve UDP address
	addr, err := net.ResolveUDPAddr("udp", udpAddr)
	if err != nil {
		return fmt.Errorf("failed to resolve UDP address: %w", err)
	}

	// Create UDP listener with SO_REUSEADDR
	lc := &net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var opErr error
			err := c.Control(func(fd uintptr) {
				opErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
			})
			if err != nil {
				return err
			}
			return opErr
		},
	}

	// Listen for UDP packets
	packetConn, err := lc.ListenPacket(context.Background(), "udp", udpAddr)
	if err != nil {
		return fmt.Errorf("failed to start UDP listener: %w", err)
	}

	n.udpDataListener = packetConn.(*net.UDPConn)
	fmt.Printf("[Node %s] UDP DATA listener started on %s (for producer DATA messages)\n", 
		n.id[:8], addr)

	// Start UDP receive loop in background
	go func() {
		defer func() {
			if r := recover(); r != nil {
				buf := make([]byte, 4096)
				stackLen := runtime.Stack(buf, false)
				fmt.Printf("[Node %s] [UDP-DATA-Listener] PANIC RECOVERED: %v\nStack:\n%s\n", 
					n.id[:8], r, buf[:stackLen])
				log.Printf("[Node %s] [UDP-DATA-Listener] PANIC: %v\nStack:\n%s\n", 
					n.id[:8], r, buf[:stackLen])
			}
		}()
		n.receiveUDPData()
	}()

	return nil
}

// receiveUDPData is the main receive loop for UDP DATA messages
func (n *Node) receiveUDPData() {
	fmt.Printf("[Node %s] [UDP-DATA-Listener] Starting receive loop on %s\n", 
		n.id[:8], n.udpDataListener.LocalAddr())

	for {
		// Check for shutdown before reading
		select {
		case <-n.shutdownCtx.Done():
			fmt.Printf("[Node %s] [UDP-DATA-Listener] Shutdown requested, exiting receive loop cleanly\n", 
				n.id[:8])
			return
		default:
			// Continue with read
		}

		// Set read deadline to allow periodic shutdown checks
		n.udpDataListener.SetReadDeadline(time.Now().Add(1 * time.Second))

		// Read UDP message using protocol package function
		msg, remoteAddr, err := protocol.ReadUDPMessage(n.udpDataListener)
		if err != nil {
			// Check if it's a timeout (normal when checking for shutdown)
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue // Check shutdown and try again
			}

			// Check if connection was closed (shutdown scenario)
			if strings.Contains(err.Error(), "use of closed network connection") {
				fmt.Printf("[Node %s] [UDP-DATA-Listener] Connection closed, exiting receive loop cleanly\n", 
					n.id[:8])
				return // Clean exit
			}

			// Check if we've been shut down
			select {
			case <-n.shutdownCtx.Done():
				fmt.Printf("[Node %s] [UDP-DATA-Listener] Shutdown detected during read error, exiting cleanly\n",
					n.id[:8])
				return
			default:
				// Not shutdown - check if it's just a timeout (normal when no producers sending)
				if strings.Contains(err.Error(), "i/o timeout") {
					// Timeouts are expected when no data is being sent - don't log
					continue
				}
				// Log non-timeout errors
				log.Printf("[Node %s] [UDP-DATA-Listener] Read error (will retry): %v\n", n.id[:8], err)
				time.Sleep(100 * time.Millisecond) // Brief pause before retry
				continue
			}
		}

		// Handle only DATA messages
		if dataMsg, ok := msg.(*protocol.DataMsg); ok {
			fmt.Printf("[Node %s] [UDP-DATA-Listener] Received DATA from %s\n", 
				n.id[:8], remoteAddr)
			// Handle DATA message
			go n.handleData(dataMsg, remoteAddr)
		} else {
			// Ignore non-DATA messages on this listener
			fmt.Printf("[Node %s] [UDP-DATA-Listener] Ignoring non-DATA message type: %T\n", 
				n.id[:8], msg)
		}
	}
}

// acceptTCPConnections is the main accept loop for TCP connections
func (n *Node) acceptTCPConnections() {
	fmt.Printf("[Node %s] [TCP-Listener] Starting accept loop on %s\n", n.id[:8], n.tcpListener.Addr())

	for {
		// Check for shutdown before accepting
		select {
		case <-n.shutdownCtx.Done():
			fmt.Printf("[Node %s] [TCP-Listener] Shutdown requested, exiting accept loop cleanly\n", n.id[:8])
			return
		default:
			// Continue with accept
		}

		// Set a deadline to allow periodic shutdown checks
		if tcpListener, ok := n.tcpListener.(*net.TCPListener); ok {
			tcpListener.SetDeadline(time.Now().Add(1 * time.Second))
		}

		conn, err := n.tcpListener.Accept()
		if err != nil {
			// Check if it's a timeout (normal when checking for shutdown)
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue // Check shutdown and try again
			}

			// Check if listener was closed (shutdown scenario)
			if err.Error() == "use of closed network connection" {
				fmt.Printf("[Node %s] [TCP-Listener] Listener closed, exiting accept loop cleanly\n", n.id[:8])
				return // Clean exit, not a crash
			}

			// Check if we've been shut down
			select {
			case <-n.shutdownCtx.Done():
				fmt.Printf("[Node %s] [TCP-Listener] Shutdown detected during accept error, exiting cleanly\n", n.id[:8])
				return
			default:
				// Not shutdown - log the error but don't crash
				fmt.Printf("[Node %s] [TCP-Listener] Accept error (will retry): %v\n", n.id[:8], err)
				log.Printf("[Node %s] TCP accept error: %v\n", n.id[:8], err)
				time.Sleep(100 * time.Millisecond) // Brief pause before retry
				continue
			}
		}

		// Clear deadline for accepted connection
		if tcpListener, ok := n.tcpListener.(*net.TCPListener); ok {
			tcpListener.SetDeadline(time.Time{})
		}

		remoteAddr := conn.RemoteAddr()
		fmt.Printf("[Node %s] [TCP-Listener] New connection accepted from %s\n", n.id[:8], remoteAddr)

		// Handle connection in goroutine
		go n.handleTCPConnection(conn)
	}
}

// listenMulticast listens for multicast messages
func (n *Node) listenMulticast() {
	fmt.Printf("[Node %s] [Multicast-Listener] ========================================\n", n.id[:8])
	fmt.Printf("[Node %s] [Multicast-Listener] Starting multicast message listener...\n", n.id[:8])
	fmt.Printf("[Node %s] [Multicast-Listener] Receiver: %v\n", n.id[:8], n.multicastReceiver != nil)
	fmt.Printf("[Node %s] [Multicast-Listener] Shutdown context: %v\n", n.id[:8], n.shutdownCtx != nil)
	fmt.Printf("[Node %s] [Multicast-Listener] ========================================\n", n.id[:8])

	errorCount := 0
	maxConsecutiveErrors := 10

	for {
		// Check for shutdown
		select {
		case <-n.shutdownCtx.Done():
			fmt.Printf("[Node %s] [Multicast-Listener] Shutdown requested, exiting cleanly\n", n.id[:8])
			return
		default:
			// Continue with normal operation
		}

		// Set read timeout to allow periodic shutdown checks
		// Use ReadMessageWithTimeout for better control
		msg, sender, err := n.multicastReceiver.ReadMessageWithTimeout(1 * time.Second)

		if err != nil {
			// Check if it's just a timeout (normal when checking for shutdown)
			// Timeouts are expected when no other nodes are sending messages
			errStr := err.Error()

			// Check for timeout errors - use multiple methods to catch all cases
			isTimeout := false

			// Method 1: Check if error implements net.Error and is a timeout
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				isTimeout = true
			}

			// Method 2: Check error string for timeout patterns (some errors don't implement net.Error correctly)
			// Use Contains to catch wrapped errors (e.g., "failed to read UDP message: read udp4 0.0.0.0:9999: i/o timeout")
			if strings.Contains(errStr, "i/o timeout") ||
				strings.Contains(errStr, "deadline exceeded") ||
				strings.Contains(errStr, "read udp") && strings.Contains(errStr, "timeout") {
				isTimeout = true
			}

			if isTimeout {
				continue // Normal timeout, don't log - just check shutdown and try again
			}

			// Check if it's just "not a protobuf message" - this is normal network noise
			if errStr == "not a protobuf message" {
				continue
			}

			// Check if connection was closed (shutdown scenario)
			if err.Error() == "use of closed network connection" ||
				err.Error() == "read udp4 0.0.0.0:9999: use of closed network connection" {
				fmt.Printf("[Node %s] [Multicast-Listener] Connection closed, exiting listener\n", n.id[:8])
				return // Clean exit, not a crash
			}

			// Other errors - log and count
			errorCount++
			if errorCount <= maxConsecutiveErrors {
				fmt.Printf("[Node %s] [Multicast-Listener] ERROR (%d/%d): %v\n",
					n.id[:8], errorCount, maxConsecutiveErrors, err)
				time.Sleep(100 * time.Millisecond) // Brief pause before retry
				continue
			} else {
				// Too many errors - something is seriously wrong
				fmt.Printf("[Node %s] [Multicast-Listener] FATAL: Too many consecutive errors, exiting\n", n.id[:8])
				log.Printf("[Node %s] Multicast listener failed after %d errors, last error: %v\n",
					n.id[:8], errorCount, err)
				return
			}
		}

		// Successful read - reset error count
		errorCount = 0

		msgType := msg.GetHeader().Type
		senderID := msg.GetHeader().SenderId
		fmt.Printf("[Node %s] [Multicast-Listener] Received %s from %s (sender: %s, addr: %s)\n",
			n.id[:8], msgType, senderID[:8], senderID[:8], sender)

		// Route message based on type (protocol provides message types)
		switch m := msg.(type) {
		case *protocol.HeartbeatMsg:
			fmt.Printf("[Node %s] [Multicast-Listener] Routing HEARTBEAT to handler\n", n.id[:8])
			n.handleHeartbeat(msg, sender)

		case *protocol.ReplicateMsg:
			fmt.Printf("[Node %s] [Multicast-Listener] Routing REPLICATE to handler\n", n.id[:8])
			n.handleReplication(m, sender)

		case *protocol.ElectionMsg:
			fmt.Printf("[Node %s] [Multicast-Listener] Routing ELECTION to handler\n", n.id[:8])
			fmt.Printf("[Node %s] [Multicast-Listener] Election details: candidate=%s, electionID=%d, phase=%v\n",
				n.id[:8], m.CandidateId[:8], m.ElectionId, m.Phase)
			n.handleElection(m, sender)

		case *protocol.NackMsg:
			fmt.Printf("[Node %s] [Multicast-Listener] Routing NACK to handler\n", n.id[:8])
			n.handleNack(m, sender)

		case *protocol.DataMsg:
			fmt.Printf("[Node %s] [Multicast-Listener] Routing DATA to handler\n", n.id[:8])
			n.handleData(m, sender)
		}
	}
}

// discoverCluster attempts to discover an existing cluster
// If LEADER_ADDRESS is configured, joins directly via TCP (bypasses broadcast)
// Otherwise, uses broadcast discovery
func (n *Node) discoverCluster() error {
	// Check if explicit leader address is configured (for environments where broadcast doesn't work)
	if n.config.HasExplicitLeader() {
		return n.joinClusterDirectly(n.config.LeaderAddress)
	}

	// Use broadcast discovery
	fmt.Printf("[Node %s] Discovering cluster via broadcast...\n", n.id[:8])

	config := protocol.DefaultBroadcastConfig()
	config.MaxRetries = 2                      // Reduced from 3 to 2 for faster startup
	config.Timeout = 2 * time.Second           // Reduced from 3s to 2s per attempt
	config.RetryDelay = 500 * time.Millisecond // Reduced from 1s to 500ms

	response, err := protocol.DiscoverClusterWithRetry(
		n.id,
		protocol.NodeType_BROKER,
		n.address,
		config,
	)

	if err != nil {
		return err
	}

	fmt.Printf("[Node %s] Found cluster! Leader: %s\n", n.id[:8], response.LeaderAddress)
	n.config.MulticastGroup = response.MulticastGroup

	// Store leader address immediately so we can send heartbeats before registry sync
	n.leaderAddress = response.LeaderAddress
	fmt.Printf("[Node %s] Stored leader address for immediate heartbeats: %s\n", n.id[:8], n.leaderAddress)

	return nil
}

// joinClusterDirectly connects to the leader via TCP to join the cluster
// This bypasses broadcast discovery for environments with AP isolation
func (n *Node) joinClusterDirectly(leaderAddress string) error {
	fmt.Printf("[Node %s] Joining cluster directly via TCP to leader at %s\n", n.id[:8], leaderAddress)
	fmt.Printf("[Node %s] (Broadcast discovery bypassed - LEADER_ADDRESS configured)\n", n.id[:8])

	// Connect to leader via TCP
	conn, err := net.DialTimeout("tcp", leaderAddress, 5*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to leader at %s: %w", leaderAddress, err)
	}
	defer conn.Close()

	// Send JOIN message via TCP
	joinMsg := protocol.NewJoinMsg(n.id, protocol.NodeType_BROKER, n.address)
	if err := protocol.WriteTCPMessage(conn, joinMsg); err != nil {
		return fmt.Errorf("failed to send JOIN to leader: %w", err)
	}

	// Wait for JOIN_RESPONSE
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	response, err := protocol.ReadTCPMessage(conn)
	if err != nil {
		return fmt.Errorf("failed to receive JOIN_RESPONSE from leader: %w", err)
	}

	joinResponse, ok := response.(*protocol.JoinResponseMsg)
	if !ok {
		return fmt.Errorf("unexpected response type: %T (expected JoinResponseMsg)", response)
	}

	fmt.Printf("[Node %s] Joined cluster! Leader: %s, Multicast: %s\n",
		n.id[:8], joinResponse.LeaderAddress, joinResponse.MulticastGroup)

	// Store cluster configuration
	n.leaderAddress = joinResponse.LeaderAddress
	n.config.MulticastGroup = joinResponse.MulticastGroup

	return nil
}
