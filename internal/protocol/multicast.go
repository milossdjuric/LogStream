package protocol

import (
	"context"
	"fmt"
	"net"
	"syscall"
	"time"

	"golang.org/x/net/ipv4"
	"golang.org/x/sys/unix"
)

// MulticastConnection wraps a UDP connection for multicast communication
type MulticastConnection struct {
	conn       *net.UDPConn
	groupAddr  *net.UDPAddr
	packetConn *ipv4.PacketConn
}

// JoinMulticastGroup joins a UDP multicast group
func JoinMulticastGroup(multicastAddr, interfaceAddr string) (*MulticastConnection, error) {
	// Parse multicast group address
	addr, err := net.ResolveUDPAddr("udp4", multicastAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve multicast address: %w", err)
	}

	// Validate it's a multicast address
	if !addr.IP.IsMulticast() {
		return nil, fmt.Errorf("address %s is not a multicast address", multicastAddr)
	}

	// Listen on the multicast port with SO_REUSEADDR
	listenAddr := &net.UDPAddr{
		IP:   net.IPv4zero,
		Port: addr.Port,
	}

	// Use ListenConfig to set socket options BEFORE binding
	lc := &net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var opErr error
			err := c.Control(func(fd uintptr) {
				// SO_REUSEADDR: Allows reusing addresses in TIME_WAIT state
				// NOTE: We do NOT use SO_REUSEPORT for multicast receivers
				// because it causes the kernel to deliver each multicast packet
				// to only ONE socket instead of ALL sockets (load balancing behavior)
				opErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
			})
			if err != nil {
				return err
			}
			return opErr
		},
	}

	// Create packet connection with reuse options
	packetConn, err := lc.ListenPacket(nil, "udp4", listenAddr.String())
	if err != nil {
		return nil, fmt.Errorf("failed to listen on port %d: %w", addr.Port, err)
	}

	// Type assert to UDPConn
	conn, ok := packetConn.(*net.UDPConn)
	if !ok {
		packetConn.Close()
		return nil, fmt.Errorf("failed to convert to UDPConn")
	}

	// Wrap in PacketConn for multicast operations
	p4 := ipv4.NewPacketConn(conn)

	// FIXED: Determine which interface to use for multicast
	// Always try to find the specific interface for Vagrant VMs
	var iface *net.Interface

	if interfaceAddr != "" && interfaceAddr != "0.0.0.0" {
		ifaces, err := net.Interfaces()
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to get interfaces: %w", err)
		}

		for i := range ifaces {
			// Skip loopback and down interfaces
			if ifaces[i].Flags&net.FlagLoopback != 0 || ifaces[i].Flags&net.FlagUp == 0 {
				continue
			}

			addrs, err := ifaces[i].Addrs()
			if err != nil {
				continue
			}

			for _, a := range addrs {
				if ipnet, ok := a.(*net.IPNet); ok {
					if ipnet.IP.String() == interfaceAddr {
						iface = &ifaces[i]
						break
					}
				}
			}
			if iface != nil {
				break
			}
		}

		// If we couldn't find the specific interface, warn but continue
		if iface == nil {
			fmt.Printf("[Multicast] WARNING: Could not find interface for %s, using default\n", interfaceAddr)
		}
	}

	// JOIN THE MULTICAST GROUP
	if err := p4.JoinGroup(iface, &net.UDPAddr{IP: addr.IP}); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to join multicast group: %w", err)
	}

	// Set multicast loopback (receive own messages - useful for testing)
	if err := p4.SetMulticastLoopback(true); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to set multicast loopback: %w", err)
	}

	// Set read buffer size
	if err := conn.SetReadBuffer(2048 * 1024); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to set read buffer: %w", err)
	}

	// Log the interface being used
	if iface == nil {
		fmt.Printf("[Multicast] Joined group %s on default interface (no specific interface found)\n", addr.IP)
	} else {
		fmt.Printf("[Multicast] Joined group %s on interface %s (%s)\n",
			addr.IP, iface.Name, interfaceAddr)
	}

	return &MulticastConnection{
		conn:       conn,
		groupAddr:  addr,
		packetConn: p4,
	}, nil
}

// CreateMulticastSender creates a UDP connection for sending to multicast group
func CreateMulticastSender(localAddr string) (*MulticastConnection, error) {
	addr, err := net.ResolveUDPAddr("udp4", localAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve local address: %w", err)
	}

	conn, err := net.ListenUDP("udp4", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to create sender: %w", err)
	}

	// Wrap for multicast options
	packetConn := ipv4.NewPacketConn(conn)

	// Set multicast TTL (Time To Live) - how many hops
	// 1 = same subnet only (good for testing)
	if err := packetConn.SetMulticastTTL(1); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to set multicast TTL: %w", err)
	}

	// Set multicast loopback
	if err := packetConn.SetMulticastLoopback(true); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to set multicast loopback: %w", err)
	}

	// Find the interface that has this local address and use it for multicast
	var iface *net.Interface
	if addr.IP != nil && !addr.IP.IsUnspecified() {
		// Get the interface that has this IP address
		ifaces, err := net.Interfaces()
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to get interfaces: %w", err)
		}

		for i := range ifaces {
			// Skip loopback and down interfaces
			if ifaces[i].Flags&net.FlagLoopback != 0 || ifaces[i].Flags&net.FlagUp == 0 {
				continue
			}

			addrs, err := ifaces[i].Addrs()
			if err != nil {
				continue
			}
			for _, a := range addrs {
				if ipnet, ok := a.(*net.IPNet); ok {
					if ipnet.IP.Equal(addr.IP) {
						iface = &ifaces[i]
						fmt.Printf("[Multicast] Found interface %s for IP %s\n", ifaces[i].Name, addr.IP)
						break
					}
				}
			}
			if iface != nil {
				break
			}
		}
	}

	// Set the outgoing multicast interface
	if err := packetConn.SetMulticastInterface(iface); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to set multicast interface: %w", err)
	}

	// Set write buffer size
	if err := conn.SetWriteBuffer(2048 * 1024); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to set write buffer: %w", err)
	}

	if iface != nil {
		fmt.Printf("[Multicast] Sender configured on interface %s for cross-container multicast\n", iface.Name)
	} else {
		fmt.Printf("[Multicast] Sender configured with default interface\n")
	}

	return &MulticastConnection{
		conn:       conn,
		packetConn: packetConn,
	}, nil
}

// SendMessage sends a message to a multicast group
func (mc *MulticastConnection) SendMessage(msg Message, targetAddr string) error {
	msgType := msg.GetHeader().Type
	senderID := msg.GetHeader().SenderId
	seqNum := msg.GetHeader().SequenceNum

	fmt.Printf("[Multicast-Send] Preparing to send %s message\n", msgType)
	fmt.Printf("[Multicast-Send] Sender: %s, Sequence: %d\n", senderID[:8], seqNum)
	fmt.Printf("[Multicast-Send] Target: %s\n", targetAddr)

	addr, err := net.ResolveUDPAddr("udp4", targetAddr)
	if err != nil {
		fmt.Printf("[Multicast-Send] ERROR: Failed to resolve target address: %v\n", err)
		return fmt.Errorf("failed to resolve target address: %w", err)
	}

	if !addr.IP.IsMulticast() {
		fmt.Printf("[Multicast-Send] ERROR: Target address %s is not a multicast address\n", targetAddr)
		return fmt.Errorf("target address %s is not a multicast address", targetAddr)
	}

	fmt.Printf("[Multicast-Send] Sending message to multicast group %s...\n", targetAddr)
	err = WriteUDPMessage(mc.conn, msg, addr)
	if err != nil {
		fmt.Printf("[Multicast-Send] ERROR: Failed to send message: %v\n", err)
	} else {
		fmt.Printf("[Multicast-Send] Successfully sent %s message to %s\n\n", msgType, targetAddr)
	}
	return err
}

// ReadMessage receives a message from the multicast group
func (mc *MulticastConnection) ReadMessage() (Message, *net.UDPAddr, error) {
	return ReadUDPMessage(mc.conn)
}

// ReadMessageWithTimeout receives a message with a timeout
func (mc *MulticastConnection) ReadMessageWithTimeout(timeout time.Duration) (Message, *net.UDPAddr, error) {
	mc.conn.SetReadDeadline(time.Now().Add(timeout))
	defer mc.conn.SetReadDeadline(time.Time{}) // Clear deadline
	return ReadUDPMessage(mc.conn)
}

// ReadMessageWithContext receives a message with context cancellation support
func (mc *MulticastConnection) ReadMessageWithContext(ctx context.Context) (Message, *net.UDPAddr, error) {
	// Create a channel for the result
	type result struct {
		msg    Message
		addr   *net.UDPAddr
		err    error
	}
	resultChan := make(chan result, 1)

	// Read in a goroutine
	go func() {
		msg, addr, err := ReadUDPMessage(mc.conn)
		resultChan <- result{msg, addr, err}
	}()

	// Wait for either result or context cancellation
	select {
	case <-ctx.Done():
		// Context cancelled - close connection to unblock the read
		// (This is a bit aggressive but necessary to unblock the read)
		return nil, nil, ctx.Err()
	case res := <-resultChan:
		return res.msg, res.addr, res.err
	}
}

// Close closes the multicast connection and leaves the group
func (mc *MulticastConnection) Close() error {
	if mc.packetConn != nil {
		// Leave the multicast group
		if mc.groupAddr != nil {
			mc.packetConn.LeaveGroup(nil, &net.UDPAddr{IP: mc.groupAddr.IP})
		}
	}

	if mc.conn != nil {
		return mc.conn.Close()
	}
	return nil
}

// GetLocalAddr returns the local address of the connection
func (mc *MulticastConnection) GetLocalAddr() net.Addr {
	if mc.conn != nil {
		return mc.conn.LocalAddr()
	}
	return nil
}

// SendHeartbeatMulticast sends a heartbeat to the multicast group
func SendHeartbeatMulticast(sender *MulticastConnection, senderID string, senderType NodeType, multicastAddr string) error {
	msg := NewHeartbeatMsg(senderID, senderType, 0)
	return sender.SendMessage(msg, multicastAddr)
}

// SendReplicationMulticast sends state replication to the multicast group
func SendReplicationMulticast(sender *MulticastConnection, leaderID string, stateSnapshot []byte, updateType string, seqNum int64, multicastAddr string) error {
	msg := NewReplicateMsg(leaderID, stateSnapshot, updateType, seqNum)
	return sender.SendMessage(msg, multicastAddr)
}

// SendElectionMulticast sends an election message to the multicast group
func SendElectionMulticast(sender *MulticastConnection, senderID, candidateID string, electionID int64, phase ElectionMessage_Phase, multicastAddr string) error {
	msg := NewElectionMsg(senderID, candidateID, electionID, phase)
	return sender.SendMessage(msg, multicastAddr)
}

// SendNackMulticast sends a NACK message to request missing messages
func SendNackMulticast(sender *MulticastConnection, senderID, targetSenderID string, senderType NodeType, fromSeq, toSeq int64, multicastAddr string) error {
	msg := NewNackMsg(senderID, targetSenderID, senderType, fromSeq, toSeq)
	return sender.SendMessage(msg, multicastAddr)
}