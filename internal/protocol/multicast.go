package protocol

import (
	"fmt"
	"net"
	"syscall"

	"golang.org/x/net/ipv4"
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

	// Listen on the multicast port with SO_REUSEADDR and SO_REUSEPORT
	listenAddr := &net.UDPAddr{
		IP:   net.IPv4zero,
		Port: addr.Port,
	}

	// Use ListenConfig to set socket options BEFORE binding
	lc := &net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error { // syscall.RawConn here!
			var opErr error
			err := c.Control(func(fd uintptr) {
				// SO_REUSEADDR: Allows reusing addresses in TIME_WAIT state
				// On Windows, SO_REUSEADDR also allows binding to the same port  (Pedro: I added windows support)
				opErr = syscall.SetsockoptInt(syscall.Handle(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
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

	// Get the network interface to use
	var iface *net.Interface
	if interfaceAddr != "" && interfaceAddr != "0.0.0.0" {
		ifaces, err := net.Interfaces()
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to get interfaces: %w", err)
		}

		for i := range ifaces {
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

	fmt.Printf("[Multicast] Joined group %s on interface %v (port reuse enabled)\n", addr.IP, iface)

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

	// Set write buffer size
	if err := conn.SetWriteBuffer(2048 * 1024); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to set write buffer: %w", err)
	}

	return &MulticastConnection{
		conn:       conn,
		packetConn: packetConn,
	}, nil
}

// SendMessage sends a message to a multicast group
func (mc *MulticastConnection) SendMessage(msg Message, targetAddr string) error {
	addr, err := net.ResolveUDPAddr("udp4", targetAddr)
	if err != nil {
		return fmt.Errorf("failed to resolve target address: %w", err)
	}

	if !addr.IP.IsMulticast() {
		return fmt.Errorf("target address %s is not a multicast address", targetAddr)
	}

	return WriteUDPMessage(mc.conn, msg, addr)
}

// ReadMessage receives a message from the multicast group
func (mc *MulticastConnection) ReadMessage() (Message, *net.UDPAddr, error) {
	return ReadUDPMessage(mc.conn)
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

// ============================================================================
// CONVENIENCE FUNCTIONS FOR COMMON MULTICAST OPERATIONS
// ============================================================================

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
