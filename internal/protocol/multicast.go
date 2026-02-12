package protocol

import (
	"context"
	"fmt"
	"net"
	"syscall"
	"time"

	"golang.org/x/net/ipv4"
)

type MulticastConnection struct {
	conn       *net.UDPConn
	groupAddr  *net.UDPAddr
	packetConn *ipv4.PacketConn
}

func JoinMulticastGroup(multicastAddr, interfaceAddr string) (*MulticastConnection, error) {
	addr, err := net.ResolveUDPAddr("udp4", multicastAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve multicast address: %w", err)
	}

	if !addr.IP.IsMulticast() {
		return nil, fmt.Errorf("address %s is not a multicast address", multicastAddr)
	}

	listenAddr := &net.UDPAddr{
		IP:   net.IPv4zero,
		Port: addr.Port,
	}

	lc := &net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var opErr error
			err := c.Control(func(fd uintptr) {
				// SO_REUSEADDR: Allows reusing addresses in TIME_WAIT state
				opErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
			})
			if err != nil {
				return err
			}
			return opErr
		},
	}

	packetConn, err := lc.ListenPacket(nil, "udp4", listenAddr.String())
	if err != nil {
		return nil, fmt.Errorf("failed to listen on port %d: %w", addr.Port, err)
	}

	conn, ok := packetConn.(*net.UDPConn)
	if !ok {
		packetConn.Close()
		return nil, fmt.Errorf("failed to convert to UDPConn")
	}

	p4 := ipv4.NewPacketConn(conn)

	var iface *net.Interface

	if interfaceAddr != "" && interfaceAddr != "0.0.0.0" {
		ifaces, err := net.Interfaces()
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to get interfaces: %w", err)
		}

		for i := range ifaces {
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

	if err := p4.JoinGroup(iface, &net.UDPAddr{IP: addr.IP}); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to join multicast group: %w", err)
	}

	// Set multicast loopback (receive own messages - useful for testing)
	if err := p4.SetMulticastLoopback(true); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to set multicast loopback: %w", err)
	}

	if err := conn.SetReadBuffer(2048 * 1024); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to set read buffer: %w", err)
	}

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

func CreateMulticastSender(localAddr string) (*MulticastConnection, error) {
	addr, err := net.ResolveUDPAddr("udp4", localAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve local address: %w", err)
	}

	conn, err := net.ListenUDP("udp4", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to create sender: %w", err)
	}

	packetConn := ipv4.NewPacketConn(conn)

	// Set multicast TTL (Time To Live) - how many hops
	// 1 = same subnet only (good for testing)
	if err := packetConn.SetMulticastTTL(1); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to set multicast TTL: %w", err)
	}

	if err := packetConn.SetMulticastLoopback(true); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to set multicast loopback: %w", err)
	}

	var iface *net.Interface
	if addr.IP != nil && !addr.IP.IsUnspecified() {
		ifaces, err := net.Interfaces()
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to get interfaces: %w", err)
		}

		for i := range ifaces {
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

	if err := packetConn.SetMulticastInterface(iface); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to set multicast interface: %w", err)
	}

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

func (mc *MulticastConnection) ReadMessage() (Message, *net.UDPAddr, error) {
	return ReadUDPMessage(mc.conn)
}

func (mc *MulticastConnection) ReadMessageWithTimeout(timeout time.Duration) (Message, *net.UDPAddr, error) {
	mc.conn.SetReadDeadline(time.Now().Add(timeout))
	defer mc.conn.SetReadDeadline(time.Time{}) // Clear deadline
	return ReadUDPMessage(mc.conn)
}

func (mc *MulticastConnection) ReadMessageWithContext(ctx context.Context) (Message, *net.UDPAddr, error) {
	type result struct {
		msg    Message
		addr   *net.UDPAddr
		err    error
	}
	resultChan := make(chan result, 1)

	go func() {
		msg, addr, err := ReadUDPMessage(mc.conn)
		resultChan <- result{msg, addr, err}
	}()

	select {
	case <-ctx.Done():
		// Context cancelled - close connection to unblock the read
		// (This is a bit aggressive but necessary to unblock the read)
		return nil, nil, ctx.Err()
	case res := <-resultChan:
		return res.msg, res.addr, res.err
	}
}

func (mc *MulticastConnection) Close() error {
	if mc.packetConn != nil {
		if mc.groupAddr != nil {
			mc.packetConn.LeaveGroup(nil, &net.UDPAddr{IP: mc.groupAddr.IP})
		}
	}

	if mc.conn != nil {
		return mc.conn.Close()
	}
	return nil
}

func (mc *MulticastConnection) GetLocalAddr() net.Addr {
	if mc.conn != nil {
		return mc.conn.LocalAddr()
	}
	return nil
}

// Includes viewNumber to detect zombie leaders
func SendHeartbeatMulticast(sender *MulticastConnection, senderID string, senderType NodeType, viewNumber int64, senderAddress string, multicastAddr string) error {
	msg := NewHeartbeatMsg(senderID, senderType, 0, viewNumber, senderAddress)
	return sender.SendMessage(msg, multicastAddr)
}

// Includes viewNumber and leaderID for consistency validation by receivers
func SendReplicationMulticast(sender *MulticastConnection, leaderID string, stateSnapshot []byte, updateType string, seqNum int64, viewNumber int64, multicastAddr string) error {
	msg := NewReplicateMsg(leaderID, stateSnapshot, updateType, seqNum, viewNumber, leaderID)
	return sender.SendMessage(msg, multicastAddr)
}

// ringParticipants ensures all nodes use the same filtered ring for consistency
// ringAddrs maps participant IDs to their addresses
func SendElectionMulticast(sender *MulticastConnection, senderID, candidateID string, electionID int64, phase ElectionMessage_Phase, ringParticipants []string, ringAddrs map[string]string, multicastAddr string) error {
	msg := NewElectionMsg(senderID, candidateID, electionID, phase, ringParticipants, ringAddrs)
	return sender.SendMessage(msg, multicastAddr)
}

func SendNackMulticast(sender *MulticastConnection, senderID, targetSenderID string, senderType NodeType, fromSeq, toSeq int64, multicastAddr string) error {
	msg := NewNackMsg(senderID, targetSenderID, senderType, fromSeq, toSeq)
	return sender.SendMessage(msg, multicastAddr)
}