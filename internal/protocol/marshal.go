package protocol

import (
	"encoding/binary"
	"fmt"
	"net"
)

// Send a TCP message
func WriteTCPMessage(conn net.Conn, msg Message) error {
	// Marshal the protobuf message
	msgBytes, err := msg.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// Write size prefix (4 bytes, big endian)
	size := uint32(len(msgBytes))
	if err := binary.Write(conn, binary.BigEndian, size); err != nil {
		return fmt.Errorf("failed to write size: %w", err)
	}

	// Write message bytes
	if _, err := conn.Write(msgBytes); err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	return nil
}

// Wrapper for WriteTCPMessage
func WriteMessage(conn net.Conn, msg Message) error {
	return WriteTCPMessage(conn, msg)
}

// Send a UDP message to specified address
func WriteUDPMessage(conn *net.UDPConn, msg Message, addr *net.UDPAddr) error {
	msgBytes, err := msg.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal UDP message: %w", err)
	}

	// Send the message, no need for size prefix
	_, err = conn.WriteToUDP(msgBytes, addr)
	if err != nil {
		return fmt.Errorf("failed to write UDP message: %w", err)
	}

	return nil
}
