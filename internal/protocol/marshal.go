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
	// Get message details for logging
	msgType := msg.GetHeader().Type
	senderID := msg.GetHeader().SenderId
	seqNum := msg.GetHeader().SequenceNum

	// Marshal the protobuf message
	msgBytes, err := msg.Marshal()
	if err != nil {
		fmt.Printf("[UDP-Write] MARSHAL FAILED: type=%s sender=%s seq=%d err=%v\n",
			msgType, senderID[:8], seqNum, err)
		return fmt.Errorf("failed to marshal UDP message: %w", err)
	}

	// Log the marshaled message
	fmt.Printf("[UDP-Write] Marshaled %s from %s seq=%d size=%d bytes\n",
		msgType, senderID[:8], seqNum, len(msgBytes))

	// Print first 64 bytes in hex
	debugLen := 64
	if len(msgBytes) < debugLen {
		debugLen = len(msgBytes)
	}
	fmt.Printf("[UDP-Write] First %d bytes: %x\n", debugLen, msgBytes[:debugLen])

	// Send the message
	n, err := conn.WriteToUDP(msgBytes, addr)
	if err != nil {
		fmt.Printf("[UDP-Write] SEND FAILED: %v\n", err)
		return fmt.Errorf("failed to write UDP message: %w", err)
	}

	fmt.Printf("[UDP-Write] Sent %d bytes to %s\n\n", n, addr)
	return nil
}
