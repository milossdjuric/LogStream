package protocol

import (
	"encoding/binary"
	"fmt"
	"net"
)

func WriteTCPMessage(conn net.Conn, msg Message) error {
	msgBytes, err := msg.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	size := uint32(len(msgBytes))
	if err := binary.Write(conn, binary.BigEndian, size); err != nil {
		return fmt.Errorf("failed to write size: %w", err)
	}

	if _, err := conn.Write(msgBytes); err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	return nil
}

func WriteMessage(conn net.Conn, msg Message) error {
	return WriteTCPMessage(conn, msg)
}

func WriteUDPMessage(conn *net.UDPConn, msg Message, addr *net.UDPAddr) error {
	msgBytes, err := msg.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal UDP message: %w", err)
	}

	_, err = conn.WriteToUDP(msgBytes, addr)
	if err != nil {
		return fmt.Errorf("failed to write UDP message: %w", err)
	}

	return nil
}
