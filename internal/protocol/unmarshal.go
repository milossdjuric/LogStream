package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"google.golang.org/protobuf/proto"
)

// ReadTCPMessage reads and unmarshals a message from a TCP connection
func ReadTCPMessage(conn net.Conn) (Message, error) {
	// Read size prefix
	var size uint32
	if err := binary.Read(conn, binary.BigEndian, &size); err != nil {
		return nil, fmt.Errorf("failed to read size: %w", err)
	}

	// Read message bytes
	msgBytes := make([]byte, size)
	if _, err := io.ReadFull(conn, msgBytes); err != nil {
		return nil, fmt.Errorf("failed to read message: %w", err)
	}

	// Unmarshal and return message
	return unmarshalMessage(msgBytes)
}

// Wrapper for ReadTCPMessage
func ReadMessage(conn net.Conn) (Message, error) {
	return ReadTCPMessage(conn)
}

// ReadUDPMessage reads and unmarshals a message from a UDP connection
func ReadUDPMessage(conn *net.UDPConn) (Message, *net.UDPAddr, error) {
	buffer := make([]byte, 65536) // Max UDP packet size
	n, addr, err := conn.ReadFromUDP(buffer)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read UDP message: %w", err)
	}

	// Unmarshal the datagram
	msg, err := unmarshalMessage(buffer[:n])
	return msg, addr, err
}

// MesasageFactory is a function that creates a new protobuf message instance
type MessageFactory func() proto.Message

// Registry mapping MessageType to corresponding constructor
var messageRegistry = map[MessageType]MessageFactory{
	MessageType_PRODUCE:       func() proto.Message { return &ProduceMessage{} },
	MessageType_DATA:          func() proto.Message { return &DataMessage{} },
	MessageType_CONSUME:       func() proto.Message { return &ConsumeMessage{} },
	MessageType_RESULT:        func() proto.Message { return &ResultMessage{} },
	MessageType_HEARTBEAT:     func() proto.Message { return &HeartbeatMessage{} },
	MessageType_JOIN:          func() proto.Message { return &JoinMessage{} },
	MessageType_JOIN_RESPONSE: func() proto.Message { return &JoinResponseMessage{} },
	MessageType_ELECTION:      func() proto.Message { return &ElectionMessage{} },
	MessageType_REPLICATE:     func() proto.Message { return &ReplicateMessage{} },
	MessageType_NACK:          func() proto.Message { return &NackMessage{} },
}

// It first peeks at the header to determine the message type, then unmarshals accordingly to message type
func unmarshalMessage(data []byte) (Message, error) {
	// Unmarshal header
	temp := &ProduceMessage{}
	if err := proto.Unmarshal(data, temp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal header: %w", err)
	}

	// If no header, error
	if temp.Header == nil {
		return nil, fmt.Errorf("message has no header")
	}

	// Lookup message type in registry based on its type in header
	factory, ok := messageRegistry[temp.Header.Type]
	if !ok {
		return nil, fmt.Errorf("unknown message type: %v", temp.Header.Type)
	}

	// Create new protobuf message instance
	protoMsg := factory()
	if err := proto.Unmarshal(data, protoMsg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}

	// Wrap the protobuf message in its Go wrapper type
	return wrapProtoMessage(protoMsg)
}

// Wraps a protobuf message into its corresponding Message interface wrapper
func wrapProtoMessage(pm proto.Message) (Message, error) {
	switch m := pm.(type) {
	case *ProduceMessage:
		return &ProduceMsg{ProduceMessage: m}, nil
	case *DataMessage:
		return &DataMsg{DataMessage: m}, nil
	case *ConsumeMessage:
		return &ConsumeMsg{ConsumeMessage: m}, nil
	case *ResultMessage:
		return &ResultMsg{ResultMessage: m}, nil
	case *HeartbeatMessage:
		return &HeartbeatMsg{HeartbeatMessage: m}, nil
	case *JoinMessage:
		return &JoinMsg{JoinMessage: m}, nil
	case *JoinResponseMessage:
		return &JoinResponseMsg{JoinResponseMessage: m}, nil
	case *ElectionMessage:
		return &ElectionMsg{ElectionMessage: m}, nil
	case *ReplicateMessage:
		return &ReplicateMsg{ReplicateMessage: m}, nil
	case *NackMessage:
		return &NackMsg{NackMessage: m}, nil
	// Unknown type, return error
	default:
		return nil, fmt.Errorf("unknown proto message type: %T", pm)
	}
}
