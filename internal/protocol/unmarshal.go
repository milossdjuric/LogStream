package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"google.golang.org/protobuf/proto"
)

func ReadTCPMessage(conn net.Conn) (Message, error) {
	var size uint32
	if err := binary.Read(conn, binary.BigEndian, &size); err != nil {
		return nil, fmt.Errorf("failed to read size: %w", err)
	}

	msgBytes := make([]byte, size)
	if _, err := io.ReadFull(conn, msgBytes); err != nil {
		return nil, fmt.Errorf("failed to read message: %w", err)
	}

	return unmarshalMessage(msgBytes)
}

func ReadMessage(conn net.Conn) (Message, error) {
	return ReadTCPMessage(conn)
}

func ReadUDPMessage(conn *net.UDPConn) (Message, *net.UDPAddr, error) {
	buffer := make([]byte, 65536)
	n, addr, err := conn.ReadFromUDP(buffer)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read UDP message: %w", err)
	}

	if n < 2 || buffer[0] != 0x0a {
		return nil, addr, fmt.Errorf("not a protobuf message")
	}

	msg, err := unmarshalMessage(buffer[:n])
	if err != nil {
		return nil, addr, err
	}

	return msg, addr, err
}

// MessageFactory is a function that creates a new protobuf message instance
type MessageFactory func() proto.Message

var messageRegistry = map[MessageType]MessageFactory{
	MessageType_PRODUCE:                 func() proto.Message { return &ProduceMessage{} },
	MessageType_DATA:                    func() proto.Message { return &DataMessage{} },
	MessageType_CONSUME:                 func() proto.Message { return &ConsumeMessage{} },
	MessageType_RESULT:                  func() proto.Message { return &ResultMessage{} },
	MessageType_SUBSCRIBE:               func() proto.Message { return &SubscribeMessage{} },
	MessageType_HEARTBEAT:               func() proto.Message { return &HeartbeatMessage{} },
	MessageType_JOIN:                    func() proto.Message { return &JoinMessage{} },
	MessageType_JOIN_RESPONSE:           func() proto.Message { return &JoinResponseMessage{} },
	MessageType_ELECTION:                func() proto.Message { return &ElectionMessage{} },
	MessageType_REPLICATE:               func() proto.Message { return &ReplicateMessage{} },
	MessageType_REPLICATE_ACK:           func() proto.Message { return &ReplicateAckMessage{} },
	MessageType_NACK:                    func() proto.Message { return &NackMessage{} },
	MessageType_STATE_EXCHANGE:          func() proto.Message { return &StateExchangeMessage{} },
	MessageType_STATE_EXCHANGE_RESPONSE: func() proto.Message { return &StateExchangeResponseMessage{} },
	MessageType_VIEW_INSTALL:            func() proto.Message { return &ViewInstallMessage{} },
	MessageType_VIEW_INSTALL_ACK:        func() proto.Message { return &ViewInstallAckMessage{} },
}

func unmarshalMessage(data []byte) (Message, error) {
	peek := &HeartbeatMessage{}
	if err := proto.Unmarshal(data, peek); err != nil {
		return nil, fmt.Errorf("failed to unmarshal header: %w", err)
	}

	if peek.Header == nil {
		return nil, fmt.Errorf("message has no header")
	}

	factory, ok := messageRegistry[peek.Header.Type]
	if !ok {
		return nil, fmt.Errorf("unknown message type: %v", peek.Header.Type)
	}

	protoMsg := factory()
	if err := proto.Unmarshal(data, protoMsg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}

	return wrapProtoMessage(protoMsg)
}

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
	case *SubscribeMessage:
		return &SubscribeMsg{SubscribeMessage: m}, nil
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
	case *ReplicateAckMessage:
		return &ReplicateAckMsg{ReplicateAckMessage: m}, nil
	case *NackMessage:
		return &NackMsg{NackMessage: m}, nil
	case *StateExchangeMessage:
		return &StateExchangeMsg{StateExchangeMessage: m}, nil
	case *StateExchangeResponseMessage:
		return &StateExchangeResponseMsg{StateExchangeResponseMessage: m}, nil
	case *ViewInstallMessage:
		return &ViewInstallMsg{ViewInstallMessage: m}, nil
	case *ViewInstallAckMessage:
		return &ViewInstallAckMsg{ViewInstallAckMessage: m}, nil
	default:
		return nil, fmt.Errorf("unknown proto message type: %T", pm)
	}
}
