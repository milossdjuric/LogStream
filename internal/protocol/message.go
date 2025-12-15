// Used for defining and handling protocol messages between clients and servers.
// Translates go structs to byte streams and vice versa.

package protocol

import (
	"encoding/binary"
	"fmt" // formatting of strings and errors
	"io"
	"net" //network connections
)

// Message types
const (
	TypeProduce   = "PRODUCE"
	TypeConsume   = "CONSUME"
	TypeReplicate = "REPLICATE"
	TypeError     = "ERROR"
	TypeData      = "DATA"
	TypeEnd       = "END"
)

type Message struct {
	Type      string
	Topic     string
	Offset    int64
	Data      []byte
	Timestamp int64
}

func NewMessage(msgType string, data []byte) *Message {
	return &Message{
		Type:      msgType,
		Data:      data,
		Timestamp: 0,
	}
}

// Creates a PRODUCE message
func NewProduceMessage(topic string, data []byte) *Message {
	return &Message{
		Type:      TypeProduce,
		Topic:     topic,
		Data:      data,
		Timestamp: 0,
	}
}

// Creates a CONSUME message
func NewConsumeMessage(topic string, offset int64) *Message {
	return &Message{
		Type:      TypeConsume,
		Topic:     topic,
		Offset:    offset,
		Timestamp: 0,
	}
}

/* Formatting:
- 4 bytes: total size
- 1 byte: type length
- N bytes: type string
- 1 byte: topic length
- N bytes: topic string
- 8 bytes: offset
- 8 bytes: timestamp
- 4 bytes: data length
- N bytes: data */

func WriteMessage(conn net.Conn, msg *Message) error {
	typeBytes := []byte(msg.Type)
	topicBytes := []byte(msg.Topic)

	// Calculate total size
	totalSize := 1 + len(typeBytes) + 1 + len(topicBytes) + 8 + 8 + 4 + len(msg.Data) // typeLen + type + topicLen + topic + offset + timestamp + dataLen + data

	// Write size
	err := binary.Write(conn, binary.BigEndian, int32(totalSize))
	if err != nil {
		return fmt.Errorf("failed to write size: %w", err)
	}

	// Write type
	_, err = conn.Write([]byte{byte(len(typeBytes))}) 
	if err != nil {
		return fmt.Errorf("failed to write type length: %w", err)
	}
	_, err = conn.Write(typeBytes)
	if err != nil {
		return fmt.Errorf("failed to write type: %w", err)
	}

	// Write topic
	_, err = conn.Write([]byte{byte(len(topicBytes))})
	if err != nil {
		return fmt.Errorf("failed to write topic length: %w", err)
	}
	_, err = conn.Write(topicBytes)
	if err != nil {
		return fmt.Errorf("failed to write topic: %w", err)
	}

	// Write offset y timestamp
	err = binary.Write(conn, binary.BigEndian, msg.Offset)
	if err != nil {
		return fmt.Errorf("failed to write offset: %w", err)
	}
	err = binary.Write(conn, binary.BigEndian, msg.Timestamp)
	if err != nil {
		return fmt.Errorf("failed to write timestamp: %w", err)
	}

	// Write data
	err = binary.Write(conn, binary.BigEndian, int32(len(msg.Data)))
	if err != nil {
		return fmt.Errorf("failed to write data length: %w", err)
	}
	if len(msg.Data) > 0 {
		_, err = conn.Write(msg.Data) // we discard the int returned and just check the error
		if err != nil {
			return fmt.Errorf("failed to write data: %w", err)
		}
	}

	return nil
}

func ReadMessage(conn net.Conn) (*Message, error) {
	// Read total size
	var totalSize int32
	err := binary.Read(conn, binary.BigEndian, &totalSize)
	if err != nil {
		return nil, fmt.Errorf("failed to read size: %w", err)
	}

	if totalSize < 0 || totalSize > 10*1024*1024 { // Max 10MB - for safety (e.g DOS protection)
		return nil, fmt.Errorf("invalid message size: %d", totalSize)
	}

	// Read type
	var typeLen byte
	err = binary.Read(conn, binary.BigEndian, &typeLen)
	if err != nil {
		return nil, fmt.Errorf("failed to read type length: %w", err)
	}
	typeBytes := make([]byte, typeLen)
	_, err = io.ReadFull(conn, typeBytes) // readfull reads exactly len(data) bytes
	if err != nil {
		return nil, fmt.Errorf("failed to read type: %w", err)
	}

	// Read topic
	var topicLen byte
	err = binary.Read(conn, binary.BigEndian, &topicLen)
	if err != nil {
		return nil, fmt.Errorf("failed to read topic length: %w", err)
	}
	topicBytes := make([]byte, topicLen)
	_, err = io.ReadFull(conn, topicBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to read topic: %w", err)
	}

	// Read offset y timestamp
	var offset, timestamp int64
	err = binary.Read(conn, binary.BigEndian, &offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read offset: %w", err)
	}
	err = binary.Read(conn, binary.BigEndian, &timestamp)
	if err != nil {
		return nil, fmt.Errorf("failed to read timestamp: %w", err)
	}

	// Read data
	var dataLen int32
	err = binary.Read(conn, binary.BigEndian, &dataLen) // reads from conn
	if err != nil {
		return nil, fmt.Errorf("failed to read data length: %w", err)
	}
	if dataLen < 0 {
		return nil, fmt.Errorf("invalid data length: %d", dataLen)
	}

	var data []byte
	if dataLen > 0 {
		data = make([]byte, dataLen)
		_, err = io.ReadFull(conn, data)
		if err != nil {
			return nil, fmt.Errorf("failed to read data: %w", err)
		}
	}

	return &Message{
		Type:      string(typeBytes),
		Topic:     string(topicBytes),
		Offset:    offset,
		Data:      data,
		Timestamp: timestamp,
	}, nil
}
