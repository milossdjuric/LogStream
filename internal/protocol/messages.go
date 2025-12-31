package protocol

import "google.golang.org/protobuf/proto"

type Message interface {
	GetHeader() *MessageHeader
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
}

// TCP unicast from Producer to Leader
type ProduceMsg struct {
	*ProduceMessage
}

func (m *ProduceMsg) GetHeader() *MessageHeader { return m.Header }
func (m *ProduceMsg) Marshal() ([]byte, error)  { return proto.Marshal(m.ProduceMessage) }
func (m *ProduceMsg) Unmarshal(data []byte) error {
	return proto.Unmarshal(data, m.ProduceMessage)
}

// UDP unicast from Producer to Broker
type DataMsg struct {
	*DataMessage
}

func (m *DataMsg) GetHeader() *MessageHeader { return m.Header }
func (m *DataMsg) Marshal() ([]byte, error)  { return proto.Marshal(m.DataMessage) }
func (m *DataMsg) Unmarshal(data []byte) error {
	return proto.Unmarshal(data, m.DataMessage)
}

// TCP unicast from Consumer to Leader
type ConsumeMsg struct {
	*ConsumeMessage
}

func (m *ConsumeMsg) GetHeader() *MessageHeader { return m.Header }
func (m *ConsumeMsg) Marshal() ([]byte, error)  { return proto.Marshal(m.ConsumeMessage) }
func (m *ConsumeMsg) Unmarshal(data []byte) error {
	return proto.Unmarshal(data, m.ConsumeMessage)
}

// TCP unicast from Broker to Consumer
type ResultMsg struct {
	*ResultMessage
}

func (m *ResultMsg) GetHeader() *MessageHeader { return m.Header }
func (m *ResultMsg) Marshal() ([]byte, error)  { return proto.Marshal(m.ResultMessage) }
func (m *ResultMsg) Unmarshal(data []byte) error {
	return proto.Unmarshal(data, m.ResultMessage)
}

// TCP unicast for Leader to Producer/Consumer
// TCP unicast for Broker to Leader
// UDP multicast for Leader to Brokers
type HeartbeatMsg struct {
	*HeartbeatMessage
}

func (m *HeartbeatMsg) GetHeader() *MessageHeader { return m.Header }
func (m *HeartbeatMsg) Marshal() ([]byte, error)  { return proto.Marshal(m.HeartbeatMessage) }
func (m *HeartbeatMsg) Unmarshal(data []byte) error {
	return proto.Unmarshal(data, m.HeartbeatMessage)
}

// UDP broadcast from new node to discover cluster
type JoinMsg struct {
	*JoinMessage
}

func (m *JoinMsg) GetHeader() *MessageHeader { return m.Header }
func (m *JoinMsg) Marshal() ([]byte, error)  { return proto.Marshal(m.JoinMessage) }
func (m *JoinMsg) Unmarshal(data []byte) error {
	return proto.Unmarshal(data, m.JoinMessage)
}

// UDP unicast from Leader to new node
type JoinResponseMsg struct {
	*JoinResponseMessage
}

func (m *JoinResponseMsg) GetHeader() *MessageHeader { return m.Header }
func (m *JoinResponseMsg) Marshal() ([]byte, error)  { return proto.Marshal(m.JoinResponseMessage) }
func (m *JoinResponseMsg) Unmarshal(data []byte) error {
	return proto.Unmarshal(data, m.JoinResponseMessage)
}

// TCP unicast from Broker to next Broker in logical ring
type ElectionMsg struct {
	*ElectionMessage
}

func (m *ElectionMsg) GetHeader() *MessageHeader { return m.Header }
func (m *ElectionMsg) Marshal() ([]byte, error)  { return proto.Marshal(m.ElectionMessage) }
func (m *ElectionMsg) Unmarshal(data []byte) error {
	return proto.Unmarshal(data, m.ElectionMessage)
}

// UDP multicast from Leader to Brokers, FIFO ordering
type ReplicateMsg struct {
	*ReplicateMessage
}

func (m *ReplicateMsg) GetHeader() *MessageHeader { return m.Header }
func (m *ReplicateMsg) Marshal() ([]byte, error)  { return proto.Marshal(m.ReplicateMessage) }
func (m *ReplicateMsg) Unmarshal(data []byte) error {
	return proto.Unmarshal(data, m.ReplicateMessage)
}

// TCP unicast from Broker to specific sender
// UDP multicast from Broker to all (if sender unknown)
type NackMsg struct {
	*NackMessage
}

func (m *NackMsg) GetHeader() *MessageHeader { return m.Header }
func (m *NackMsg) Marshal() ([]byte, error)  { return proto.Marshal(m.NackMessage) }
func (m *NackMsg) Unmarshal(data []byte) error {
	return proto.Unmarshal(data, m.NackMessage)
}

func GetMessageType(msg Message) MessageType {
	return msg.GetHeader().Type
}

func GetSenderID(msg Message) string {
	return msg.GetHeader().SenderId
}

func GetSequenceNum(msg Message) int64 {
	return msg.GetHeader().SequenceNum
}

func GetSenderType(msg Message) NodeType {
	return msg.GetHeader().SenderType
}

func GetTimestamp(msg Message) int64 {
	return msg.GetHeader().Timestamp
}
