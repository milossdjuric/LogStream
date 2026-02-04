package unit

import (
	"testing"
	"time"

	"github.com/milossdjuric/logstream/internal/protocol"
)

func TestProtocol_ProduceMessage(t *testing.T) {
	msg := protocol.NewProduceMsg("producer-123", "test-topic", "192.168.1.10:9000", 42)

	// Verify header
	header := msg.GetHeader()
	if header.Type != protocol.MessageType_PRODUCE {
		t.Errorf("Expected PRODUCE type, got %v", header.Type)
	}
	if header.SenderId != "producer-123" {
		t.Errorf("Expected sender 'producer-123', got '%s'", header.SenderId)
	}
	if header.SequenceNum != 42 {
		t.Errorf("Expected sequence 42, got %d", header.SequenceNum)
	}
	if header.SenderType != protocol.NodeType_PRODUCER {
		t.Errorf("Expected PRODUCER type, got %v", header.SenderType)
	}

	// Verify message fields
	if msg.Topic != "test-topic" {
		t.Errorf("Expected topic 'test-topic', got '%s'", msg.Topic)
	}
	if msg.ProducerAddress != "192.168.1.10:9000" {
		t.Errorf("Expected address '192.168.1.10:9000', got '%s'", msg.ProducerAddress)
	}

	// Test serialization roundtrip
	data, err := msg.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	msg2 := &protocol.ProduceMsg{ProduceMessage: &protocol.ProduceMessage{}}
	err = msg2.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if msg2.Topic != msg.Topic {
		t.Errorf("Topic mismatch after roundtrip: '%s' != '%s'", msg2.Topic, msg.Topic)
	}
}

func TestProtocol_DataMessage(t *testing.T) {
	payload := []byte("test data payload")
	msg := protocol.NewDataMsg("producer-123", "test-topic", payload, 99)

	// Verify header
	header := msg.GetHeader()
	if header.Type != protocol.MessageType_DATA {
		t.Errorf("Expected DATA type, got %v", header.Type)
	}
	if header.SequenceNum != 99 {
		t.Errorf("Expected sequence 99, got %d", header.SequenceNum)
	}

	// Verify data
	if string(msg.Data) != "test data payload" {
		t.Errorf("Expected 'test data payload', got '%s'", msg.Data)
	}

	// Test serialization roundtrip
	data, err := msg.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	msg2 := &protocol.DataMsg{DataMessage: &protocol.DataMessage{}}
	err = msg2.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if string(msg2.Data) != string(msg.Data) {
		t.Errorf("Data mismatch after roundtrip")
	}
}

func TestProtocol_ConsumeMessage(t *testing.T) {
	msg := protocol.NewConsumeMsg("consumer-456", "test-topic", "192.168.1.20:9001", 10)

	// Verify header
	header := msg.GetHeader()
	if header.Type != protocol.MessageType_CONSUME {
		t.Errorf("Expected CONSUME type, got %v", header.Type)
	}
	if header.SenderType != protocol.NodeType_CONSUMER {
		t.Errorf("Expected CONSUMER type, got %v", header.SenderType)
	}

	// Verify fields
	if msg.Topic != "test-topic" {
		t.Errorf("Expected topic 'test-topic', got '%s'", msg.Topic)
	}

	// Test serialization
	data, err := msg.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("Marshaled data should not be empty")
	}
}

func TestProtocol_HeartbeatMessage(t *testing.T) {
	msg := protocol.NewHeartbeatMsg("broker-789", protocol.NodeType_BROKER, 100, 1)

	// Verify header
	header := msg.GetHeader()
	if header.Type != protocol.MessageType_HEARTBEAT {
		t.Errorf("Expected HEARTBEAT type, got %v", header.Type)
	}
	if header.SenderType != protocol.NodeType_BROKER {
		t.Errorf("Expected BROKER type, got %v", header.SenderType)
	}
	if header.SequenceNum != 100 {
		t.Errorf("Expected sequence 100, got %d", header.SequenceNum)
	}

	// Test serialization roundtrip
	data, err := msg.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	msg2 := &protocol.HeartbeatMsg{HeartbeatMessage: &protocol.HeartbeatMessage{}}
	err = msg2.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if msg2.GetHeader().SenderId != msg.GetHeader().SenderId {
		t.Error("SenderId mismatch after roundtrip")
	}
}

func TestProtocol_ElectionMessage(t *testing.T) {
	// NewElectionMsg(senderID, candidateID string, electionID int64, phase ElectionMessage_Phase, ringParticipants []string)
	ringParticipants := []string{"node-1", "node-2", "node-3"}
	msg := protocol.NewElectionMsg("sender-111", "candidate-111", 12345, protocol.ElectionMessage_ANNOUNCE, ringParticipants)

	// Verify header
	header := msg.GetHeader()
	if header.Type != protocol.MessageType_ELECTION {
		t.Errorf("Expected ELECTION type, got %v", header.Type)
	}

	// Verify election fields
	if msg.CandidateId != "candidate-111" {
		t.Errorf("Expected candidate 'candidate-111', got '%s'", msg.CandidateId)
	}
	if msg.ElectionId != 12345 {
		t.Errorf("Expected election ID 12345, got %d", msg.ElectionId)
	}
	if msg.Phase != protocol.ElectionMessage_ANNOUNCE {
		t.Errorf("Expected ANNOUNCE phase, got %v", msg.Phase)
	}

	// Verify ring participants
	if len(msg.RingParticipants) != 3 {
		t.Errorf("Expected 3 ring participants, got %d", len(msg.RingParticipants))
	}

	// Test VICTORY phase
	msg2 := protocol.NewElectionMsg("sender-222", "winner-222", 12345, protocol.ElectionMessage_VICTORY, ringParticipants)
	if msg2.Phase != protocol.ElectionMessage_VICTORY {
		t.Errorf("Expected VICTORY phase, got %v", msg2.Phase)
	}
}

func TestProtocol_ReplicateMessage(t *testing.T) {
	stateData := []byte("serialized cluster state")
	// NewReplicateMsg(senderID string, stateSnapshot []byte, updateType string, seqNum int64, viewNumber int64, leaderID string)
	msg := protocol.NewReplicateMsg("leader-333", stateData, "REGISTRY_UPDATE", 50, 5, "leader-333")

	// Verify header
	header := msg.GetHeader()
	if header.Type != protocol.MessageType_REPLICATE {
		t.Errorf("Expected REPLICATE type, got %v", header.Type)
	}
	if header.SequenceNum != 50 {
		t.Errorf("Expected sequence 50, got %d", header.SequenceNum)
	}

	// Verify replicate fields
	if string(msg.StateSnapshot) != "serialized cluster state" {
		t.Errorf("State snapshot mismatch")
	}
	if msg.UpdateType != "REGISTRY_UPDATE" {
		t.Errorf("Expected update type 'REGISTRY_UPDATE', got '%s'", msg.UpdateType)
	}
	if msg.ViewNumber != 5 {
		t.Errorf("Expected view number 5, got %d", msg.ViewNumber)
	}
	if msg.LeaderId != "leader-333" {
		t.Errorf("Expected leader ID 'leader-333', got '%s'", msg.LeaderId)
	}

	// Test serialization roundtrip
	data, err := msg.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	msg2 := &protocol.ReplicateMsg{ReplicateMessage: &protocol.ReplicateMessage{}}
	err = msg2.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if string(msg2.StateSnapshot) != string(msg.StateSnapshot) {
		t.Error("StateSnapshot mismatch after roundtrip")
	}
}

func TestProtocol_SubscribeMessage(t *testing.T) {
	msg := protocol.NewSubscribeMsg("consumer-444", "test-topic", "consumer-444", "192.168.1.30:9002", true)

	// Verify header
	header := msg.GetHeader()
	if header.Type != protocol.MessageType_SUBSCRIBE {
		t.Errorf("Expected SUBSCRIBE type, got %v", header.Type)
	}

	// Verify subscribe fields
	if msg.Topic != "test-topic" {
		t.Errorf("Expected topic 'test-topic', got '%s'", msg.Topic)
	}
	if msg.ConsumerId != "consumer-444" {
		t.Errorf("Expected consumer ID 'consumer-444', got '%s'", msg.ConsumerId)
	}
	if !msg.EnableProcessing {
		t.Error("Expected EnableProcessing to be true")
	}

	// Test with processing disabled
	msg2 := protocol.NewSubscribeMsg("consumer-555", "topic2", "consumer-555", "192.168.1.31:9002", false)
	if msg2.EnableProcessing {
		t.Error("Expected EnableProcessing to be false")
	}
}

func TestProtocol_ResultMessage(t *testing.T) {
	resultData := []byte("processed result data")
	msg := protocol.NewResultMsg("broker-666", "test-topic", resultData, 42, 100)

	// Verify header
	header := msg.GetHeader()
	if header.Type != protocol.MessageType_RESULT {
		t.Errorf("Expected RESULT type, got %v", header.Type)
	}

	// Verify result fields
	if string(msg.Data) != "processed result data" {
		t.Errorf("Data mismatch")
	}
	if msg.Topic != "test-topic" {
		t.Errorf("Expected topic 'test-topic', got '%s'", msg.Topic)
	}
	if msg.Offset != 42 {
		t.Errorf("Expected offset 42, got %d", msg.Offset)
	}
}

func TestProtocol_StateExchangeMessage(t *testing.T) {
	msg := protocol.NewStateExchangeMsg("new-leader-777", 98765, 10)

	// Verify header
	header := msg.GetHeader()
	if header.Type != protocol.MessageType_STATE_EXCHANGE {
		t.Errorf("Expected STATE_EXCHANGE type, got %v", header.Type)
	}

	// Verify fields
	if msg.ElectionId != 98765 {
		t.Errorf("Expected election ID 98765, got %d", msg.ElectionId)
	}
	if msg.ViewNumber != 10 {
		t.Errorf("Expected view number 10, got %d", msg.ViewNumber)
	}
}

func TestProtocol_ViewInstallMessage(t *testing.T) {
	memberIDs := []string{"broker-1", "broker-2", "broker-3"}
	memberAddrs := []string{"192.168.1.1:8001", "192.168.1.2:8001", "192.168.1.3:8001"}
	stateData := []byte("agreed state")
	agreedLogOffsets := map[string]uint64{"topic1": 100, "topic2": 200}

	// View-synchronous: merged logs for union-based merge
	mergedLogs := []*protocol.TopicLogEntries{
		{
			Topic: "topic1",
			Entries: []*protocol.LogEntry{
				{Offset: 1, Data: []byte("data1"), Timestamp: 1000},
				{Offset: 2, Data: []byte("data2"), Timestamp: 2000},
			},
		},
	}

	// NewViewInstallMsg(senderID string, viewNumber, agreedSeq int64, stateSnapshot []byte, memberIDs, memberAddresses []string, agreedLogOffsets map[string]uint64, mergedLogs []*TopicLogEntries)
	msg := protocol.NewViewInstallMsg("leader-888", 15, 1000, stateData, memberIDs, memberAddrs, agreedLogOffsets, mergedLogs)

	// Verify header
	header := msg.GetHeader()
	if header.Type != protocol.MessageType_VIEW_INSTALL {
		t.Errorf("Expected VIEW_INSTALL type, got %v", header.Type)
	}

	// Verify fields
	if msg.ViewNumber != 15 {
		t.Errorf("Expected view number 15, got %d", msg.ViewNumber)
	}
	if msg.AgreedSeq != 1000 {
		t.Errorf("Expected agreed seq 1000, got %d", msg.AgreedSeq)
	}
	if len(msg.MemberIds) != 3 {
		t.Errorf("Expected 3 members, got %d", len(msg.MemberIds))
	}
	if len(msg.MemberAddresses) != 3 {
		t.Errorf("Expected 3 addresses, got %d", len(msg.MemberAddresses))
	}
	if len(msg.AgreedLogOffsets) != 2 {
		t.Errorf("Expected 2 log offsets, got %d", len(msg.AgreedLogOffsets))
	}
}

func TestProtocol_GetMessageHelpers(t *testing.T) {
	msg := protocol.NewDataMsg("sender-999", "topic", []byte("data"), 123)

	// Test GetMessageType
	msgType := protocol.GetMessageType(msg)
	if msgType != protocol.MessageType_DATA {
		t.Errorf("Expected DATA type, got %v", msgType)
	}

	// Test GetSenderID
	senderID := protocol.GetSenderID(msg)
	if senderID != "sender-999" {
		t.Errorf("Expected sender 'sender-999', got '%s'", senderID)
	}

	// Test GetSequenceNum
	seqNum := protocol.GetSequenceNum(msg)
	if seqNum != 123 {
		t.Errorf("Expected sequence 123, got %d", seqNum)
	}

	// Test GetSenderType
	senderType := protocol.GetSenderType(msg)
	if senderType != protocol.NodeType_PRODUCER {
		t.Errorf("Expected PRODUCER type, got %v", senderType)
	}

	// Test GetTimestamp (should be non-zero)
	timestamp := protocol.GetTimestamp(msg)
	if timestamp == 0 {
		t.Error("Timestamp should be non-zero")
	}
}

func TestProtocol_TimestampIsRecent(t *testing.T) {
	before := time.Now().UnixNano()
	msg := protocol.NewHeartbeatMsg("test", protocol.NodeType_BROKER, 0, 0)
	after := time.Now().UnixNano()

	timestamp := protocol.GetTimestamp(msg)
	if timestamp < before || timestamp > after {
		t.Errorf("Timestamp %d should be between %d and %d", timestamp, before, after)
	}
}

func BenchmarkProtocol_MarshalData(b *testing.B) {
	data := make([]byte, 1024)
	msg := protocol.NewDataMsg("producer", "topic", data, 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg.Marshal()
	}
}

func BenchmarkProtocol_UnmarshalData(b *testing.B) {
	data := make([]byte, 1024)
	msg := protocol.NewDataMsg("producer", "topic", data, 0)
	serialized, _ := msg.Marshal()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg2 := &protocol.DataMsg{DataMessage: &protocol.DataMessage{}}
		msg2.Unmarshal(serialized)
	}
}
