package unit

import (
	"testing"
	"time"

	"github.com/milossdjuric/logstream/internal/state"
)

func TestClusterState_RegisterBroker(t *testing.T) {
	cs := state.NewClusterState()

	cs.RegisterBroker("broker1", "192.168.1.10:8001", false)
	cs.RegisterBroker("broker2", "192.168.1.20:8001", true)

	if cs.GetBrokerCount() != 2 {
		t.Errorf("Expected 2 brokers, got %d", cs.GetBrokerCount())
	}

	broker, ok := cs.GetBroker("broker1")
	if !ok {
		t.Fatal("Broker1 should exist")
	}
	if broker.IsLeader {
		t.Error("Broker1 should not be leader")
	}

	broker2, ok := cs.GetBroker("broker2")
	if !ok {
		t.Fatal("Broker2 should exist")
	}
	if !broker2.IsLeader {
		t.Error("Broker2 should be leader")
	}
}

func TestClusterState_RegisterProducer(t *testing.T) {
	cs := state.NewClusterState()

	cs.RegisterProducer("producer1", "192.168.1.30:9001", "topic1")

	if cs.CountProducers() != 1 {
		t.Errorf("Expected 1 producer, got %d", cs.CountProducers())
	}

	producer, ok := cs.GetProducer("producer1")
	if !ok {
		t.Fatal("Producer should exist")
	}
	if producer.Topic != "topic1" {
		t.Errorf("Expected topic1, got %s", producer.Topic)
	}
}

func TestClusterState_RegisterConsumer(t *testing.T) {
	cs := state.NewClusterState()

	cs.RegisterConsumer("consumer1", "192.168.1.40:9002")

	if cs.CountConsumers() != 1 {
		t.Errorf("Expected 1 consumer, got %d", cs.CountConsumers())
	}

	consumer, ok := cs.GetConsumer("consumer1")
	if !ok {
		t.Fatal("Consumer should exist")
	}
	if len(consumer.Topics) != 0 {
		t.Errorf("Expected 0 topics initially, got %d", len(consumer.Topics))
	}
}

func TestClusterState_BrokerTimeout(t *testing.T) {
	cs := state.NewClusterState()

	cs.RegisterBroker("broker1", "192.168.1.10:8001", false)
	time.Sleep(100 * time.Millisecond)

	removed := cs.CheckBrokerTimeouts(50 * time.Millisecond)
	if len(removed) != 1 {
		t.Errorf("Expected 1 removed broker, got %d", len(removed))
	}

	if cs.GetBrokerCount() != 0 {
		t.Errorf("Expected 0 brokers after timeout, got %d", cs.GetBrokerCount())
	}
}

func TestClusterState_Serialization(t *testing.T) {
	cs := state.NewClusterState()

	cs.RegisterBroker("broker1", "192.168.1.10:8001", true)
	cs.RegisterProducer("producer1", "192.168.1.30:9001", "topic1")
	cs.RegisterConsumer("consumer1", "192.168.1.40:9002")

	data, err := cs.Serialize()
	if err != nil {
		t.Fatalf("Serialization failed: %v", err)
	}

	cs2 := state.NewClusterState()
	if err := cs2.Deserialize(data); err != nil {
		t.Fatalf("Deserialization failed: %v", err)
	}

	if cs2.GetBrokerCount() != 1 {
		t.Errorf("Expected 1 broker after deserialization, got %d", cs2.GetBrokerCount())
	}

	if cs2.CountProducers() != 1 {
		t.Errorf("Expected 1 producer after deserialization, got %d", cs2.CountProducers())
	}

	if cs2.CountConsumers() != 1 {
		t.Errorf("Expected 1 consumer after deserialization, got %d", cs2.CountConsumers())
	}
}

func TestClusterState_StreamAssignment(t *testing.T) {
	cs := state.NewClusterState()

	// Test AssignStream
	err := cs.AssignStream("topic1", "producer1", "broker1", "192.168.1.10:8001")
	if err != nil {
		t.Fatalf("AssignStream failed: %v", err)
	}

	// Test GetStreamAssignment
	stream, exists := cs.GetStreamAssignment("topic1")
	if !exists {
		t.Fatal("Stream assignment should exist")
	}
	if stream.Topic != "topic1" {
		t.Errorf("Expected topic1, got %s", stream.Topic)
	}
	if stream.ProducerId != "producer1" {
		t.Errorf("Expected producer1, got %s", stream.ProducerId)
	}
	if stream.AssignedBrokerId != "broker1" {
		t.Errorf("Expected broker1, got %s", stream.AssignedBrokerId)
	}
	if stream.BrokerAddress != "192.168.1.10:8001" {
		t.Errorf("Expected 192.168.1.10:8001, got %s", stream.BrokerAddress)
	}
	if stream.ConsumerId != "" {
		t.Errorf("Expected empty consumer ID initially, got %s", stream.ConsumerId)
	}

	// Test duplicate assignment fails
	err = cs.AssignStream("topic1", "producer2", "broker2", "192.168.1.20:8001")
	if err == nil {
		t.Error("Expected error for duplicate stream assignment")
	}

	// Test non-existent stream
	_, exists = cs.GetStreamAssignment("nonexistent")
	if exists {
		t.Error("Non-existent stream should not exist")
	}
}

func TestClusterState_AssignConsumerToStream(t *testing.T) {
	cs := state.NewClusterState()

	// Setup: create a stream first
	err := cs.AssignStream("topic1", "producer1", "broker1", "192.168.1.10:8001")
	if err != nil {
		t.Fatalf("AssignStream failed: %v", err)
	}

	// Test assigning consumer to stream
	err = cs.AssignConsumerToStream("consumer1", "topic1")
	if err != nil {
		t.Fatalf("AssignConsumerToStream failed: %v", err)
	}

	// Verify consumer was assigned
	stream, exists := cs.GetStreamAssignment("topic1")
	if !exists {
		t.Fatal("Stream should exist")
	}
	if stream.ConsumerId != "consumer1" {
		t.Errorf("Expected consumer1, got %s", stream.ConsumerId)
	}

	// Test assigning second consumer fails (one-to-one mapping)
	err = cs.AssignConsumerToStream("consumer2", "topic1")
	if err == nil {
		t.Error("Expected error for duplicate consumer assignment")
	}

	// Test assigning to non-existent stream
	err = cs.AssignConsumerToStream("consumer1", "nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent stream")
	}
}

func TestClusterState_HasProducerForTopic(t *testing.T) {
	cs := state.NewClusterState()

	// Initially no producer
	if cs.HasProducerForTopic("topic1") {
		t.Error("Should not have producer for topic1 initially")
	}

	// Assign stream
	cs.AssignStream("topic1", "producer1", "broker1", "192.168.1.10:8001")

	// Now should have producer
	if !cs.HasProducerForTopic("topic1") {
		t.Error("Should have producer for topic1 after assignment")
	}
}

func TestClusterState_HasConsumerForTopic(t *testing.T) {
	cs := state.NewClusterState()

	// Setup stream
	cs.AssignStream("topic1", "producer1", "broker1", "192.168.1.10:8001")

	// Initially no consumer
	if cs.HasConsumerForTopic("topic1") {
		t.Error("Should not have consumer for topic1 initially")
	}

	// Assign consumer
	cs.AssignConsumerToStream("consumer1", "topic1")

	// Now should have consumer
	if !cs.HasConsumerForTopic("topic1") {
		t.Error("Should have consumer for topic1 after assignment")
	}
}

func TestClusterState_GetStreamsByBroker(t *testing.T) {
	cs := state.NewClusterState()

	// Create multiple streams on same broker
	cs.AssignStream("topic1", "producer1", "broker1", "192.168.1.10:8001")
	cs.AssignStream("topic2", "producer2", "broker1", "192.168.1.10:8001")
	cs.AssignStream("topic3", "producer3", "broker2", "192.168.1.20:8001")

	// Get streams for broker1
	streams := cs.GetStreamsByBroker("broker1")
	if len(streams) != 2 {
		t.Errorf("Expected 2 streams for broker1, got %d", len(streams))
	}

	// Get streams for broker2
	streams = cs.GetStreamsByBroker("broker2")
	if len(streams) != 1 {
		t.Errorf("Expected 1 stream for broker2, got %d", len(streams))
	}

	// Get streams for non-existent broker
	streams = cs.GetStreamsByBroker("broker999")
	if len(streams) != 0 {
		t.Errorf("Expected 0 streams for non-existent broker, got %d", len(streams))
	}
}

func TestClusterState_ReassignStreamBroker(t *testing.T) {
	cs := state.NewClusterState()

	// Setup stream
	cs.AssignStream("topic1", "producer1", "broker1", "192.168.1.10:8001")

	// Reassign to different broker
	err := cs.ReassignStreamBroker("topic1", "broker2", "192.168.1.20:8001")
	if err != nil {
		t.Fatalf("ReassignStreamBroker failed: %v", err)
	}

	// Verify reassignment
	stream, exists := cs.GetStreamAssignment("topic1")
	if !exists {
		t.Fatal("Stream should exist")
	}
	if stream.AssignedBrokerId != "broker2" {
		t.Errorf("Expected broker2, got %s", stream.AssignedBrokerId)
	}
	if stream.BrokerAddress != "192.168.1.20:8001" {
		t.Errorf("Expected 192.168.1.20:8001, got %s", stream.BrokerAddress)
	}

	// Test reassigning non-existent stream
	err = cs.ReassignStreamBroker("nonexistent", "broker3", "192.168.1.30:8001")
	if err == nil {
		t.Error("Expected error for non-existent stream")
	}
}

func TestClusterState_RemoveStream(t *testing.T) {
	cs := state.NewClusterState()

	// Setup stream
	cs.AssignStream("topic1", "producer1", "broker1", "192.168.1.10:8001")

	// Remove stream
	err := cs.RemoveStream("topic1")
	if err != nil {
		t.Fatalf("RemoveStream failed: %v", err)
	}

	// Verify removal
	_, exists := cs.GetStreamAssignment("topic1")
	if exists {
		t.Error("Stream should not exist after removal")
	}

	// Test removing non-existent stream
	err = cs.RemoveStream("nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent stream")
	}
}

func TestClusterState_RemoveConsumerFromStream(t *testing.T) {
	cs := state.NewClusterState()

	// Setup stream with consumer
	cs.AssignStream("topic1", "producer1", "broker1", "192.168.1.10:8001")
	cs.AssignConsumerToStream("consumer1", "topic1")

	// Remove consumer from stream
	err := cs.RemoveConsumerFromStream("topic1", "consumer1")
	if err != nil {
		t.Fatalf("RemoveConsumerFromStream failed: %v", err)
	}

	// Verify consumer removed but stream still exists
	stream, exists := cs.GetStreamAssignment("topic1")
	if !exists {
		t.Fatal("Stream should still exist")
	}
	if stream.ConsumerId != "" {
		t.Errorf("Expected empty consumer ID, got %s", stream.ConsumerId)
	}

	// Test removing wrong consumer
	cs.AssignConsumerToStream("consumer2", "topic1")
	err = cs.RemoveConsumerFromStream("topic1", "wrongconsumer")
	if err == nil {
		t.Error("Expected error for wrong consumer ID")
	}
}

func TestClusterState_ConsumerSubscription(t *testing.T) {
	cs := state.NewClusterState()

	// Register consumer
	cs.RegisterConsumer("consumer1", "192.168.1.40:9002")

	// Subscribe to topic
	err := cs.SubscribeConsumer("consumer1", "topic1")
	if err != nil {
		t.Fatalf("SubscribeConsumer failed: %v", err)
	}

	// Verify subscription
	consumer, ok := cs.GetConsumer("consumer1")
	if !ok {
		t.Fatal("Consumer should exist")
	}
	if len(consumer.Topics) != 1 || consumer.Topics[0] != "topic1" {
		t.Errorf("Expected [topic1], got %v", consumer.Topics)
	}

	// Subscribe to another topic
	cs.SubscribeConsumer("consumer1", "topic2")
	consumer, _ = cs.GetConsumer("consumer1")
	if len(consumer.Topics) != 2 {
		t.Errorf("Expected 2 topics, got %d", len(consumer.Topics))
	}

	// Subscribe to same topic again (should be idempotent)
	cs.SubscribeConsumer("consumer1", "topic1")
	consumer, _ = cs.GetConsumer("consumer1")
	if len(consumer.Topics) != 2 {
		t.Errorf("Expected 2 topics (no duplicate), got %d", len(consumer.Topics))
	}
}

func TestClusterState_UnsubscribeConsumer(t *testing.T) {
	cs := state.NewClusterState()

	// Setup consumer with subscriptions
	cs.RegisterConsumer("consumer1", "192.168.1.40:9002")
	cs.SubscribeConsumer("consumer1", "topic1")
	cs.SubscribeConsumer("consumer1", "topic2")

	// Unsubscribe from one topic
	err := cs.UnsubscribeConsumer("consumer1", "topic1")
	if err != nil {
		t.Fatalf("UnsubscribeConsumer failed: %v", err)
	}

	// Verify unsubscription
	consumer, _ := cs.GetConsumer("consumer1")
	if len(consumer.Topics) != 1 || consumer.Topics[0] != "topic2" {
		t.Errorf("Expected [topic2], got %v", consumer.Topics)
	}
}

func TestClusterState_GetConsumerSubscribers(t *testing.T) {
	cs := state.NewClusterState()

	// Setup consumers with subscriptions
	cs.RegisterConsumer("consumer1", "192.168.1.40:9002")
	cs.RegisterConsumer("consumer2", "192.168.1.41:9002")
	cs.RegisterConsumer("consumer3", "192.168.1.42:9002")

	cs.SubscribeConsumer("consumer1", "topic1")
	cs.SubscribeConsumer("consumer2", "topic1")
	cs.SubscribeConsumer("consumer3", "topic2")

	// Get subscribers for topic1
	subscribers := cs.GetConsumerSubscribers("topic1")
	if len(subscribers) != 2 {
		t.Errorf("Expected 2 subscribers for topic1, got %d", len(subscribers))
	}

	// Get subscribers for topic2
	subscribers = cs.GetConsumerSubscribers("topic2")
	if len(subscribers) != 1 {
		t.Errorf("Expected 1 subscriber for topic2, got %d", len(subscribers))
	}

	// Get subscribers for non-existent topic
	subscribers = cs.GetConsumerSubscribers("nonexistent")
	if len(subscribers) != 0 {
		t.Errorf("Expected 0 subscribers for non-existent topic, got %d", len(subscribers))
	}
}

func TestClusterState_SequenceNumberIncrement(t *testing.T) {
	cs := state.NewClusterState()

	initialSeq := cs.GetSequenceNum()
	if initialSeq != 0 {
		t.Errorf("Expected initial sequence 0, got %d", initialSeq)
	}

	// Each mutation should increment sequence
	cs.RegisterBroker("broker1", "192.168.1.10:8001", false)
	if cs.GetSequenceNum() != 1 {
		t.Errorf("Expected sequence 1 after RegisterBroker, got %d", cs.GetSequenceNum())
	}

	cs.RegisterProducer("producer1", "192.168.1.30:9001", "topic1")
	if cs.GetSequenceNum() != 2 {
		t.Errorf("Expected sequence 2 after RegisterProducer, got %d", cs.GetSequenceNum())
	}

	cs.RegisterConsumer("consumer1", "192.168.1.40:9002")
	if cs.GetSequenceNum() != 3 {
		t.Errorf("Expected sequence 3 after RegisterConsumer, got %d", cs.GetSequenceNum())
	}

	cs.AssignStream("topic1", "producer1", "broker1", "192.168.1.10:8001")
	if cs.GetSequenceNum() != 4 {
		t.Errorf("Expected sequence 4 after AssignStream, got %d", cs.GetSequenceNum())
	}
}

func TestClusterState_ProducerTimeout(t *testing.T) {
	cs := state.NewClusterState()

	cs.RegisterProducer("producer1", "192.168.1.30:9001", "topic1")
	time.Sleep(100 * time.Millisecond)

	removed := cs.CheckProducerTimeouts(50 * time.Millisecond)
	if len(removed) != 1 {
		t.Errorf("Expected 1 removed producer, got %d", len(removed))
	}

	if cs.CountProducers() != 0 {
		t.Errorf("Expected 0 producers after timeout, got %d", cs.CountProducers())
	}
}

func TestClusterState_ConsumerTimeout(t *testing.T) {
	cs := state.NewClusterState()

	cs.RegisterConsumer("consumer1", "192.168.1.40:9002")
	time.Sleep(100 * time.Millisecond)

	removed := cs.CheckConsumerTimeouts(50 * time.Millisecond)
	if len(removed) != 1 {
		t.Errorf("Expected 1 removed consumer, got %d", len(removed))
	}

	if cs.CountConsumers() != 0 {
		t.Errorf("Expected 0 consumers after timeout, got %d", cs.CountConsumers())
	}
}

func TestClusterState_HeartbeatUpdate(t *testing.T) {
	cs := state.NewClusterState()

	// Register and immediately update heartbeat
	cs.RegisterBroker("broker1", "192.168.1.10:8001", false)
	time.Sleep(50 * time.Millisecond)
	cs.UpdateBrokerHeartbeat("broker1")

	// Should not be removed with short timeout since we just updated
	time.Sleep(30 * time.Millisecond)
	removed := cs.CheckBrokerTimeouts(40 * time.Millisecond)
	if len(removed) != 0 {
		t.Errorf("Expected 0 removed brokers (heartbeat was updated), got %d", len(removed))
	}

	// Now wait longer and it should timeout
	time.Sleep(60 * time.Millisecond)
	removed = cs.CheckBrokerTimeouts(40 * time.Millisecond)
	if len(removed) != 1 {
		t.Errorf("Expected 1 removed broker after longer wait, got %d", len(removed))
	}
}

func BenchmarkClusterState_RegisterBroker(b *testing.B) {
	cs := state.NewClusterState()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cs.RegisterBroker("broker"+string(rune(i)), "192.168.1.10:8001", false)
	}
}

func BenchmarkClusterState_Serialize(b *testing.B) {
	cs := state.NewClusterState()
	for i := 0; i < 100; i++ {
		cs.RegisterBroker("broker"+string(rune(i)), "192.168.1.10:8001", false)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cs.Serialize()
	}
}
