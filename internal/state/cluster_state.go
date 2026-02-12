package state

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/milossdjuric/logstream/internal/protocol"
	"google.golang.org/protobuf/proto"
)

func shortID(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}

// ClusterState manages all cluster state: brokers + producers + consumers
// This is what gets replicated to all nodes for view-synchronous recovery
type ClusterState struct {
	mu        sync.RWMutex
	brokers   map[string]*protocol.BrokerInfo       // key: broker ID
	producers map[string]*protocol.ProducerInfo     // key: producer ID
	consumers map[string]*protocol.ConsumerInfo     // key: consumer ID
	streams   map[string]*protocol.StreamAssignment // key: topic (one-to-one mapping)
	seqNum    int64                                 // Monotonic sequence for FIFO ordering (tracks ALL changes)
}

func NewClusterState() *ClusterState {
	return &ClusterState{
		brokers:   make(map[string]*protocol.BrokerInfo),
		producers: make(map[string]*protocol.ProducerInfo),
		consumers: make(map[string]*protocol.ConsumerInfo),
		streams:   make(map[string]*protocol.StreamAssignment),
		seqNum:    0,
	}
}

func (cs *ClusterState) RegisterBroker(id, address string, isLeader bool) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if address != "" && !strings.Contains(address, ":") {
		fmt.Printf("[ClusterState] WARNING: RegisterBroker called with address missing port: %s\n", address)
	}
	if strings.HasSuffix(address, ":0") {
		fmt.Printf("[ClusterState] WARNING: RegisterBroker called with port 0: %s\n", address)
	}

	cs.brokers[id] = &protocol.BrokerInfo{
		Id:            id,
		Address:       address,
		IsLeader:      isLeader,
		LastHeartbeat: time.Now().UnixNano(),
	}

	cs.seqNum++

	fmt.Printf("[ClusterState] Registered broker %s at %s (leader=%v, seq=%d)\n",
		shortID(id), address, isLeader, cs.seqNum)

	return nil
}

func (cs *ClusterState) RemoveBroker(id string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if _, exists := cs.brokers[id]; !exists {
		return fmt.Errorf("broker %s not found", id)
	}

	delete(cs.brokers, id)
	cs.seqNum++

	fmt.Printf("[ClusterState] Removed broker %s (seq=%d)\n", shortID(id), cs.seqNum)
	return nil
}

func (cs *ClusterState) CheckBrokerTimeouts(timeout time.Duration) []string {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	now := time.Now().UnixNano()
	removed := []string{}

	for id, broker := range cs.brokers {
		lastSeen := time.Duration(now - broker.LastHeartbeat)

		if lastSeen > timeout {
			delete(cs.brokers, id)
			cs.seqNum++
			removed = append(removed, id)

			fmt.Printf("[ClusterState] Timeout: Removed broker %s (last seen: %s, seq=%d)\n",
				shortID(id), lastSeen.Round(time.Second), cs.seqNum)
		}
	}

	return removed
}

func (cs *ClusterState) UpdateBrokerHeartbeat(brokerID string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if broker, ok := cs.brokers[brokerID]; ok {
		broker.LastHeartbeat = time.Now().UnixNano()
	}
}

func (cs *ClusterState) GetBroker(id string) (*protocol.BrokerInfo, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	broker, ok := cs.brokers[id]
	if !ok {
		return nil, false
	}

	if strings.HasSuffix(broker.Address, ":0") {
		fmt.Printf("[ClusterState] WARNING: GetBroker returning broker %s with port 0 address: %s\n",
			shortID(id), broker.Address)
	}

	return proto.Clone(broker).(*protocol.BrokerInfo), true
}

func (cs *ClusterState) GetBrokerCount() int {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return len(cs.brokers)
}

func (cs *ClusterState) ListBrokers() []string {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	brokerIDs := make([]string, 0, len(cs.brokers))
	for id := range cs.brokers {
		brokerIDs = append(brokerIDs, id)
	}
	return brokerIDs
}

func (cs *ClusterState) RegisterProducer(id, address, topic string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	cs.producers[id] = &protocol.ProducerInfo{
		Id:            id,
		Address:       address,
		Topic:         topic,
		LastHeartbeat: time.Now().UnixNano(),
	}

	cs.seqNum++

	fmt.Printf("[ClusterState] Registered producer %s (topic: %s, seq=%d)\n", shortID(id), topic, cs.seqNum)
	return nil
}

func (cs *ClusterState) RemoveProducer(id string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if _, exists := cs.producers[id]; !exists {
		return fmt.Errorf("producer %s not found", id)
	}

	delete(cs.producers, id)
	cs.seqNum++

	fmt.Printf("[ClusterState] Removed producer %s (seq=%d)\n", shortID(id), cs.seqNum)
	return nil
}

func (cs *ClusterState) UpdateProducerHeartbeat(id string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if producer, ok := cs.producers[id]; ok {
		producer.LastHeartbeat = time.Now().UnixNano()
	}
}

func (cs *ClusterState) CheckProducerTimeouts(timeout time.Duration) []string {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	now := time.Now().UnixNano()
	removed := []string{}

	for id, producer := range cs.producers {
		lastSeen := time.Duration(now - producer.LastHeartbeat)

		if lastSeen > timeout {
			topic := producer.Topic

			delete(cs.producers, id)
			cs.seqNum++
			removed = append(removed, id)

			fmt.Printf("[ClusterState] Timeout: Removed producer %s (last seen: %s, seq=%d)\n",
				shortID(id), lastSeen.Round(time.Second), cs.seqNum)

			if _, exists := cs.streams[topic]; exists {
				delete(cs.streams, topic)
				cs.seqNum++
				fmt.Printf("[ClusterState] Timeout: Removed stream for topic %s (producer timed out, seq=%d)\n",
					topic, cs.seqNum)
			}
		}
	}

	return removed
}

func (cs *ClusterState) GetProducer(id string) (*protocol.ProducerInfo, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	producer, ok := cs.producers[id]
	if !ok {
		return nil, false
	}

	return proto.Clone(producer).(*protocol.ProducerInfo), true
}

func (cs *ClusterState) ListProducers() []string {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	ids := make([]string, 0, len(cs.producers))
	for id := range cs.producers {
		ids = append(ids, id)
	}
	return ids
}

func (cs *ClusterState) CountProducers() int {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return len(cs.producers)
}

func (cs *ClusterState) RegisterConsumer(id, address string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if _, exists := cs.consumers[id]; !exists {
		cs.consumers[id] = &protocol.ConsumerInfo{
			Id:            id,
			Address:       address,
			Topics:        []string{},
			LastHeartbeat: time.Now().UnixNano(),
		}
		cs.seqNum++
		fmt.Printf("[ClusterState] Registered consumer %s (seq=%d)\n", shortID(id), cs.seqNum)
	} else {
		cs.consumers[id].LastHeartbeat = time.Now().UnixNano()
	}

	return nil
}

func (cs *ClusterState) SubscribeConsumer(consumerID, topic string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	consumer, ok := cs.consumers[consumerID]
	if !ok {
		return fmt.Errorf("consumer %s not found", consumerID)
	}

	for _, t := range consumer.Topics {
		if t == topic {
			return nil // Already subscribed
		}
	}

	consumer.Topics = append(consumer.Topics, topic)
	cs.seqNum++

	fmt.Printf("[ClusterState] Consumer %s subscribed to topic: %s (seq=%d)\n", shortID(consumerID), topic, cs.seqNum)
	return nil
}

func (cs *ClusterState) UnsubscribeConsumer(consumerID, topic string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	consumer, ok := cs.consumers[consumerID]
	if !ok {
		return fmt.Errorf("consumer %s not found", consumerID)
	}

	newTopics := []string{}
	for _, t := range consumer.Topics {
		if t != topic {
			newTopics = append(newTopics, t)
		}
	}
	consumer.Topics = newTopics
	cs.seqNum++

	fmt.Printf("[ClusterState] Consumer %s unsubscribed from topic: %s (seq=%d)\n", shortID(consumerID), topic, cs.seqNum)
	return nil
}

func (cs *ClusterState) RemoveConsumer(id string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if _, exists := cs.consumers[id]; !exists {
		return fmt.Errorf("consumer %s not found", id)
	}

	delete(cs.consumers, id)
	cs.seqNum++

	fmt.Printf("[ClusterState] Removed consumer %s (seq=%d)\n", shortID(id), cs.seqNum)
	return nil
}

func (cs *ClusterState) UpdateConsumerHeartbeat(id string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if consumer, ok := cs.consumers[id]; ok {
		consumer.LastHeartbeat = time.Now().UnixNano()
	}
}

func (cs *ClusterState) CheckConsumerTimeouts(timeout time.Duration) []string {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	now := time.Now().UnixNano()
	removed := []string{}

	for id, consumer := range cs.consumers {
		lastSeen := time.Duration(now - consumer.LastHeartbeat)

		if lastSeen > timeout {
				topics := consumer.Topics

			delete(cs.consumers, id)
			cs.seqNum++
			removed = append(removed, id)

			fmt.Printf("[ClusterState] Timeout: Removed consumer %s (last seen: %s, seq=%d)\n",
				shortID(id), lastSeen.Round(time.Second), cs.seqNum)

			for _, topic := range topics {
				if stream, exists := cs.streams[topic]; exists && stream.ConsumerId == id {
					stream.ConsumerId = ""
					cs.seqNum++
					fmt.Printf("[ClusterState] Timeout: Cleared consumer from stream for topic %s (consumer timed out, seq=%d)\n",
						topic, cs.seqNum)
				}
			}
		}
	}

	return removed
}

func (cs *ClusterState) GetConsumer(id string) (*protocol.ConsumerInfo, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	consumer, ok := cs.consumers[id]
	if !ok {
		return nil, false
	}

	return proto.Clone(consumer).(*protocol.ConsumerInfo), true
}

func (cs *ClusterState) GetConsumerSubscribers(topic string) []string {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	subscribers := []string{}
	for id, consumer := range cs.consumers {
		for _, t := range consumer.Topics {
			if t == topic {
				subscribers = append(subscribers, id)
				break
			}
		}
	}
	return subscribers
}

func (cs *ClusterState) ListConsumers() []string {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	ids := make([]string, 0, len(cs.consumers))
	for id := range cs.consumers {
		ids = append(ids, id)
	}
	return ids
}

func (cs *ClusterState) CountConsumers() int {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return len(cs.consumers)
}

func (cs *ClusterState) GetSequenceNum() int64 {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.seqNum
}

func (cs *ClusterState) Serialize() ([]byte, error) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	fmt.Printf("[ClusterState-Serialize] Starting serialization (seq=%d, brokers=%d, producers=%d, consumers=%d, streams=%d)\n",
		cs.seqNum, len(cs.brokers), len(cs.producers), len(cs.consumers), len(cs.streams))

	snapshot := &protocol.ClusterStateSnapshot{
		Brokers:   cs.brokers,
		Producers: cs.producers,
		Consumers: cs.consumers,
		SeqNum:    cs.seqNum,
		Streams:   cs.streams,
	}

	data, err := proto.Marshal(snapshot)
	if err != nil {
		fmt.Printf("[ClusterState-Serialize] FAILED: %v\n", err)
		return nil, fmt.Errorf("failed to serialize cluster state: %w", err)
	}

	fmt.Printf("[ClusterState-Serialize] Success: %d bytes\n", len(data))
	return data, nil
}

func (cs *ClusterState) Deserialize(data []byte) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	fmt.Printf("[ClusterState-Deserialize] Starting deserialization (%d bytes)\n", len(data))

	snapshot := &protocol.ClusterStateSnapshot{}
	if err := proto.Unmarshal(data, snapshot); err != nil {
		fmt.Printf("[ClusterState-Deserialize] FAILED: %v\n", err)
		return fmt.Errorf("failed to deserialize cluster state: %w", err)
	}

	fmt.Printf("[ClusterState-Deserialize] Parsed snapshot: seq=%d brokers=%d producers=%d consumers=%d streams=%d\n",
		snapshot.SeqNum, len(snapshot.Brokers), len(snapshot.Producers), len(snapshot.Consumers), len(snapshot.Streams))

	if snapshot.Brokers == nil {
		cs.brokers = make(map[string]*protocol.BrokerInfo)
	} else {
		cs.brokers = make(map[string]*protocol.BrokerInfo, len(snapshot.Brokers))
		for id, broker := range snapshot.Brokers {
			cs.brokers[id] = broker
		}
	}

	if snapshot.Producers == nil {
		cs.producers = make(map[string]*protocol.ProducerInfo)
	} else {
		cs.producers = make(map[string]*protocol.ProducerInfo, len(snapshot.Producers))
		for id, producer := range snapshot.Producers {
			cs.producers[id] = producer
		}
	}

	if snapshot.Consumers == nil {
		cs.consumers = make(map[string]*protocol.ConsumerInfo)
	} else {
		cs.consumers = make(map[string]*protocol.ConsumerInfo, len(snapshot.Consumers))
		for id, consumer := range snapshot.Consumers {
			cs.consumers[id] = consumer
		}
	}

	if snapshot.Streams == nil {
		cs.streams = make(map[string]*protocol.StreamAssignment)
	} else {
		cs.streams = make(map[string]*protocol.StreamAssignment, len(snapshot.Streams))
		for topic, stream := range snapshot.Streams {
			cs.streams[topic] = stream
		}
	}

	cs.seqNum = snapshot.SeqNum

	return nil
}

func (cs *ClusterState) PrintStatus() {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	fmt.Println("\n=== Cluster State Status ===")
	fmt.Printf("Sequence Number: %d\n", cs.seqNum)
	fmt.Printf("Brokers: %d\n", len(cs.brokers))
	for id, broker := range cs.brokers {
		role := "Follower"
		if broker.IsLeader {
			role = "Leader"
		}
		lastSeen := time.Unix(0, broker.LastHeartbeat)
		fmt.Printf("  %s: %s [%s] (last seen: %s)\n",
			shortID(id), broker.Address, role, time.Since(lastSeen).Round(time.Second))
	}
	fmt.Printf("Producers: %d\n", len(cs.producers))
	for id, producer := range cs.producers {
		lastSeen := time.Unix(0, producer.LastHeartbeat)
		fmt.Printf("  %s: %s (topic: %s, last seen: %s)\n",
			shortID(id), producer.Address, producer.Topic, time.Since(lastSeen).Round(time.Second))
	}
	fmt.Printf("Consumers: %d\n", len(cs.consumers))
	for id, consumer := range cs.consumers {
		lastSeen := time.Unix(0, consumer.LastHeartbeat)
		fmt.Printf("  %s: %s (topics: %v, last seen: %s)\n",
			shortID(id), consumer.Address, consumer.Topics, time.Since(lastSeen).Round(time.Second))
	}
	fmt.Println("============================")
}

func (cs *ClusterState) GetStreamAssignment(topic string) (*protocol.StreamAssignment, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	stream, exists := cs.streams[topic]
	return stream, exists
}

func (cs *ClusterState) HasProducerForTopic(topic string) bool {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	stream, exists := cs.streams[topic]
	return exists && stream.ProducerId != ""
}

func (cs *ClusterState) HasConsumerForTopic(topic string) bool {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	stream, exists := cs.streams[topic]
	return exists && stream.ConsumerId != ""
}

func (cs *ClusterState) AssignStream(topic, producerID, brokerID, brokerAddress string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if existing, exists := cs.streams[topic]; exists {
		return fmt.Errorf("stream already exists for topic %s (producer: %s, broker: %s)",
			topic, shortID(existing.ProducerId), shortID(existing.AssignedBrokerId))
	}

	cs.streams[topic] = &protocol.StreamAssignment{
		Topic:            topic,
		ProducerId:       producerID,
		ConsumerId:       "", // Will be set when consumer subscribes
		AssignedBrokerId: brokerID,
		BrokerAddress:    brokerAddress,
	}

	cs.seqNum++

	fmt.Printf("[ClusterState] Stream assigned: topic=%s producer=%s broker=%s seq=%d\n",
		topic, shortID(producerID), shortID(brokerID), cs.seqNum)

	return nil
}

func (cs *ClusterState) AssignConsumerToStream(consumerID, topic string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	stream, exists := cs.streams[topic]
	if !exists {
		return fmt.Errorf("no stream exists for topic %s", topic)
	}

	if stream.ConsumerId != "" {
		return fmt.Errorf("consumer already assigned to topic %s: %s",
			topic, shortID(stream.ConsumerId))
	}

	stream.ConsumerId = consumerID
	cs.seqNum++

	fmt.Printf("[ClusterState] Consumer assigned to stream: topic=%s consumer=%s broker=%s seq=%d\n",
		topic, shortID(consumerID), shortID(stream.AssignedBrokerId), cs.seqNum)

	return nil
}

func (cs *ClusterState) ListAllStreams() map[string]*protocol.StreamAssignment {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	result := make(map[string]*protocol.StreamAssignment, len(cs.streams))
	for topic, stream := range cs.streams {
		result[topic] = stream
	}
	return result
}

func (cs *ClusterState) GetStreamsByBroker(brokerID string) []*protocol.StreamAssignment {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	var streams []*protocol.StreamAssignment
	for _, stream := range cs.streams {
		if stream.AssignedBrokerId == brokerID {
			streams = append(streams, stream)
		}
	}

	return streams
}

func (cs *ClusterState) ReassignStreamBroker(topic, newBrokerID, newBrokerAddress string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	stream, exists := cs.streams[topic]
	if !exists {
		return fmt.Errorf("no stream exists for topic %s", topic)
	}

	oldBrokerID := stream.AssignedBrokerId
	stream.AssignedBrokerId = newBrokerID
	stream.BrokerAddress = newBrokerAddress

	cs.seqNum++

	fmt.Printf("[ClusterState] Stream reassigned: topic=%s from %s to %s seq=%d\n",
		topic, shortID(oldBrokerID), shortID(newBrokerID), cs.seqNum)

	return nil
}

func (cs *ClusterState) RemoveStream(topic string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	stream, exists := cs.streams[topic]
	if !exists {
		return fmt.Errorf("no stream exists for topic %s", topic)
	}

	fmt.Printf("[ClusterState] Removing stream: topic=%s producer=%s\n",
		topic, shortID(stream.ProducerId))
	delete(cs.streams, topic)
	cs.seqNum++

	return nil
}

func (cs *ClusterState) RemoveConsumerFromStream(topic, consumerID string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	stream, exists := cs.streams[topic]
	if !exists {
		return fmt.Errorf("no stream exists for topic %s", topic)
	}

	if stream.ConsumerId == "" {
		return fmt.Errorf("no consumer assigned to topic %s", topic)
	}

	if stream.ConsumerId != consumerID {
		return fmt.Errorf("consumer %s not assigned to topic %s (assigned: %s)",
			shortID(consumerID), topic, shortID(stream.ConsumerId))
	}

	fmt.Printf("[ClusterState] Removing consumer %s from stream: topic=%s\n",
		shortID(stream.ConsumerId), topic)

	stream.ConsumerId = ""
	cs.seqNum++

	return nil
}
