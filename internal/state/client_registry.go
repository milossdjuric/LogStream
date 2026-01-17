package state

import (
	"fmt"
	"sync"
	"time"
)

// ProducerInfo represents a producer client
type ProducerInfo struct {
	ID            string
	Address       string
	Topic         string
	LastHeartbeat int64
}

// ProducerRegistry maintains registry of active producers
type ProducerRegistry struct {
	mu        sync.RWMutex
	producers map[string]*ProducerInfo // key: producer ID
}

// NewProducerRegistry creates a new producer registry
func NewProducerRegistry() *ProducerRegistry {
	return &ProducerRegistry{
		producers: make(map[string]*ProducerInfo),
	}
}

// Register adds or updates a producer
func (pr *ProducerRegistry) Register(id, address, topic string) error {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	pr.producers[id] = &ProducerInfo{
		ID:            id,
		Address:       address,
		Topic:         topic,
		LastHeartbeat: time.Now().UnixNano(),
	}

	fmt.Printf("[ProducerRegistry] Registered producer %s (topic: %s)\n", id[:8], topic)
	return nil
}

// Remove removes a producer
func (pr *ProducerRegistry) Remove(id string) error {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if _, exists := pr.producers[id]; !exists {
		return fmt.Errorf("producer %s not found", id)
	}

	delete(pr.producers, id)
	fmt.Printf("[ProducerRegistry] Removed producer %s\n", id[:8])
	return nil
}

// UpdateHeartbeat updates last heartbeat timestamp
func (pr *ProducerRegistry) UpdateHeartbeat(id string) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if producer, ok := pr.producers[id]; ok {
		producer.LastHeartbeat = time.Now().UnixNano()
	}
}

// CheckTimeouts removes producers that haven't sent heartbeat
func (pr *ProducerRegistry) CheckTimeouts(timeout time.Duration) []string {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	now := time.Now().UnixNano()
	removed := []string{}

	for id, producer := range pr.producers {
		lastSeen := time.Duration(now - producer.LastHeartbeat)

		if lastSeen > timeout {
			delete(pr.producers, id)
			removed = append(removed, id)

			fmt.Printf("[ProducerRegistry] Timeout: Removed producer %s (last seen: %s)\n",
				id[:8], lastSeen.Round(time.Second))
		}
	}

	return removed
}

// Get retrieves producer info
func (pr *ProducerRegistry) Get(id string) (*ProducerInfo, bool) {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	producer, ok := pr.producers[id]
	return producer, ok
}

// List returns all producer IDs
func (pr *ProducerRegistry) List() []string {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	ids := make([]string, 0, len(pr.producers))
	for id := range pr.producers {
		ids = append(ids, id)
	}
	return ids
}

// Count returns number of producers
func (pr *ProducerRegistry) Count() int {
	pr.mu.RLock()
	defer pr.mu.RUnlock()
	return len(pr.producers)
}

// Represents a consumer client
type ConsumerInfo struct {
	ID            string
	Address       string
	Topics        []string // Subscribed topics
	LastHeartbeat int64
}

// Maintains a registry of active consumers
type ConsumerRegistry struct {
	mu        sync.RWMutex
	consumers map[string]*ConsumerInfo   // key: consumer ID
	topicSubs map[string]map[string]bool // key: topic, set of consumer IDs
}

// Create a new consumer registry
func NewConsumerRegistry() *ConsumerRegistry {
	return &ConsumerRegistry{
		consumers: make(map[string]*ConsumerInfo),
		topicSubs: make(map[string]map[string]bool),
	}
}

// Register adds or updates a consumer
func (cr *ConsumerRegistry) Register(id, address string) error {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	if _, exists := cr.consumers[id]; !exists {
		cr.consumers[id] = &ConsumerInfo{
			ID:            id,
			Address:       address,
			Topics:        []string{},
			LastHeartbeat: time.Now().UnixNano(),
		}
		fmt.Printf("[ConsumerRegistry] Registered consumer %s\n", id[:8])
	} else {
		cr.consumers[id].LastHeartbeat = time.Now().UnixNano()
	}

	return nil
}

// Subscribe adds a topic subscription for a consumer
func (cr *ConsumerRegistry) Subscribe(consumerID, topic string) error {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	consumer, ok := cr.consumers[consumerID]
	if !ok {
		return fmt.Errorf("consumer %s not found", consumerID)
	}

	// Add topic to consumer's subscription list
	found := false
	for _, t := range consumer.Topics {
		if t == topic {
			found = true
			break
		}
	}
	if !found {
		consumer.Topics = append(consumer.Topics, topic)
	}

	// Add consumer to topic's subscriber set
	if cr.topicSubs[topic] == nil {
		cr.topicSubs[topic] = make(map[string]bool)
	}
	cr.topicSubs[topic][consumerID] = true

	fmt.Printf("[ConsumerRegistry] Consumer %s subscribed to topic: %s\n", consumerID[:8], topic)
	return nil
}

// Unsubscribe removes a topic subscription for a consumer
func (cr *ConsumerRegistry) Unsubscribe(consumerID, topic string) error {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	consumer, ok := cr.consumers[consumerID]
	if !ok {
		return fmt.Errorf("consumer %s not found", consumerID)
	}

	// Remove topic from consumer's subscription list
	newTopics := []string{}
	for _, t := range consumer.Topics {
		if t != topic {
			newTopics = append(newTopics, t)
		}
	}
	consumer.Topics = newTopics

	// Remove consumer from topic's subscriber set
	if subs, ok := cr.topicSubs[topic]; ok {
		delete(subs, consumerID)
		if len(subs) == 0 {
			delete(cr.topicSubs, topic)
		}
	}

	fmt.Printf("[ConsumerRegistry] Consumer %s unsubscribed from topic: %s\n", consumerID[:8], topic)
	return nil
}

// Remove removes a consumer
func (cr *ConsumerRegistry) Remove(id string) error {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	consumer, exists := cr.consumers[id]
	if !exists {
		return fmt.Errorf("consumer %s not found", id)
	}

	// Remove from all topic subscriptions
	for _, topic := range consumer.Topics {
		if subs, ok := cr.topicSubs[topic]; ok {
			delete(subs, id)
			if len(subs) == 0 {
				delete(cr.topicSubs, topic)
			}
		}
	}

	delete(cr.consumers, id)
	fmt.Printf("[ConsumerRegistry] Removed consumer %s\n", id[:8])
	return nil
}

// Update last heartbeat timestamp
func (cr *ConsumerRegistry) UpdateHeartbeat(id string) {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	if consumer, ok := cr.consumers[id]; ok {
		consumer.LastHeartbeat = time.Now().UnixNano()
	}
}

// Remove consumers that have timed out
func (cr *ConsumerRegistry) CheckTimeouts(timeout time.Duration) []string {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	now := time.Now().UnixNano()
	removed := []string{}

	for id, consumer := range cr.consumers {
		lastSeen := time.Duration(now - consumer.LastHeartbeat)

		if lastSeen > timeout {
			// Remove from all topic subscriptions
			for _, topic := range consumer.Topics {
				if subs, ok := cr.topicSubs[topic]; ok {
					delete(subs, id)
					if len(subs) == 0 {
						delete(cr.topicSubs, topic)
					}
				}
			}

			delete(cr.consumers, id)
			removed = append(removed, id)

			fmt.Printf("[ConsumerRegistry] Timeout: Removed consumer %s (last seen: %s)\n",
				id[:8], lastSeen.Round(time.Second))
		}
	}

	return removed
}

// Get retrieves consumer info
func (cr *ConsumerRegistry) Get(id string) (*ConsumerInfo, bool) {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	consumer, ok := cr.consumers[id]
	return consumer, ok
}

func (cr *ConsumerRegistry) GetSubscribers(topic string) []string {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	if subs, ok := cr.topicSubs[topic]; ok {
		subscribers := make([]string, 0, len(subs))
		for id := range subs {
			subscribers = append(subscribers, id)
		}
		return subscribers
	}
	return []string{}
}

// List returns all consumer IDs
func (cr *ConsumerRegistry) List() []string {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	ids := make([]string, 0, len(cr.consumers))
	for id := range cr.consumers {
		ids = append(ids, id)
	}
	return ids
}

// Count returns number of consumers
func (cr *ConsumerRegistry) Count() int {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	return len(cr.consumers)
}

// Print current consumer registry status
func (cr *ConsumerRegistry) PrintStatus() {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	fmt.Println("\n=== Consumer Registry ===")
	fmt.Printf("Total Consumers: %d\n", len(cr.consumers))

	for id, consumer := range cr.consumers {
		lastSeen := time.Unix(0, consumer.LastHeartbeat)
		fmt.Printf("  %s: %s (topics: %v, last seen: %s)\n",
			id[:8],
			consumer.Address,
			consumer.Topics,
			time.Since(lastSeen).Round(time.Second))
	}

	fmt.Println("\n=== Topic Subscriptions ===")
	for topic, subs := range cr.topicSubs {
		fmt.Printf("  %s: %d subscribers\n", topic, len(subs))
	}
	fmt.Println("========================")
}
