package tests

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

// TestCluster represents a cluster of broker nodes for testing
type TestCluster struct {
	Brokers   []*TestBroker
	Leader    *TestBroker
	Followers []*TestBroker
	BasePort  int
	Multicast string
}

// TestBroker represents a single broker process
type TestBroker struct {
	ID      int
	Address string
	Port    int
	Cmd     *exec.Cmd
	LogFile string
}

// TestProducer represents a producer client process
type TestProducer struct {
	ID      string
	Topic   string
	Cmd     *exec.Cmd
	LogFile string
}

// TestConsumer represents a consumer client process
type TestConsumer struct {
	ID      string
	Topic   string
	Cmd     *exec.Cmd
	LogFile string
}

// StartTestCluster starts N broker nodes for testing
// Returns a TestCluster with running broker processes
func StartTestCluster(numBrokers int) (*TestCluster, error) {
	// Find project root and broker binary
	projectRoot, err := filepath.Abs("../..")
	if err != nil {
		projectRoot = "."
	}
	
	brokerBinary := filepath.Join(projectRoot, "broker")
	if _, err := os.Stat(brokerBinary); os.IsNotExist(err) {
		// Try current directory
		brokerBinary = "./broker"
		if _, err := os.Stat(brokerBinary); os.IsNotExist(err) {
			return nil, fmt.Errorf("broker binary not found - run: go build -o broker cmd/broker/main.go")
		}
	}

	cluster := &TestCluster{
		Brokers:   make([]*TestBroker, 0, numBrokers),
		BasePort:  8001,
		Multicast: "239.0.0.1:9999",
	}

	// Start each broker
	for i := 0; i < numBrokers; i++ {
		port := cluster.BasePort + i
		addr := fmt.Sprintf("127.0.0.1:%d", port)
		logFile := fmt.Sprintf("/tmp/test-broker-%d.log", i)

		// Create log file
		logF, err := os.Create(logFile)
		if err != nil {
			cluster.Shutdown()
			return nil, fmt.Errorf("failed to create log file: %w", err)
		}

	// Start broker process with environment variables
	cmd := exec.Command(brokerBinary)
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("NODE_ADDRESS=%s", addr),
		fmt.Sprintf("MULTICAST_GROUP=%s", cluster.Multicast),
		"BROADCAST_PORT=8888",
	)
	cmd.Stdout = logF
	cmd.Stderr = logF

	if err := cmd.Start(); err != nil {
		logF.Close()
		cluster.Shutdown()
		return nil, fmt.Errorf("failed to start broker %d: %w", i, err)
	}

		broker := &TestBroker{
			ID:      i,
			Address: addr,
			Port:    port,
			Cmd:     cmd,
			LogFile: logFile,
		}

		cluster.Brokers = append(cluster.Brokers, broker)

		// First broker is typically the leader (elected automatically)
		if i == 0 {
			cluster.Leader = broker
		} else {
			cluster.Followers = append(cluster.Followers, broker)
		}

		// Stagger startup
		time.Sleep(200 * time.Millisecond)
	}

	// Wait for cluster to stabilize
	time.Sleep(2 * time.Second)

	return cluster, nil
}

// NewProducer creates and starts a test producer
func (c *TestCluster) NewProducer(producerID, topic string) (*TestProducer, error) {
	if c.Leader == nil {
		return nil, fmt.Errorf("no leader in cluster")
	}

	projectRoot, err := filepath.Abs("../..")
	if err != nil {
		projectRoot = "."
	}

	producerBinary := filepath.Join(projectRoot, "producer")
	if _, err := os.Stat(producerBinary); os.IsNotExist(err) {
		producerBinary = "./producer"
		if _, err := os.Stat(producerBinary); os.IsNotExist(err) {
			return nil, fmt.Errorf("producer binary not found - run: go build -o producer cmd/producer/main.go")
		}
	}

	logFile := fmt.Sprintf("/tmp/test-producer-%s.log", producerID)
	logF, err := os.Create(logFile)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %w", err)
	}

	// Start producer in auto mode (1 msg/sec by default) with environment variables
	cmd := exec.Command(producerBinary)
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("LEADER_ADDRESS=%s", c.Leader.Address),
		fmt.Sprintf("TOPIC=%s", topic),
	)
	// Add command line args for rate control
	cmd.Args = []string{producerBinary, "-rate", "1"}
	cmd.Stdout = logF
	cmd.Stderr = logF

	if err := cmd.Start(); err != nil {
		logF.Close()
		return nil, fmt.Errorf("failed to start producer: %w", err)
	}

	producer := &TestProducer{
		ID:      producerID,
		Topic:   topic,
		Cmd:     cmd,
		LogFile: logFile,
	}

	time.Sleep(500 * time.Millisecond) // Let producer connect

	return producer, nil
}

// NewConsumer creates and starts a test consumer
func (c *TestCluster) NewConsumer(consumerID, topic string) (*TestConsumer, error) {
	if c.Leader == nil {
		return nil, fmt.Errorf("no leader in cluster")
	}

	projectRoot, err := filepath.Abs("../..")
	if err != nil {
		projectRoot = "."
	}

	consumerBinary := filepath.Join(projectRoot, "consumer")
	if _, err := os.Stat(consumerBinary); os.IsNotExist(err) {
		consumerBinary = "./consumer"
		if _, err := os.Stat(consumerBinary); os.IsNotExist(err) {
			return nil, fmt.Errorf("consumer binary not found - run: go build -o consumer cmd/consumer/main.go")
		}
	}

	logFile := fmt.Sprintf("/tmp/test-consumer-%s.log", consumerID)
	logF, err := os.Create(logFile)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %w", err)
	}

	// Start consumer with environment variables
	cmd := exec.Command(consumerBinary)
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("LEADER_ADDRESS=%s", c.Leader.Address),
		fmt.Sprintf("TOPIC=%s", topic),
	)
	cmd.Stdout = logF
	cmd.Stderr = logF

	if err := cmd.Start(); err != nil {
		logF.Close()
		return nil, fmt.Errorf("failed to start consumer: %w", err)
	}

	consumer := &TestConsumer{
		ID:      consumerID,
		Topic:   topic,
		Cmd:     cmd,
		LogFile: logFile,
	}

	time.Sleep(500 * time.Millisecond) // Let consumer connect

	return consumer, nil
}

// Shutdown gracefully shuts down the entire cluster
func (c *TestCluster) Shutdown() {
	var wg sync.WaitGroup

	// Stop all brokers in parallel
	for _, broker := range c.Brokers {
		if broker.Cmd != nil && broker.Cmd.Process != nil {
			wg.Add(1)
			go func(b *TestBroker) {
				defer wg.Done()
				b.Cmd.Process.Signal(os.Interrupt)
				b.Cmd.Wait()
			}(broker)
		}
	}

	// Wait with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Clean shutdown
	case <-time.After(5 * time.Second):
		// Force kill
		for _, broker := range c.Brokers {
			if broker.Cmd != nil && broker.Cmd.Process != nil {
				broker.Cmd.Process.Kill()
			}
		}
	}

	time.Sleep(500 * time.Millisecond)
}

// Stop stops a specific broker
func (b *TestBroker) Stop() error {
	if b.Cmd == nil || b.Cmd.Process == nil {
		return fmt.Errorf("broker not running")
	}
	
	b.Cmd.Process.Signal(os.Interrupt)
	
	// Wait for graceful shutdown with timeout
	done := make(chan error, 1)
	go func() {
		done <- b.Cmd.Wait()
	}()

	select {
	case <-done:
		return nil
	case <-time.After(3 * time.Second):
		// Force kill
		return b.Cmd.Process.Kill()
	}
}

// Close stops the producer
func (p *TestProducer) Close() error {
	if p.Cmd == nil || p.Cmd.Process == nil {
		return nil
	}
	p.Cmd.Process.Signal(os.Interrupt)
	return p.Cmd.Wait()
}

// Close stops the consumer
func (c *TestConsumer) Close() error {
	if c.Cmd == nil || c.Cmd.Process == nil {
		return nil
	}
	c.Cmd.Process.Signal(os.Interrupt)
	return c.Cmd.Wait()
}

// GetLeaderAddress returns the current leader's address
func (c *TestCluster) GetLeaderAddress() string {
	if c.Leader != nil {
		return c.Leader.Address
	}
	return ""
}

// GetBrokerCount returns the number of running brokers
func (c *TestCluster) GetBrokerCount() int {
	count := 0
	for _, broker := range c.Brokers {
		if broker.Cmd != nil && broker.Cmd.ProcessState == nil {
			count++
		}
	}
	return count
}

// WaitForStability waits for cluster to stabilize after changes
func (c *TestCluster) WaitForStability(duration time.Duration) {
	time.Sleep(duration)
}
