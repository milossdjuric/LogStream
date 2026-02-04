package integration

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/milossdjuric/logstream/internal/client"
)

// UseNetns determines if tests should use network namespaces
// Set via environment variable: USE_NETNS=1
var UseNetns = os.Getenv("USE_NETNS") == "1"

// TCPCircuitBreaker manages TCP connection attempts with circuit breaker pattern
type TCPCircuitBreaker struct {
	state      string    // "CLOSED", "OPEN", "HALF_OPEN"
	failures   int
	lastFail   time.Time
	cooldown   time.Duration
	maxFails   int
}

// NewTCPCircuitBreaker creates a new circuit breaker for TCP connections
func NewTCPCircuitBreaker() *TCPCircuitBreaker {
	return &TCPCircuitBreaker{
		state:    "CLOSED",
		cooldown: 5 * time.Second,
		maxFails: 3,
	}
}

// TryConnect attempts to connect to TCP address with circuit breaker logic
func (cb *TCPCircuitBreaker) TryConnect(address string) error {
	switch cb.state {
	case "OPEN":
		// Check if cooldown has elapsed
		if time.Since(cb.lastFail) < cb.cooldown {
			remaining := cb.cooldown - time.Since(cb.lastFail)
			return fmt.Errorf("circuit OPEN, cooldown: %v remaining", remaining)
		}
		// Cooldown elapsed, try half-open
		cb.state = "HALF_OPEN"
		fallthrough

	case "HALF_OPEN":
		// Single attempt to test recovery
		conn, err := net.DialTimeout("tcp", address, 500*time.Millisecond)
		if err == nil {
			conn.Close()
			cb.state = "CLOSED"
			cb.failures = 0
			return nil // Recovery successful!
		}
		// Failed - back to OPEN
		cb.state = "OPEN"
		cb.lastFail = time.Now()
		return fmt.Errorf("half-open attempt failed: %w", err)

	case "CLOSED":
		// Normal operation
		conn, err := net.DialTimeout("tcp", address, 500*time.Millisecond)
		if err == nil {
			conn.Close()
			cb.failures = 0
			return nil
		}

		// Connection failed
		cb.failures++
		if cb.failures >= cb.maxFails {
			// Too many failures - open circuit
			cb.state = "OPEN"
			cb.lastFail = time.Now()
		}
		return err
	}

	return nil
}

// TestCluster represents a running cluster for integration tests
type TestCluster struct {
	t        *testing.T
	Brokers  []*TestBroker
	cleanup  []func()
	basedir  string
	mu       sync.Mutex
}

// TestBroker represents a single broker instance
type TestBroker struct {
	ID         string
	Address    string
	Namespace  string // Network namespace (empty if not using netns)
	Cmd        *exec.Cmd
	LogFile    *os.File
	isLeader   bool
}

// StartTestCluster starts N brokers for testing
// First broker becomes leader, rest are followers
// Uses network namespaces if USE_NETNS=1 environment variable is set
func StartTestCluster(t *testing.T, numBrokers int) *TestCluster {
	t.Helper()

	if numBrokers < 1 {
		t.Fatal("numBrokers must be at least 1")
	}

	// Create temp directory for logs
	basedir := fmt.Sprintf("/tmp/logstream-integration-test-%d", time.Now().Unix())
	if err := os.MkdirAll(basedir, 0755); err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	cluster := &TestCluster{
		t:       t,
		Brokers: make([]*TestBroker, 0, numBrokers),
		basedir: basedir,
	}

	// Add cleanup for base directory
	cluster.cleanup = append(cluster.cleanup, func() {
		os.RemoveAll(basedir)
	})

	// Setup network namespaces if enabled
	if UseNetns {
		netnsConfig := DefaultNetnsConfig()
		if err := EnsureNetnsSetup(netnsConfig); err != nil {
			t.Fatalf("Failed to setup network namespaces: %v\n"+
				"Run: sudo ./deploy/netns/setup-netns.sh\n"+
				"Error: %v", err, err)
		}
		t.Logf("[OK] Network namespaces ready (mode: netns)")
		
		// Add cleanup for netns processes
		cluster.cleanup = append(cluster.cleanup, func() {
			CleanupNetnsProcesses(netnsConfig)
		})
	} else {
		t.Logf("[!] Running in localhost mode (USE_NETNS not set)")
		t.Logf("  Multi-broker tests may fail due to UDP multicast limitations")
		t.Logf("  For reliable multi-broker testing, use: sudo -E USE_NETNS=1 go test")
	}

	// Start brokers
	for i := 0; i < numBrokers; i++ {
		var addr string
		var nsName string
		
		if UseNetns {
			// Use network namespace IPs (172.20.0.10, 172.20.0.20, 172.20.0.30)
			nsName = GetNetnsName(i)
			config := DefaultNetnsConfig()
			addr = GetNetnsAddress(i, config.BaseIP, 8001)
			t.Logf("  Broker %d: %s in namespace %s", i, addr, nsName)
		} else {
			// Use localhost (legacy mode, may not work for multi-broker)
			basePort := 9001
			addr = fmt.Sprintf("127.0.0.1:%d", basePort+i)
			t.Logf("  Broker %d: %s (localhost)", i, addr)
		}
		
		broker := cluster.startBroker(addr, nsName, i == 0)
		cluster.Brokers = append(cluster.Brokers, broker)

		// Wait between starts to ensure proper ordering
		time.Sleep(500 * time.Millisecond)
	}

	// Wait for cluster to stabilize
	time.Sleep(2 * time.Second)

	t.Logf("[OK] Test cluster started: %d brokers", len(cluster.Brokers))
	return cluster
}

// startBroker starts a single broker process, optionally in a network namespace
func (c *TestCluster) startBroker(address string, namespace string, isLeader bool) *TestBroker {
	c.t.Helper()

	// Create log file with appropriate name
	logName := address
	if namespace != "" {
		logName = namespace
	}
	logPath := fmt.Sprintf("%s/broker-%s.log", c.basedir, logName)
	logFile, err := os.Create(logPath)
	if err != nil {
		c.t.Fatalf("Failed to create log file: %v", err)
	}

	// Find logstream binary (in project root, two levels up from tests/integration)
	logstreamPath := "../../logstream"
	if _, err := os.Stat(logstreamPath); os.IsNotExist(err) {
		c.t.Fatalf("logstream binary not found at %s - run: go build -o logstream main.go", logstreamPath)
	}

	// Prepare environment variables
	// Use standard multicast group (9999) and broadcast port (8888) for consistency with bash tests
	env := []string{
		fmt.Sprintf("NODE_ADDRESS=%s", address),
		"MULTICAST_GROUP=239.0.0.1:9999",
		"BROADCAST_PORT=8888",
	}

	var cmd *exec.Cmd
	
	if namespace != "" && UseNetns {
		// Start in network namespace using ip netns exec
		// This requires sudo privileges
		cmd = exec.Command("sudo", "ip", "netns", "exec", namespace, logstreamPath)
		// CRITICAL: Must append to os.Environ(), not replace it!
		// Otherwise process won't have PATH, HOME, etc.
		cmd.Env = append(os.Environ(), env...)
		c.t.Logf("    Starting in namespace %s at %s (PID will be shown after start)", namespace, address)
	} else {
		// Start normally on localhost
		cmd = exec.Command(logstreamPath)
		cmd.Env = append(os.Environ(), env...)
		c.t.Logf("    Starting on localhost at %s (PID will be shown after start)", address)
	}

	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true, // Create new process group for clean killing
	}

	if err := cmd.Start(); err != nil {
		logFile.Close()
		c.t.Fatalf("Failed to start broker: %v", err)
	}

	broker := &TestBroker{
		Address:   address,
		Namespace: namespace,
		Cmd:       cmd,
		LogFile:   logFile,
		isLeader:  isLeader,
	}

	// Add cleanup
	c.cleanup = append(c.cleanup, func() {
		broker.Stop()
	})

	c.t.Logf("    [OK] Started at %s (leader: %v, PID: %d)", address, isLeader, cmd.Process.Pid)

	// Wait for TCP listener to be ready using circuit breaker
	if err := c.waitForTCPReady(address, 30*time.Second); err != nil {
		c.t.Fatalf("TCP listener never became ready: %v", err)
	}

	return broker
}

// waitForTCPReady waits for the TCP listener to be ready using circuit breaker pattern
func (c *TestCluster) waitForTCPReady(address string, timeout time.Duration) error {
	cb := NewTCPCircuitBreaker()
	deadline := time.Now().Add(timeout)
	attempt := 0

	for time.Now().Before(deadline) {
		attempt++
		err := cb.TryConnect(address)

		if err == nil {
			c.t.Logf("    TCP listener ready on %s (attempt %d, state: %s)", address, attempt, cb.state)
			return nil
		}

		// If circuit is OPEN, wait for cooldown
		if cb.state == "OPEN" {
			time.Sleep(cb.cooldown)
		} else {
			// Normal retry delay
			time.Sleep(500 * time.Millisecond)
		}
	}

	return fmt.Errorf("timeout after %v (attempts: %d, final state: %s)", timeout, attempt, cb.state)
}

// Cleanup stops all brokers and removes temp files
func (c *TestCluster) Cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Execute cleanup functions in reverse order
	for i := len(c.cleanup) - 1; i >= 0; i-- {
		c.cleanup[i]()
	}

	c.t.Logf("[OK] Test cluster cleaned up")
}

// Stop gracefully stops the broker
func (b *TestBroker) Stop() {
	if b.Cmd != nil && b.Cmd.Process != nil {
		// Try graceful shutdown first
		b.Cmd.Process.Signal(syscall.SIGTERM)
		
		// Wait up to 3 seconds
		done := make(chan error, 1)
		go func() {
			done <- b.Cmd.Wait()
		}()

		select {
		case <-done:
			// Graceful shutdown succeeded
		case <-time.After(3 * time.Second):
			// Force kill
			b.Cmd.Process.Kill()
		}
	}

	if b.LogFile != nil {
		b.LogFile.Close()
	}
}

// Kill forcefully kills the broker (for failure simulation)
func (b *TestBroker) Kill() {
	if b.Cmd != nil && b.Cmd.Process != nil {
		b.Cmd.Process.Kill()
		// Wait for process to actually terminate
		b.Cmd.Wait()
	}
}

// Restart restarts a killed broker
func (c *TestCluster) Restart(index int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if index < 0 || index >= len(c.Brokers) {
		return fmt.Errorf("invalid broker index: %d", index)
	}

	oldBroker := c.Brokers[index]
	oldBroker.Stop()

	// Start new broker with same address and namespace
	newBroker := c.startBroker(oldBroker.Address, oldBroker.Namespace, oldBroker.isLeader)
	c.Brokers[index] = newBroker

	return nil
}

// GetLeader returns the first broker (assumed leader in tests)
func (c *TestCluster) GetLeader() *TestBroker {
	if len(c.Brokers) > 0 {
		return c.Brokers[0]
	}
	return nil
}

// NewProducer creates a test producer client
func (c *TestCluster) NewProducer(topic string) (*client.Producer, error) {
	leader := c.GetLeader()
	if leader == nil {
		return nil, fmt.Errorf("no leader available")
	}

	producer := client.NewProducer(topic, leader.Address)
	if err := producer.Connect(); err != nil {
		return nil, fmt.Errorf("producer connect failed: %w", err)
	}

	// Add cleanup
	c.cleanup = append(c.cleanup, func() {
		producer.Close()
	})

	return producer, nil
}

// NewConsumer creates a test consumer client
func (c *TestCluster) NewConsumer(topic string) (*client.Consumer, error) {
	leader := c.GetLeader()
	if leader == nil {
		return nil, fmt.Errorf("no leader available")
	}

	consumer := client.NewConsumer(topic, leader.Address)
	if err := consumer.Connect(); err != nil {
		return nil, fmt.Errorf("consumer connect failed: %w", err)
	}

	// Add cleanup
	c.cleanup = append(c.cleanup, func() {
		consumer.Close()
	})

	return consumer, nil
}

// WaitForStability waits for cluster to stabilize
func (c *TestCluster) WaitForStability(duration time.Duration) {
	c.t.Logf("Waiting %v for cluster stability...", duration)
	time.Sleep(duration)
}

// GetBrokerLogs returns the log file path for a broker
func (b *TestBroker) GetLogPath() string {
	if b.LogFile != nil {
		return b.LogFile.Name()
	}
	return ""
}

// CountRunningBrokers returns the number of brokers still running
// Uses Signal(0) to accurately detect if processes are alive
func (c *TestCluster) CountRunningBrokers() int {
	count := 0
	for _, broker := range c.Brokers {
		if broker.Cmd != nil && broker.Cmd.Process != nil {
			// Send signal 0 (null signal) to check if process exists
			// Returns nil if process is alive, error if dead
			err := broker.Cmd.Process.Signal(syscall.Signal(0))
			if err == nil {
				count++ // Process is actually alive
			}
		}
	}
	return count
}
