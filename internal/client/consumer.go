package client

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/milossdjuric/logstream/internal/protocol"
)

// Consumer handles consuming data from LogStream
type Consumer struct {
	id               string
	topic            string
	leaderAddr       string
	brokerAddr       string   // Assigned broker address (from CONSUME_ACK)
	brokerAddrMu     sync.RWMutex
	tcpConn          net.Conn // Connection to the assigned broker
	tcpConnMu        sync.Mutex
	tcpListener      net.Listener // TCP listener for incoming messages from leader
	localAddr        string       // Local address used for registration
	results          chan *protocol.ResultMessage
	errors           chan error
	stopSignal       chan struct{}
	stopListener     chan struct{}
	reconnectSignal  chan string // Signal to reconnect to a new broker
	enableProcessing       bool  // Whether to request data processing from broker
	analyticsWindowSeconds int32 // 0 = use broker default
	analyticsIntervalMs    int32 // 0 = use broker default
	clientPort             int    // Fixed TCP listener port (0 = OS picks)
	advertiseAddr          string // Override advertised IP (e.g. Windows host IP for WSL)
	wg                     sync.WaitGroup // Synchronize goroutine lifecycle
}

// NewConsumer creates a new consumer
// If leaderAddr is empty, auto-discovery via broadcast will be used during Connect()
func NewConsumer(topic, leaderAddr string) *Consumer {
	// Generate ID based on topic if no leader address (will discover later)
	idSeed := leaderAddr
	if idSeed == "" {
		idSeed = topic
	}
	return &Consumer{
		id:               protocol.GenerateClientID("consumer", idSeed),
		topic:            topic,
		leaderAddr:       leaderAddr,
		results:          make(chan *protocol.ResultMessage, 100),
		errors:           make(chan error, 10),
		stopSignal:       make(chan struct{}),
		stopListener:     make(chan struct{}),
		reconnectSignal:  make(chan string, 1),
		enableProcessing: true, // Enable data processing by default
	}
}

// ConsumerOptions holds all configurable consumer options
type ConsumerOptions struct {
	EnableProcessing       bool
	AnalyticsWindowSeconds int32  // 0 = use broker default
	AnalyticsIntervalMs    int32  // 0 = use broker default
	ClientPort             int    // Fixed TCP listener port (0 = OS picks)
	AdvertiseAddr          string // Override advertised IP (e.g. Windows host IP for WSL)
}

// NewConsumerWithOptions creates a new consumer with options
// If leaderAddr is empty, auto-discovery via broadcast will be used during Connect()
func NewConsumerWithOptions(topic, leaderAddr string, enableProcessing bool) *Consumer {
	return NewConsumerWithFullOptions(topic, leaderAddr, ConsumerOptions{
		EnableProcessing: enableProcessing,
	})
}

// NewConsumerWithFullOptions creates a new consumer with full options
// If leaderAddr is empty, auto-discovery via broadcast will be used during Connect()
func NewConsumerWithFullOptions(topic, leaderAddr string, opts ConsumerOptions) *Consumer {
	c := NewConsumer(topic, leaderAddr)
	c.enableProcessing = opts.EnableProcessing
	c.analyticsWindowSeconds = opts.AnalyticsWindowSeconds
	c.analyticsIntervalMs = opts.AnalyticsIntervalMs
	c.clientPort = opts.ClientPort
	c.advertiseAddr = opts.AdvertiseAddr
	return c
}

// Connect registers with the leader and then subscribes to the assigned broker
// If no leader address was provided, auto-discovers the cluster via broadcast first
func (c *Consumer) Connect() error {
	const (
		maxAttempts      = 10
		initialDelay     = 500 * time.Millisecond
		retryDelay       = 1 * time.Second
		halfOpenDelay    = 2 * time.Second
		failureThreshold = 5
	)

	// Auto-discover leader if not provided
	if c.leaderAddr == "" {
		fmt.Printf("[Consumer %s] No leader address provided, discovering via broadcast...\n", c.id[:8])
		leaderAddr, err := protocol.DiscoverLeader(nil)
		if err != nil {
			return fmt.Errorf("failed to discover cluster: %w", err)
		}
		c.leaderAddr = leaderAddr
		fmt.Printf("[Consumer %s] Discovered leader at %s\n", c.id[:8], c.leaderAddr)
	}

	time.Sleep(initialDelay)

	// Connect to leader for registration
	fmt.Printf("[Consumer %s] Connecting to leader at %s for registration...\n", c.id[:8], c.leaderAddr)

	failureCount := 0
	var leaderConn net.Conn
	var err error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		leaderConn, err = net.DialTimeout("tcp", c.leaderAddr, 5*time.Second)

		if err != nil {
			failureCount++
			if failureCount >= failureThreshold {
				time.Sleep(halfOpenDelay)
				leaderConn, err = net.DialTimeout("tcp", c.leaderAddr, 5*time.Second)
				if err == nil {
					break
				}
				failureCount++
			}
		} else {
			break
		}

		if attempt < maxAttempts {
			time.Sleep(retryDelay)
		}
	}

	if err != nil {
		return fmt.Errorf("failed to connect to leader after %d attempts: %w", maxAttempts, err)
	}

	// Determine local address for registration
	var localAddr string
	localIP := leaderConn.LocalAddr().(*net.TCPAddr).IP.String()

	// Override IP with advertised address if set (e.g. Windows host IP for WSL)
	if c.advertiseAddr != "" {
		localIP = c.advertiseAddr
	}

	if c.clientPort > 0 {
		localAddr = fmt.Sprintf("%s:%d", localIP, c.clientPort)
	} else {
		if c.advertiseAddr != "" {
			localPort := leaderConn.LocalAddr().(*net.TCPAddr).Port
			localAddr = fmt.Sprintf("%s:%d", localIP, localPort)
		} else {
			localAddr = leaderConn.LocalAddr().String()
		}
	}

	consumeMsg := protocol.NewConsumeMsg(c.id, c.topic, localAddr, 0)

	// Send CONSUME request to leader
	fmt.Printf("[Consumer %s] -> CONSUME (topic: %s)\n", c.id[:8], c.topic)
	if err := protocol.WriteTCPMessage(leaderConn, consumeMsg); err != nil {
		leaderConn.Close()
		return fmt.Errorf("failed to send CONSUME: %w", err)
	}

	// Read CONSUME_ACK response
	msg, err := protocol.ReadTCPMessage(leaderConn)
	if err != nil {
		leaderConn.Close()
		return fmt.Errorf("failed to read CONSUME_ACK: %w", err)
	}

	ack, ok := msg.(*protocol.ConsumeMsg)
	if !ok {
		leaderConn.Close()
		return fmt.Errorf("unexpected response type: %T", msg)
	}

	if ack.Topic == "" {
		leaderConn.Close()
		return fmt.Errorf("subscription failed - topic may not have a producer yet")
	}

	// Get the assigned broker address from the response
	c.brokerAddr = ack.ConsumerAddress // This now contains the assigned broker address
	fmt.Printf("[Consumer %s] <- CONSUME_ACK (topic: %s, assigned broker: %s)\n", c.id[:8], ack.Topic, c.brokerAddr)

	// Close leader connection - we're done with registration
	leaderConn.Close()

	// Connect to assigned broker for subscription
	fmt.Printf("[Consumer %s] Connecting to assigned broker at %s...\n", c.id[:8], c.brokerAddr)

	failureCount = 0
	var brokerConn net.Conn

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		brokerConn, err = net.DialTimeout("tcp", c.brokerAddr, 5*time.Second)

		if err != nil {
			failureCount++
			if failureCount >= failureThreshold {
				time.Sleep(halfOpenDelay)
				brokerConn, err = net.DialTimeout("tcp", c.brokerAddr, 5*time.Second)
				if err == nil {
					break
				}
				failureCount++
			}
		} else {
			break
		}

		if attempt < maxAttempts {
			time.Sleep(retryDelay)
		}
	}

	if err != nil {
		return fmt.Errorf("failed to connect to broker after %d attempts: %w", maxAttempts, err)
	}

	c.tcpConn = brokerConn

	// Get local address for subscription
	localAddr = brokerConn.LocalAddr().String()

	// Send SUBSCRIBE request to broker
	subscribeMsg := protocol.NewSubscribeMsg(c.id, c.topic, c.id, localAddr, c.enableProcessing, c.analyticsWindowSeconds, c.analyticsIntervalMs)

	fmt.Printf("[Consumer %s] -> SUBSCRIBE (topic: %s, processing: %v)\n", c.id[:8], c.topic, c.enableProcessing)
	if err := protocol.WriteTCPMessage(brokerConn, subscribeMsg); err != nil {
		brokerConn.Close()
		return fmt.Errorf("failed to send SUBSCRIBE: %w", err)
	}

	// Read SUBSCRIBE_ACK response (broker sends back a SUBSCRIBE message as ACK)
	msg, err = protocol.ReadTCPMessage(brokerConn)
	if err != nil {
		brokerConn.Close()
		return fmt.Errorf("failed to read SUBSCRIBE_ACK: %w", err)
	}

	subAck, ok := msg.(*protocol.SubscribeMsg)
	if !ok {
		brokerConn.Close()
		return fmt.Errorf("unexpected response type: %T", msg)
	}

	if subAck.Topic == "" {
		brokerConn.Close()
		return fmt.Errorf("broker subscription failed")
	}

	fmt.Printf("[Consumer %s] <- SUBSCRIBE_ACK (subscribed to broker for topic: %s)\n", c.id[:8], subAck.Topic)

	// Save local address for potential reconnection
	c.localAddr = localAddr

	// Start TCP listener for incoming messages (like REASSIGN_BROKER) from leader
	// Always bind to 0.0.0.0:port (not the advertised IP which may not be local, e.g. WSL)
	if c.clientPort > 0 {
		// Use fixed port for the listener
		listenAddr := fmt.Sprintf("0.0.0.0:%d", c.clientPort)
		c.tcpListener, err = net.Listen("tcp", listenAddr)
		if err != nil {
			fmt.Printf("[Consumer %s] Warning: failed to start TCP listener on %s: %v\n", c.id[:8], listenAddr, err)
		}
	} else {
		localTCPAddr, err := net.ResolveTCPAddr("tcp", c.localAddr)
		if err == nil {
			c.tcpListener, err = net.ListenTCP("tcp", localTCPAddr)
			if err != nil {
				// Try with any available port if the original port is in use
				c.tcpListener, err = net.Listen("tcp", ":0")
			}
		}
	}
	if c.tcpListener != nil {
		fmt.Printf("[Consumer %s] TCP listener started on %s\n", c.id[:8], c.tcpListener.Addr().String())
		c.wg.Add(1)
		go c.listenForMessages()
	}

	// Start reconnection handler
	c.wg.Add(1)
	go c.handleReconnections()

	// Start receiving results from broker in background goroutine
	c.wg.Add(1) // Register goroutine before starting
	go c.receiveResults()

	return nil
}

func (c *Consumer) receiveResults() {
	defer c.wg.Done() // Signal completion when goroutine exits

	for {
		select {
		case <-c.stopSignal:
			return

		default:
			c.tcpConnMu.Lock()
			conn := c.tcpConn
			c.tcpConnMu.Unlock()

			if conn == nil {
				// Wait a bit and check again - connection might be in process of reconnecting
				time.Sleep(100 * time.Millisecond)
				continue
			}

			msg, err := protocol.ReadTCPMessage(conn)
			if err != nil {
				errStr := err.Error()

				isEOF := false
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					isEOF = true
				}

				if !isEOF && errStr != "" {
					if errStr == "EOF" ||
						errStr == "failed to read size: EOF" ||
						errStr == "failed to read message: EOF" ||
						errStr == "failed to read result: failed to read size: EOF" ||
						(len(errStr) >= 3 && errStr[len(errStr)-3:] == "EOF") ||
						(len(errStr) >= 5 && errStr[len(errStr)-5:] == ": EOF") {
						isEOF = true
					}
				}

				if isEOF {
					select {
					case c.errors <- fmt.Errorf("connection closed by server"):
					default:
					}

					// Attempt cluster re-discovery instead of giving up
					fmt.Printf("[Consumer %s] Connection lost, waiting for cluster to stabilize...\n", c.id[:8])

					// Wait for cluster to elect new leader (election takes ~10-15s)
					select {
					case <-time.After(5 * time.Second):
					case <-c.stopSignal:
						return
					}

					fmt.Printf("[Consumer %s] Attempting cluster reconnection...\n", c.id[:8])
					if err := c.reconnectToCluster(); err != nil {
						select {
						case c.errors <- fmt.Errorf("cluster reconnection failed: %w", err):
						default:
						}
						return // Give up after all retries exhausted
					}

					fmt.Printf("[Consumer %s] Successfully reconnected, resuming message reception\n", c.id[:8])
					continue // Resume receiving on new connection
				}

				var netErr net.Error
				if errors.As(err, &netErr) && netErr.Timeout() {
					continue
				}

				select {
				case c.errors <- fmt.Errorf("failed to read result: %w", err):
				default:
				}

				time.Sleep(1 * time.Second)
				continue
			}

			// Type assert to RESULT message
			resultMsg, ok := msg.(*protocol.ResultMsg)
			if !ok {
				select {
				case c.errors <- fmt.Errorf("unexpected message type: %T", msg):
				default:
				}
				continue
			}

			// Send result to channel
			select {
			case c.results <- resultMsg.ResultMessage:
			case <-c.stopSignal:
				return
			}
		}
	}
}

// reconnectToCluster re-discovers the cluster leader and re-registers the consumer.
// Called when the broker connection is lost (leader or broker died).
func (c *Consumer) reconnectToCluster() error {
	const maxAttempts = 5
	const attemptDelay = 5 * time.Second

	// Close existing broker connection
	c.tcpConnMu.Lock()
	if c.tcpConn != nil {
		c.tcpConn.Close()
		c.tcpConn = nil
	}
	c.tcpConnMu.Unlock()

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		fmt.Printf("[Consumer %s] Reconnection attempt %d/%d...\n", c.id[:8], attempt, maxAttempts)

		// Check if we should stop
		select {
		case <-c.stopSignal:
			return fmt.Errorf("consumer stopped during reconnection")
		default:
		}

		// Wait before retrying to give cluster time to elect new leader
		if attempt > 1 {
			select {
			case <-time.After(attemptDelay):
			case <-c.stopSignal:
				return fmt.Errorf("consumer stopped during reconnection")
			}
		}

		// Discover new leader via broadcast
		leaderAddr, err := protocol.DiscoverLeader(nil)
		if err != nil {
			log.Printf("[Consumer %s] Discovery failed: %v", c.id[:8], err)
			continue
		}

		fmt.Printf("[Consumer %s] Discovered leader at %s\n", c.id[:8], leaderAddr)

		// Register with new leader (CONSUME)
		leaderConn, err := net.DialTimeout("tcp", leaderAddr, 5*time.Second)
		if err != nil {
			log.Printf("[Consumer %s] Failed to connect to leader: %v", c.id[:8], err)
			continue
		}

		var localAddr string
		reconnIP := leaderConn.LocalAddr().(*net.TCPAddr).IP.String()
		if c.advertiseAddr != "" {
			reconnIP = c.advertiseAddr
		}
		if c.clientPort > 0 {
			localAddr = fmt.Sprintf("%s:%d", reconnIP, c.clientPort)
		} else {
			localAddr = leaderConn.LocalAddr().String()
		}
		consumeMsg := protocol.NewConsumeMsg(c.id, c.topic, localAddr, 0)

		fmt.Printf("[Consumer %s] -> CONSUME to new leader (topic: %s)\n", c.id[:8], c.topic)
		if err := protocol.WriteTCPMessage(leaderConn, consumeMsg); err != nil {
			leaderConn.Close()
			log.Printf("[Consumer %s] Failed to send CONSUME: %v", c.id[:8], err)
			continue
		}

		msg, err := protocol.ReadTCPMessage(leaderConn)
		leaderConn.Close()
		if err != nil {
			log.Printf("[Consumer %s] Failed to read CONSUME_ACK: %v", c.id[:8], err)
			continue
		}

		ack, ok := msg.(*protocol.ConsumeMsg)
		if !ok {
			log.Printf("[Consumer %s] Unexpected response type: %T", c.id[:8], msg)
			continue
		}

		if ack.Topic == "" {
			log.Printf("[Consumer %s] Subscription failed - topic may not have a producer yet", c.id[:8])
			continue
		}

		newBrokerAddr := ack.ConsumerAddress
		fmt.Printf("[Consumer %s] <- CONSUME_ACK (new broker: %s)\n", c.id[:8], newBrokerAddr)

		// Update broker address
		c.brokerAddrMu.Lock()
		c.brokerAddr = newBrokerAddr
		c.brokerAddrMu.Unlock()

		// Connect to new broker
		brokerConn, err := net.DialTimeout("tcp", newBrokerAddr, 5*time.Second)
		if err != nil {
			log.Printf("[Consumer %s] Failed to connect to broker: %v", c.id[:8], err)
			continue
		}

		// Subscribe to new broker
		brokerLocalAddr := brokerConn.LocalAddr().String()
		subscribeMsg := protocol.NewSubscribeMsg(c.id, c.topic, c.id, brokerLocalAddr, c.enableProcessing, c.analyticsWindowSeconds, c.analyticsIntervalMs)

		fmt.Printf("[Consumer %s] -> SUBSCRIBE to new broker (topic: %s)\n", c.id[:8], c.topic)
		if err := protocol.WriteTCPMessage(brokerConn, subscribeMsg); err != nil {
			brokerConn.Close()
			log.Printf("[Consumer %s] Failed to send SUBSCRIBE: %v", c.id[:8], err)
			continue
		}

		// Read SUBSCRIBE_ACK
		msg, err = protocol.ReadTCPMessage(brokerConn)
		if err != nil {
			brokerConn.Close()
			log.Printf("[Consumer %s] Failed to read SUBSCRIBE_ACK: %v", c.id[:8], err)
			continue
		}

		subAck, ok := msg.(*protocol.SubscribeMsg)
		if !ok {
			brokerConn.Close()
			log.Printf("[Consumer %s] Unexpected response: %T", c.id[:8], msg)
			continue
		}

		if subAck.Topic == "" {
			brokerConn.Close()
			log.Printf("[Consumer %s] Broker subscription failed", c.id[:8])
			continue
		}

		// Update connection atomically
		c.tcpConnMu.Lock()
		c.tcpConn = brokerConn
		c.tcpConnMu.Unlock()

		// Update leader address
		c.leaderAddr = leaderAddr

		fmt.Printf("[Consumer %s] <- SUBSCRIBE_ACK (reconnected to broker for topic: %s)\n", c.id[:8], subAck.Topic)
		fmt.Printf("[Consumer %s] Reconnected: leader=%s, broker=%s\n", c.id[:8], leaderAddr, newBrokerAddr)
		return nil
	}

	return fmt.Errorf("failed to reconnect after %d attempts", maxAttempts)
}

// Results returns the channel for receiving result messages
func (c *Consumer) Results() <-chan *protocol.ResultMessage {
	return c.results
}

// Errors returns the channel for receiving errors
func (c *Consumer) Errors() <-chan error {
	return c.errors
}

// listenForMessages listens for incoming TCP messages from the leader
// Handles REASSIGN_BROKER messages for broker failover
func (c *Consumer) listenForMessages() {
	defer c.wg.Done()

	for {
		select {
		case <-c.stopListener:
			return
		default:
			if c.tcpListener == nil {
				return
			}

			// Set accept timeout to allow checking stop signal
			if tcpListener, ok := c.tcpListener.(*net.TCPListener); ok {
				tcpListener.SetDeadline(time.Now().Add(1 * time.Second))
			}

			conn, err := c.tcpListener.Accept()
			if err != nil {
				// Check if it's a timeout (expected) or actual error
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				// Check if listener was closed
				select {
				case <-c.stopListener:
					return
				default:
					continue
				}
			}

			// Handle the connection in a goroutine
			go c.handleIncomingConnection(conn)
		}
	}
}

// handleIncomingConnection handles an incoming TCP connection from the leader
func (c *Consumer) handleIncomingConnection(conn net.Conn) {
	defer conn.Close()

	// Set read deadline
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	msg, err := protocol.ReadTCPMessage(conn)
	if err != nil {
		return
	}

	switch m := msg.(type) {
	case *protocol.ReassignBrokerMsg:
		fmt.Printf("[Consumer %s] <- REASSIGN_BROKER (topic: %s, new broker: %s)\n",
			c.id[:8], m.Topic, m.NewBrokerAddress)

		// Signal reconnection to new broker
		select {
		case c.reconnectSignal <- m.NewBrokerAddress:
		default:
			// Channel full, skip
		}

	case *protocol.HeartbeatMsg:
		// Heartbeat from leader - just acknowledge
		fmt.Printf("[Consumer %s] <- HEARTBEAT from leader\n", c.id[:8])

	default:
		// Unknown message type
	}
}

// handleReconnections handles broker reconnection requests
func (c *Consumer) handleReconnections() {
	defer c.wg.Done()

	for {
		select {
		case <-c.stopSignal:
			return
		case newBrokerAddr := <-c.reconnectSignal:
			fmt.Printf("[Consumer %s] Reconnecting to new broker: %s\n", c.id[:8], newBrokerAddr)
			if err := c.Reconnect(newBrokerAddr); err != nil {
				select {
				case c.errors <- fmt.Errorf("reconnection failed: %w", err):
				default:
				}
			}
		}
	}
}

// Reconnect reconnects to a new broker after receiving REASSIGN_BROKER
func (c *Consumer) Reconnect(newBrokerAddr string) error {
	// Close existing broker connection
	c.tcpConnMu.Lock()
	if c.tcpConn != nil {
		c.tcpConn.Close()
		c.tcpConn = nil
	}
	c.tcpConnMu.Unlock()

	// Update broker address
	c.brokerAddrMu.Lock()
	c.brokerAddr = newBrokerAddr
	c.brokerAddrMu.Unlock()

	// Connect to new broker
	const maxAttempts = 5
	const retryDelay = 1 * time.Second

	var brokerConn net.Conn
	var err error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		brokerConn, err = net.DialTimeout("tcp", newBrokerAddr, 5*time.Second)
		if err == nil {
			break
		}
		if attempt < maxAttempts {
			time.Sleep(retryDelay)
		}
	}

	if err != nil {
		return fmt.Errorf("failed to connect to new broker after %d attempts: %w", maxAttempts, err)
	}

	c.tcpConnMu.Lock()
	c.tcpConn = brokerConn
	c.tcpConnMu.Unlock()

	// Get local address for subscription
	localAddr := brokerConn.LocalAddr().String()

	// Send SUBSCRIBE request to new broker
	subscribeMsg := protocol.NewSubscribeMsg(c.id, c.topic, c.id, localAddr, c.enableProcessing, c.analyticsWindowSeconds, c.analyticsIntervalMs)

	fmt.Printf("[Consumer %s] -> SUBSCRIBE to new broker (topic: %s)\n", c.id[:8], c.topic)
	if err := protocol.WriteTCPMessage(brokerConn, subscribeMsg); err != nil {
		c.tcpConnMu.Lock()
		if c.tcpConn != nil {
			c.tcpConn.Close()
			c.tcpConn = nil
		}
		c.tcpConnMu.Unlock()
		return fmt.Errorf("failed to send SUBSCRIBE: %w", err)
	}

	// Read SUBSCRIBE_ACK response
	msg, err := protocol.ReadTCPMessage(brokerConn)
	if err != nil {
		c.tcpConnMu.Lock()
		if c.tcpConn != nil {
			c.tcpConn.Close()
			c.tcpConn = nil
		}
		c.tcpConnMu.Unlock()
		return fmt.Errorf("failed to read SUBSCRIBE_ACK: %w", err)
	}

	subAck, ok := msg.(*protocol.SubscribeMsg)
	if !ok {
		c.tcpConnMu.Lock()
		if c.tcpConn != nil {
			c.tcpConn.Close()
			c.tcpConn = nil
		}
		c.tcpConnMu.Unlock()
		return fmt.Errorf("unexpected response type: %T", msg)
	}

	if subAck.Topic == "" {
		c.tcpConnMu.Lock()
		if c.tcpConn != nil {
			c.tcpConn.Close()
			c.tcpConn = nil
		}
		c.tcpConnMu.Unlock()
		return fmt.Errorf("broker subscription failed")
	}

	fmt.Printf("[Consumer %s] <- SUBSCRIBE_ACK (reconnected to broker for topic: %s)\n", c.id[:8], subAck.Topic)
	return nil
}

// ID returns the consumer's unique identifier
func (c *Consumer) ID() string {
	return c.id
}

// GetBrokerAddress returns the current broker address
func (c *Consumer) GetBrokerAddress() string {
	c.brokerAddrMu.RLock()
	defer c.brokerAddrMu.RUnlock()
	return c.brokerAddr
}

// Close shuts down the consumer
func (c *Consumer) Close() {
	close(c.stopSignal)
	close(c.stopListener)

	// Close TCP listener to unblock Accept()
	if c.tcpListener != nil {
		c.tcpListener.Close()
	}

	// Close broker connection
	c.tcpConnMu.Lock()
	if c.tcpConn != nil {
		c.tcpConn.Close()
		c.tcpConn = nil
	}
	c.tcpConnMu.Unlock()

	// Wait for goroutines to actually exit (proper synchronization)
	c.wg.Wait()

	close(c.results)
	close(c.errors)
}
