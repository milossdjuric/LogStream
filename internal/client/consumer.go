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
	id                     string
	topic                  string
	leaderAddr             string
	brokerAddr             string // Assigned broker address (from CONSUME_ACK)
	brokerAddrMu           sync.RWMutex
	tcpConn                net.Conn // Connection to the assigned broker
	tcpConnMu              sync.Mutex
	tcpListener            net.Listener // TCP listener for incoming messages from leader
	localAddr              string       // Local address used for registration
	results                chan *protocol.ResultMessage
	stopSignal             chan struct{}
	stopListener           chan struct{}
	enableProcessing       bool           // Whether to request data processing from broker
	analyticsWindowSeconds int32          // 0 = use broker default
	analyticsIntervalMs    int32          // 0 = use broker default
	clientPort             int            // Fixed TCP listener port (0 = OS picks)
	advertiseAddr          string         // Override advertised IP (e.g. Windows host IP for WSL)
	wg                     sync.WaitGroup // Synchronize goroutine lifecycle
	closeOnce              sync.Once      // Ensures Close() is safe to call multiple times

	// Configurable timing
	heartbeatInterval time.Duration
	reconnectAttempts int
	reconnectDelay    time.Duration

	// UDP heartbeat state (consumer sends heartbeats to leader to stay alive)
	udpConn       *net.UDPConn
	leaderAddrUDP *net.UDPAddr
	stopHeartbeat chan struct{}
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
		id:                protocol.GenerateClientID("consumer", idSeed),
		topic:             topic,
		leaderAddr:        leaderAddr,
		results:           make(chan *protocol.ResultMessage, 100),
		stopSignal:        make(chan struct{}),
		stopListener:      make(chan struct{}),
		stopHeartbeat:     make(chan struct{}),
		enableProcessing:  true, // Enable data processing by default
		heartbeatInterval: 2 * time.Second,
		reconnectAttempts: 10,
		reconnectDelay:    5 * time.Second,
	}
}

// ConsumerOptions holds all configurable consumer options
type ConsumerOptions struct {
	EnableProcessing       bool
	AnalyticsWindowSeconds int32         // 0 = use broker default
	AnalyticsIntervalMs    int32         // 0 = use broker default
	ClientPort             int           // Fixed TCP listener port (0 = OS picks)
	AdvertiseAddr          string        // Override advertised IP (e.g. Windows host IP for WSL)
	HeartbeatInterval      time.Duration // How often to send heartbeats to leader (default: 2s)
	ReconnectAttempts      int           // Max reconnection attempts (default: 10)
	ReconnectDelay         time.Duration // Delay between reconnection attempts (default: 5s)
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
	if opts.HeartbeatInterval > 0 {
		c.heartbeatInterval = opts.HeartbeatInterval
	}
	if opts.ReconnectAttempts > 0 {
		c.reconnectAttempts = opts.ReconnectAttempts
	}
	if opts.ReconnectDelay > 0 {
		c.reconnectDelay = opts.ReconnectDelay
	}
	return c
}

// Connect registers with the leader and then subscribes to the assigned broker
// If no leader address was provided, auto-discovers the cluster via broadcast first
func (c *Consumer) Connect() error {
	const (
		maxAttempts   = 15
		initialDelay  = 500 * time.Millisecond
		retryDelay    = 2 * time.Second
		halfOpenDelay = 5 * time.Second
	)

	// Auto-discover leader if not provided
	if c.leaderAddr == "" {
		fmt.Printf("[Consumer %s] No leader address provided, discovering via broadcast...\n", c.id[:8])
		discoveryConfig := protocol.DefaultBroadcastConfig()
		discoveryConfig.DiscoveryPort = c.clientPort
		leaderAddr, err := protocol.DiscoverLeader(discoveryConfig)
		if err != nil {
			return fmt.Errorf("failed to discover cluster: %w", err)
		}
		c.leaderAddr = leaderAddr
		fmt.Printf("[Consumer %s] Discovered leader at %s\n", c.id[:8], c.leaderAddr)
	}

	time.Sleep(initialDelay)

	// Phase 1: Registration with circuit breaker retry.
	// Handles transient failures like leader frozen during view change.
	regDelay := retryDelay
	var lastErr error

	for regAttempt := 1; regAttempt <= maxAttempts; regAttempt++ {
		lastErr = c.attemptRegistration()
		if lastErr == nil {
			break
		}
		fmt.Printf("[Consumer %s] Registration attempt %d/%d failed: %v\n",
			c.id[:8], regAttempt, maxAttempts, lastErr)
		if regAttempt < maxAttempts {
			fmt.Printf("[Consumer %s] Retrying in %v...\n", c.id[:8], regDelay)
			time.Sleep(regDelay)
			regDelay = time.Duration(float64(regDelay) * 1.5)
			if regDelay > halfOpenDelay {
				regDelay = halfOpenDelay
			}
		}
	}

	if lastErr != nil {
		return fmt.Errorf("registration failed after %d attempts: %w", maxAttempts, lastErr)
	}

	fmt.Printf("[Consumer %s] <- CONSUME_ACK (assigned broker: %s)\n", c.id[:8], c.brokerAddr)

	// Phase 2: Broker subscription with circuit breaker retry.
	// Handles transient failures like broker not yet having stream assignment.
	subDelay := retryDelay

	for subAttempt := 1; subAttempt <= maxAttempts; subAttempt++ {
		lastErr = c.attemptSubscription()
		if lastErr == nil {
			break
		}
		fmt.Printf("[Consumer %s] Subscription attempt %d/%d failed: %v\n",
			c.id[:8], subAttempt, maxAttempts, lastErr)
		if subAttempt < maxAttempts {
			fmt.Printf("[Consumer %s] Retrying in %v...\n", c.id[:8], subDelay)
			time.Sleep(subDelay)
			subDelay = time.Duration(float64(subDelay) * 1.5)
			if subDelay > halfOpenDelay {
				subDelay = halfOpenDelay
			}
		}
	}

	if lastErr != nil {
		return fmt.Errorf("subscription failed after %d attempts: %w", maxAttempts, lastErr)
	}

	fmt.Printf("[Consumer %s] <- SUBSCRIBE_ACK (subscribed to broker for topic: %s)\n", c.id[:8], c.topic)

	// Create UDP socket for heartbeats (bind to clientPort so leader can send heartbeats back)
	udpPort := 0
	if c.clientPort > 0 {
		udpPort = c.clientPort
	}
	var err error
	c.udpConn, err = net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: udpPort})
	if err != nil {
		return fmt.Errorf("failed to create UDP socket: %w", err)
	}
	c.leaderAddrUDP, _ = net.ResolveUDPAddr("udp", c.leaderAddr)

	// Start UDP heartbeat goroutine (consumer → leader, keeps consumer alive on leader side)
	c.wg.Add(1)
	go c.sendHeartbeats()

	// Start TCP listener for incoming messages (like REASSIGN_BROKER) from leader
	// Always bind to 0.0.0.0:port (not the advertised IP which may not be local, e.g. WSL)
	if c.clientPort > 0 {
		listenAddr := fmt.Sprintf("0.0.0.0:%d", c.clientPort)
		c.tcpListener, err = net.Listen("tcp", listenAddr)
		if err != nil {
			fmt.Printf("[Consumer %s] Warning: failed to start TCP listener on %s: %v\n", c.id[:8], listenAddr, err)
		}
	} else {
		c.tcpListener, err = net.Listen("tcp", ":0")
		if err != nil {
			fmt.Printf("[Consumer %s] Warning: failed to start TCP listener: %v\n", c.id[:8], err)
		}
	}
	if c.tcpListener != nil {
		fmt.Printf("[Consumer %s] TCP listener started on %s\n", c.id[:8], c.tcpListener.Addr().String())
		c.wg.Add(1)
		go c.listenForMessages()
	}

	// Start receiving results from broker in background goroutine
	c.wg.Add(1)
	go c.receiveResults()

	return nil
}

// attemptRegistration performs a single registration attempt with the leader.
// Connects via TCP, sends CONSUME, reads CONSUME_ACK, and sets c.brokerAddr on success.
func (c *Consumer) attemptRegistration() error {
	conn, err := net.DialTimeout("tcp", c.leaderAddr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to leader: %w", err)
	}
	defer conn.Close()

	// Determine local address for registration
	localIP := conn.LocalAddr().(*net.TCPAddr).IP.String()
	if c.advertiseAddr != "" {
		localIP = c.advertiseAddr
	}

	var localAddr string
	if c.clientPort > 0 {
		localAddr = fmt.Sprintf("%s:%d", localIP, c.clientPort)
	} else if c.advertiseAddr != "" {
		localPort := conn.LocalAddr().(*net.TCPAddr).Port
		localAddr = fmt.Sprintf("%s:%d", localIP, localPort)
	} else {
		localAddr = conn.LocalAddr().String()
	}

	consumeMsg := protocol.NewConsumeMsg(c.id, c.topic, localAddr, 0)

	fmt.Printf("[Consumer %s] -> CONSUME (topic: %s, addr: %s)\n", c.id[:8], c.topic, localAddr)
	if err := protocol.WriteTCPMessage(conn, consumeMsg); err != nil {
		return fmt.Errorf("failed to send CONSUME: %w", err)
	}

	msg, err := protocol.ReadTCPMessage(conn)
	if err != nil {
		return fmt.Errorf("failed to read CONSUME_ACK: %w", err)
	}

	ack, ok := msg.(*protocol.ConsumeMsg)
	if !ok {
		return fmt.Errorf("unexpected response type: %T", msg)
	}

	if ack.Topic == "" {
		return fmt.Errorf("no broker assigned (leader may be temporarily unavailable)")
	}

	c.brokerAddrMu.Lock()
	c.brokerAddr = ack.ConsumerAddress
	c.brokerAddrMu.Unlock()
	return nil
}

// attemptSubscription performs a single subscription attempt with the assigned broker.
// Connects via TCP, sends SUBSCRIBE, reads SUBSCRIBE_ACK, and sets c.tcpConn on success.
func (c *Consumer) attemptSubscription() error {
	c.brokerAddrMu.RLock()
	brokerAddr := c.brokerAddr
	c.brokerAddrMu.RUnlock()

	brokerConn, err := net.DialTimeout("tcp", brokerAddr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to broker: %w", err)
	}

	localAddr := brokerConn.LocalAddr().String()
	subscribeMsg := protocol.NewSubscribeMsg(c.id, c.topic, c.id, localAddr, c.enableProcessing, c.analyticsWindowSeconds, c.analyticsIntervalMs)

	fmt.Printf("[Consumer %s] -> SUBSCRIBE (topic: %s, processing: %v)\n", c.id[:8], c.topic, c.enableProcessing)
	if err := protocol.WriteTCPMessage(brokerConn, subscribeMsg); err != nil {
		brokerConn.Close()
		return fmt.Errorf("failed to send SUBSCRIBE: %w", err)
	}

	msg, err := protocol.ReadTCPMessage(brokerConn)
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
		return fmt.Errorf("broker subscription failed (broker may not have assignment yet)")
	}

	// Success - set connection and save local address
	c.tcpConnMu.Lock()
	c.tcpConn = brokerConn
	c.tcpConnMu.Unlock()
	c.localAddr = localAddr

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

			// Set read deadline to detect broker death faster
			// If broker dies, read will timeout instead of blocking indefinitely
			conn.SetReadDeadline(time.Now().Add(5 * time.Second))

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
					log.Printf("[Consumer %s] Connection closed by server (EOF)", c.id[:8])

					// Close broken connection
					c.tcpConnMu.Lock()
					if c.tcpConn != nil {
						c.tcpConn.Close()
						c.tcpConn = nil
					}
					c.tcpConnMu.Unlock()

					// Wait for REASSIGN_BROKER from leader before attempting re-discovery.
					// The leader checks broker timeouts every 5s with a 30s threshold,
					// so REASSIGN_BROKER should arrive within ~35s if the leader is alive.
					// Exponential backoff: 2s, 4s, 8s, 16s = 30s total wait.
					fmt.Printf("[Consumer %s] Connection lost, waiting for REASSIGN_BROKER from leader...\n", c.id[:8])

					backoff := 2 * time.Second
					maxBackoff := 16 * time.Second
					totalWait := time.Duration(0)
					maxTotalWait := 35 * time.Second

					restored := false
					for totalWait < maxTotalWait {
						select {
						case <-time.After(backoff):
						case <-c.stopSignal:
							return
						}
						totalWait += backoff

						// Check if REASSIGN_BROKER restored the connection via Reconnect()
						c.tcpConnMu.Lock()
						restored = c.tcpConn != nil
						c.tcpConnMu.Unlock()

						if restored {
							fmt.Printf("[Consumer %s] Connection restored by REASSIGN_BROKER, resuming\n", c.id[:8])
							break
						}

						fmt.Printf("[Consumer %s] Still waiting for REASSIGN_BROKER (%.0fs elapsed)...\n",
							c.id[:8], totalWait.Seconds())

						backoff *= 2
						if backoff > maxBackoff {
							backoff = maxBackoff
						}
					}

					if restored {
						continue // Resume receiving on new connection
					}

					// REASSIGN_BROKER never arrived - leader is probably dead too.
					// Fall back to full cluster re-discovery.
					fmt.Printf("[Consumer %s] No REASSIGN_BROKER received after %.0fs, attempting cluster re-discovery...\n",
						c.id[:8], totalWait.Seconds())
					if err := c.reconnectToCluster(); err != nil {
						log.Printf("[Consumer %s] Cluster reconnection failed: %v", c.id[:8], err)
						return // Give up after all retries exhausted
					}

					fmt.Printf("[Consumer %s] Successfully reconnected, resuming message reception\n", c.id[:8])
					continue // Resume receiving on new connection
				}

				var netErr net.Error
				if errors.As(err, &netErr) && netErr.Timeout() {
					continue
				}

				log.Printf("[Consumer %s] Failed to read result: %v", c.id[:8], err)

				time.Sleep(1 * time.Second)
				continue
			}

			// Type assert to RESULT message
			resultMsg, ok := msg.(*protocol.ResultMsg)
			if !ok {
				log.Printf("[Consumer %s] Unexpected message type: %T", c.id[:8], msg)
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

// sendHeartbeats sends periodic heartbeats to leader via UDP
func (c *Consumer) sendHeartbeats() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			heartbeat := protocol.NewHeartbeatMsg(c.id, protocol.NodeType_CONSUMER, 0, 0, "")
			if c.udpConn != nil && c.leaderAddrUDP != nil {
				protocol.WriteUDPMessage(c.udpConn, heartbeat, c.leaderAddrUDP)
			}
		case <-c.stopHeartbeat:
			return
		}
	}
}

// reconnectToCluster re-discovers the cluster leader and re-registers the consumer.
// Called when the broker connection is lost (leader or broker died).
func (c *Consumer) reconnectToCluster() error {
	maxAttempts := c.reconnectAttempts
	attemptDelay := c.reconnectDelay

	// Close existing broker connection
	c.tcpConnMu.Lock()
	if c.tcpConn != nil {
		c.tcpConn.Close()
		c.tcpConn = nil
	}
	c.tcpConnMu.Unlock()

	// Close existing UDP socket before discovery to free the port
	// DiscoverLeader needs to bind broadcast sockets to clientPort,
	// which conflicts with the existing udpConn on the same port
	if c.udpConn != nil {
		c.udpConn.Close()
		c.udpConn = nil
	}

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
		discoveryConfig := protocol.DefaultBroadcastConfig()
		discoveryConfig.DiscoveryPort = c.clientPort
		leaderAddr, err := protocol.DiscoverLeader(discoveryConfig)
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

		// Re-create UDP socket (was closed before discovery to free the port)
		udpPort := 0
		if c.clientPort > 0 {
			udpPort = c.clientPort
		}
		newUDPConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: udpPort})
		if err != nil {
			log.Printf("[Consumer %s] Warning: Failed to re-create UDP socket: %v", c.id[:8], err)
			// Continue anyway - consumer can still receive data via TCP from broker
		} else {
			c.udpConn = newUDPConn
		}

		// Update leader address and UDP heartbeat target
		c.leaderAddr = leaderAddr
		c.leaderAddrUDP, _ = net.ResolveUDPAddr("udp", leaderAddr)

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

		// Reconnect directly to new broker (same pattern as producer's UpdateBrokerAddress)
		if err := c.Reconnect(m.NewBrokerAddress); err != nil {
			log.Printf("[Consumer %s] Reconnection to new broker failed: %v", c.id[:8], err)
		}

	default:
		// Unknown message type
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
	const maxAttempts = 10
	const retryDelay = 2 * time.Second

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

// Close shuts down the consumer. Safe to call multiple times.
func (c *Consumer) Close() {
	c.closeOnce.Do(func() {
		// Signal all goroutines to stop
		close(c.stopSignal)
		close(c.stopListener)
		close(c.stopHeartbeat)

		// Close TCP listener to unblock Accept()
		if c.tcpListener != nil {
			c.tcpListener.Close()
		}

		// Close broker connection to unblock ReadTCPMessage()
		c.tcpConnMu.Lock()
		if c.tcpConn != nil {
			c.tcpConn.Close()
			c.tcpConn = nil
		}
		c.tcpConnMu.Unlock()

		// Close UDP socket to free the port immediately
		if c.udpConn != nil {
			c.udpConn.Close()
		}

		// Wait for goroutines with timeout to prevent hanging
		done := make(chan struct{})
		go func() {
			c.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			fmt.Printf("[Consumer %s] Shutdown timeout - forcing exit\n", c.id[:8])
		}

		close(c.results)
	})
}
