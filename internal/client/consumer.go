package client

import (
	"errors"
	"fmt"
	"io"
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
	tcpConn          net.Conn // Connection to the assigned broker
	results          chan *protocol.ResultMessage
	errors           chan error
	stopSignal       chan struct{}
	enableProcessing bool // Whether to request data processing from broker
	wg               sync.WaitGroup // Synchronize goroutine lifecycle
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
		enableProcessing: true, // Enable data processing by default
	}
}

// NewConsumerWithOptions creates a new consumer with options
// If leaderAddr is empty, auto-discovery via broadcast will be used during Connect()
func NewConsumerWithOptions(topic, leaderAddr string, enableProcessing bool) *Consumer {
	c := NewConsumer(topic, leaderAddr)
	c.enableProcessing = enableProcessing
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

	// Get local address for registration
	localAddr := leaderConn.LocalAddr().String()

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
	subscribeMsg := protocol.NewSubscribeMsg(c.id, c.topic, c.id, localAddr, c.enableProcessing)

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

	// Start receiving results from broker in background goroutine
	c.wg.Add(1) // Register goroutine before starting
	go c.receiveResults()

	return nil
}

func (c *Consumer) receiveResults() {
	defer c.wg.Done() // Signal completion when goroutine exits
	defer func() {
		if c.tcpConn != nil {
			c.tcpConn.Close()
		}
	}()

	for {
		select {
		case <-c.stopSignal:
			return

		default:
			if c.tcpConn == nil {
				select {
				case c.errors <- fmt.Errorf("connection closed"):
				default:
				}
				return
			}

			msg, err := protocol.ReadTCPMessage(c.tcpConn)
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
					return
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

// Results returns the channel for receiving result messages
func (c *Consumer) Results() <-chan *protocol.ResultMessage {
	return c.results
}

// Errors returns the channel for receiving errors
func (c *Consumer) Errors() <-chan error {
	return c.errors
}

// Close shuts down the consumer
func (c *Consumer) Close() {
	close(c.stopSignal)
	
	// Wait for goroutine to actually exit (proper synchronization)
	c.wg.Wait()
	
	if c.tcpConn != nil {
		c.tcpConn.Close()
		c.tcpConn = nil
	}
	close(c.results)
	close(c.errors)
}
