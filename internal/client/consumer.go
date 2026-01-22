package client

import (
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/milossdjuric/logstream/internal/protocol"
)

// Consumer handles consuming data from LogStream
type Consumer struct {
	id         string
	topic      string
	leaderAddr string
	tcpConn    net.Conn
	results    chan *protocol.ResultMessage
	errors     chan error
	stopSignal chan struct{}
}

// NewConsumer creates a new consumer
func NewConsumer(topic, leaderAddr string) *Consumer {
	return &Consumer{
		id:         protocol.GenerateNodeID(fmt.Sprintf("consumer-%d", time.Now().UnixNano())),
		topic:      topic,
		leaderAddr: leaderAddr,
		results:    make(chan *protocol.ResultMessage, 100),
		errors:     make(chan error, 10),
		stopSignal: make(chan struct{}),
	}
}

// Connect registers with the leader and subscribes to topic
func (c *Consumer) Connect() error {
	const (
		maxAttempts      = 10
		initialDelay     = 500 * time.Millisecond
		retryDelay       = 1 * time.Second
		halfOpenDelay    = 2 * time.Second
		failureThreshold = 5
	)

	failureCount := 0
	var conn net.Conn
	var err error

	time.Sleep(initialDelay)

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		conn, err = net.DialTimeout("tcp", c.leaderAddr, 5*time.Second)

		if err != nil {
			failureCount++
			if failureCount >= failureThreshold {
				time.Sleep(halfOpenDelay)
				conn, err = net.DialTimeout("tcp", c.leaderAddr, 5*time.Second)
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

	c.tcpConn = conn

	// Get local address for registration
	localAddr := conn.LocalAddr().String()

	consumeMsg := protocol.NewConsumeMsg(c.id, c.topic, localAddr, 0)

	// Send CONSUME request
	fmt.Printf("[Consumer %s] -> CONSUME (topic: %s)\n", c.id[:8], c.topic)
	if err := protocol.WriteTCPMessage(conn, consumeMsg); err != nil {
		return fmt.Errorf("failed to send CONSUME: %w", err)
	}

	// Read CONSUME_ACK response
	msg, err := protocol.ReadTCPMessage(conn)
	if err != nil {
		return fmt.Errorf("failed to read CONSUME_ACK: %w", err)
	}

	ack, ok := msg.(*protocol.ConsumeMsg)
	if !ok {
		return fmt.Errorf("unexpected response type: %T", msg)
	}

	if ack.Topic == "" {
		return fmt.Errorf("subscription failed")
	}

	fmt.Printf("[Consumer %s] <- CONSUME_ACK (subscribed to: %s)\n", c.id[:8], ack.Topic)

	// Start receiving results in background goroutine
	go c.receiveResults()

	return nil
}

func (c *Consumer) receiveResults() {
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
				c.errors <- fmt.Errorf("connection closed")
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
	if c.tcpConn != nil {
		c.tcpConn.Close()
		c.tcpConn = nil
	}
	close(c.results)
	close(c.errors)
}
