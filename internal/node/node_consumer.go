package node

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/milossdjuric/logstream/internal/analytics"
	"github.com/milossdjuric/logstream/internal/protocol"
	"github.com/milossdjuric/logstream/internal/storage"
)

// getConsumerOffset returns the last sent offset for a consumer on a topic
func (n *Node) getConsumerOffset(consumerID, topic string) uint64 {
	return n.consumerOffsets.GetOffset(consumerID, topic)
}

// setConsumerOffset updates the last sent offset for a consumer on a topic
func (n *Node) setConsumerOffset(consumerID, topic string, offset uint64) {
	n.consumerOffsets.SetOffset(consumerID, topic, offset)
}

// removeConsumerOffsets cleans up offset tracking for a consumer
func (n *Node) removeConsumerOffsets(consumerID string) {
	n.consumerOffsets.RemoveConsumer(consumerID)
}

// handleSubscribe processes SUBSCRIBE requests from consumers
// This is called when a consumer connects to the assigned broker after registration
func (n *Node) handleSubscribe(msg *protocol.SubscribeMsg, conn net.Conn) {
	consumerID := msg.ConsumerId
	topic := msg.Topic
	address := msg.ConsumerAddress
	enableProcessing := msg.EnableProcessing
	analyticsWindowSeconds := msg.AnalyticsWindowSeconds
	analyticsIntervalMs := msg.AnalyticsIntervalMs

	fmt.Printf("[Broker %s] <- SUBSCRIBE from consumer %s @ %s (topic: %s, processing: %v, window: %ds, interval: %dms)\n",
		n.id[:8], consumerID[:8], address, topic, enableProcessing, analyticsWindowSeconds, analyticsIntervalMs)

	// Check if we're the assigned broker for this topic
	stream, exists := n.clusterState.GetStreamAssignment(topic)
	if !exists {
		log.Printf("[Broker %s] SUBSCRIBE from %s REJECTED - no stream assignment for topic %s\n",
			n.id[:8], consumerID[:8], topic)
		// Send failure response
		ack := &protocol.SubscribeMessage{
			Header: &protocol.MessageHeader{
				Type:        protocol.MessageType_SUBSCRIBE,
				Timestamp:   time.Now().UnixNano(),
				SenderId:    n.id,
				SequenceNum: 0,
				SenderType:  protocol.NodeType_BROKER,
			},
			Topic:           "",
			ConsumerId:      consumerID,
			ConsumerAddress: "",
		}
		protocol.WriteTCPMessage(conn, &protocol.SubscribeMsg{SubscribeMessage: ack})
		return
	}

	// Verify we are the assigned broker
	if stream.AssignedBrokerId != n.id {
		log.Printf("[Broker %s] SUBSCRIBE from %s REJECTED - I'm not the assigned broker (assigned: %s)\n",
			n.id[:8], consumerID[:8], stream.AssignedBrokerId[:8])
		// Send failure response
		ack := &protocol.SubscribeMessage{
			Header: &protocol.MessageHeader{
				Type:        protocol.MessageType_SUBSCRIBE,
				Timestamp:   time.Now().UnixNano(),
				SenderId:    n.id,
				SequenceNum: 0,
				SenderType:  protocol.NodeType_BROKER,
			},
			Topic:           "",
			ConsumerId:      consumerID,
			ConsumerAddress: "",
		}
		protocol.WriteTCPMessage(conn, &protocol.SubscribeMsg{SubscribeMessage: ack})
		return
	}

	// Verify consumer is assigned to this stream
	if stream.ConsumerId != consumerID {
		log.Printf("[Broker %s] SUBSCRIBE from %s REJECTED - consumer not assigned to topic %s (assigned: %s)\n",
			n.id[:8], consumerID[:8], topic, safeIDStr(stream.ConsumerId))
		// Send failure response
		ack := &protocol.SubscribeMessage{
			Header: &protocol.MessageHeader{
				Type:        protocol.MessageType_SUBSCRIBE,
				Timestamp:   time.Now().UnixNano(),
				SenderId:    n.id,
				SequenceNum: 0,
				SenderType:  protocol.NodeType_BROKER,
			},
			Topic:           "",
			ConsumerId:      consumerID,
			ConsumerAddress: "",
		}
		protocol.WriteTCPMessage(conn, &protocol.SubscribeMsg{SubscribeMessage: ack})
		return
	}

	// Send success response
	ack := &protocol.SubscribeMessage{
		Header: &protocol.MessageHeader{
			Type:        protocol.MessageType_SUBSCRIBE,
			Timestamp:   time.Now().UnixNano(),
			SenderId:    n.id,
			SequenceNum: 0,
			SenderType:  protocol.NodeType_BROKER,
		},
		Topic:           topic,
		ConsumerId:      consumerID,
		ConsumerAddress: address,
	}

	if err := protocol.WriteTCPMessage(conn, &protocol.SubscribeMsg{SubscribeMessage: ack}); err != nil {
		log.Printf("[Broker %s] Failed to send SUBSCRIBE_ACK: %v\n", n.id[:8], err)
		return
	}

	fmt.Printf("[Broker %s] -> SUBSCRIBE_ACK to consumer %s @ %s (topic: %s)\n", n.id[:8], consumerID[:8], address, topic)

	// Start streaming results with optional processing
	go n.streamResultsToConsumerWithProcessing(consumerID, topic, conn, enableProcessing, analyticsWindowSeconds, analyticsIntervalMs)
}

// safeIDStr safely formats a node ID string for logging (defined in node_handlers.go)
// Returns first 8 characters if long enough, or "(empty)" if empty

// handleConsumeRequest processes CONSUME requests from consumers
// Implements one-to-one consumer-to-topic mapping and broker assignment
func (n *Node) handleConsumeRequest(msg *protocol.ConsumeMsg, conn net.Conn) {
	consumerID := protocol.GetSenderID(msg)
	topic := msg.Topic
	address := msg.ConsumerAddress

	fmt.Printf("[Leader %s] <- CONSUME from %s (topic: %s, addr: %s)\n",
		n.id[:8], consumerID[:8], topic, address)

	// View-synchronous check: queue message during view change instead of rejecting
	// This ensures no requests are lost during state exchange
	if n.IsFrozen() {
		fmt.Printf("[Leader %s] CONSUME from %s queued - operations frozen for view change\n",
			n.id[:8], consumerID[:8])
		n.queueFrozenMessage("CONSUME", msg, conn)
		return
	}

	// One-to-one check: verify topic doesn't already have a consumer
	if n.clusterState.HasConsumerForTopic(topic) {
		log.Printf("[Leader %s] CONSUME from %s REJECTED - topic %s already has a consumer (one-to-one mapping)\n",
			n.id[:8], consumerID[:8], topic)
		// Send failure response
		ack := &protocol.ConsumeMessage{
			Header: &protocol.MessageHeader{
				Type:        protocol.MessageType_CONSUME,
				Timestamp:   time.Now().UnixNano(),
				SenderId:    n.id,
				SequenceNum: 0,
				SenderType:  protocol.NodeType_LEADER,
			},
			Topic:           "",
			ConsumerAddress: "",
		}
		protocol.WriteTCPMessage(conn, &protocol.ConsumeMsg{ConsumeMessage: ack})
		return
	}

	// Get stream assignment - producer must have registered first
	stream, streamExists := n.clusterState.GetStreamAssignment(topic)
	if !streamExists {
		log.Printf("[Leader %s] CONSUME from %s REJECTED - no producer registered for topic %s yet\n",
			n.id[:8], consumerID[:8], topic)
		// Send failure response
		ack := &protocol.ConsumeMessage{
			Header: &protocol.MessageHeader{
				Type:        protocol.MessageType_CONSUME,
				Timestamp:   time.Now().UnixNano(),
				SenderId:    n.id,
				SequenceNum: 0,
				SenderType:  protocol.NodeType_LEADER,
			},
			Topic:           "",
			ConsumerAddress: "",
		}
		protocol.WriteTCPMessage(conn, &protocol.ConsumeMsg{ConsumeMessage: ack})
		return
	}

	// Register consumer
	if err := n.clusterState.RegisterConsumer(consumerID, address); err != nil {
		log.Printf("[Leader %s] Failed to register consumer: %v\n", n.id[:8], err)
		// Send failure response
		ack := &protocol.ConsumeMessage{
			Header: &protocol.MessageHeader{
				Type:        protocol.MessageType_CONSUME,
				Timestamp:   time.Now().UnixNano(),
				SenderId:    n.id,
				SequenceNum: 0,
				SenderType:  protocol.NodeType_LEADER,
			},
			Topic:           "",
			ConsumerAddress: "",
		}
		protocol.WriteTCPMessage(conn, &protocol.ConsumeMsg{ConsumeMessage: ack})
		return
	}

	// Assign consumer to the stream (one-to-one mapping)
	if err := n.clusterState.AssignConsumerToStream(consumerID, topic); err != nil {
		log.Printf("[Leader %s] Failed to assign consumer to stream: %v\n", n.id[:8], err)
		// Send failure response
		ack := &protocol.ConsumeMessage{
			Header: &protocol.MessageHeader{
				Type:        protocol.MessageType_CONSUME,
				Timestamp:   time.Now().UnixNano(),
				SenderId:    n.id,
				SequenceNum: 0,
				SenderType:  protocol.NodeType_LEADER,
			},
			Topic:           "",
			ConsumerAddress: "",
		}
		protocol.WriteTCPMessage(conn, &protocol.ConsumeMsg{ConsumeMessage: ack})
		return
	}

	// Subscribe consumer to topic (legacy - for backwards compatibility)
	if err := n.clusterState.SubscribeConsumer(consumerID, topic); err != nil {
		log.Printf("[Leader %s] Failed to subscribe consumer: %v\n", n.id[:8], err)
		// Send failure response so client doesn't get EOF
		ack := &protocol.ConsumeMessage{
			Header: &protocol.MessageHeader{
				Type:        protocol.MessageType_CONSUME,
				Timestamp:   time.Now().UnixNano(),
				SenderId:    n.id,
				SequenceNum: 0,
				SenderType:  protocol.NodeType_LEADER,
			},
			Topic:           "",
			ConsumerAddress: "",
		}
		protocol.WriteTCPMessage(conn, &protocol.ConsumeMsg{ConsumeMessage: ack})
		return
	}

	// Replicate state change immediately
	if err := n.replicateAllState(); err != nil {
		log.Printf("[Leader %s] Failed to replicate after consumer subscription: %v\n", n.id[:8], err)
	}

	// The assigned broker address from the stream assignment
	// This is where the consumer should receive results from
	assignedBrokerAddress := stream.BrokerAddress

	// Send success response with assigned broker address
	ack := &protocol.ConsumeMessage{
		Header: &protocol.MessageHeader{
			Type:        protocol.MessageType_CONSUME,
			Timestamp:   time.Now().UnixNano(),
			SenderId:    n.id,
			SequenceNum: 0,
			SenderType:  protocol.NodeType_LEADER,
		},
		Topic:           topic,
		ConsumerAddress: assignedBrokerAddress, // Consumer will receive results from this broker
	}

	if err := protocol.WriteTCPMessage(conn, &protocol.ConsumeMsg{ConsumeMessage: ack}); err != nil {
		log.Printf("[Leader %s] Failed to send CONSUME_ACK: %v\n", n.id[:8], err)
		return
	}

	fmt.Printf("[Leader %s] -> CONSUME_ACK to %s (topic: %s, broker: %s @ %s)\n",
		n.id[:8], consumerID[:8], topic, stream.AssignedBrokerId[:8], assignedBrokerAddress)

	// Connection will be closed by handleTCPConnection
	// Consumer will connect to the assigned broker with SUBSCRIBE message
	fmt.Printf("[Leader %s] Consumer %s should now connect to broker %s for results\n",
		n.id[:8], consumerID[:8], assignedBrokerAddress)
}

// streamResultsToConsumer streams data to a consumer
func (n *Node) streamResultsToConsumer(consumerID, topic string, conn net.Conn) {
	fmt.Printf("[Node %s] Started result stream to consumer %s (topic: %s)\n",
		n.id[:8], consumerID[:8], topic)

	defer func() {
		conn.Close()
		fmt.Printf("[Node %s] Closed connection to consumer %s\n", n.id[:8], consumerID[:8])
	}()

	// First, send any existing data (catch-up)
	n.sendCatchUpData(consumerID, topic, conn)

	// Then keep streaming new data as it arrives
	pollTicker := time.NewTicker(100 * time.Millisecond) // Poll for new data
	heartbeatTicker := time.NewTicker(30 * time.Second)
	defer pollTicker.Stop()
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-n.shutdownCtx.Done():
			fmt.Printf("[Node %s] Shutdown requested, closing consumer stream for %s\n", n.id[:8], consumerID[:8])
			return

		case <-heartbeatTicker.C:
			// Update consumer heartbeat
			n.clusterState.UpdateConsumerHeartbeat(consumerID)

		case <-pollTicker.C:
			// Check for new data and send it
			if err := n.sendNewDataToConsumer(consumerID, topic, conn); err != nil {
				fmt.Printf("[Node %s] Error sending data to consumer %s: %v\n",
					n.id[:8], consumerID[:8], err)
				return // Connection likely closed
			}
		}
	}
}

// sendCatchUpData sends all existing data from the topic to a newly subscribed consumer
func (n *Node) sendCatchUpData(consumerID, topic string, conn net.Conn) {
	topicLog, exists := n.GetTopicLog(topic)
	if !exists {
		fmt.Printf("[Node %s] No existing data for topic %s, consumer %s will receive new data only\n",
			n.id[:8], topic, consumerID[:8])
		return
	}

	startOffset := n.getConsumerOffset(consumerID, topic)
	highestOffset, err := topicLog.HighestOffset()
	if err != nil {
		// Empty log, nothing to catch up
		return
	}

	if startOffset > highestOffset {
		// Already caught up
		return
	}

	fmt.Printf("[Node %s] Sending catch-up data to consumer %s (offsets %d to %d)\n",
		n.id[:8], consumerID[:8], startOffset, highestOffset)

	for offset := startOffset; offset <= highestOffset; offset++ {
		raw, err := topicLog.Read(offset)
		if err != nil {
			continue
		}

		timestamp, data, err := storage.DecodeRecord(raw)
		if err != nil {
			continue
		}

		resultMsg := protocol.NewResultMsg(
			n.id,
			topic,
			data,
			int64(offset),
			timestamp,
		)

		if err := protocol.WriteTCPMessage(conn, resultMsg); err != nil {
			fmt.Printf("[Node %s] Failed to send catch-up data to consumer %s: %v\n",
				n.id[:8], consumerID[:8], err)
			return
		}

		n.setConsumerOffset(consumerID, topic, offset+1)
	}

	fmt.Printf("[Node %s] Catch-up complete for consumer %s on topic %s\n",
		n.id[:8], consumerID[:8], topic)
}

// sendNewDataToConsumer checks for new data and sends it to the consumer
func (n *Node) sendNewDataToConsumer(consumerID, topic string, conn net.Conn) error {
	topicLog, exists := n.GetTopicLog(topic)
	if !exists {
		return nil // No data yet
	}

	currentOffset := n.getConsumerOffset(consumerID, topic)
	highestOffset, err := topicLog.HighestOffset()
	if err != nil {
		return nil // Empty log
	}

	if currentOffset > highestOffset {
		return nil // Already caught up
	}

	// Send any new data
	for offset := currentOffset; offset <= highestOffset; offset++ {
		raw, err := topicLog.Read(offset)
		if err != nil {
			continue
		}

		timestamp, data, err := storage.DecodeRecord(raw)
		if err != nil {
			continue
		}

		resultMsg := protocol.NewResultMsg(
			n.id,
			topic,
			data,
			int64(offset),
			timestamp,
		)

		if err := protocol.WriteTCPMessage(conn, resultMsg); err != nil {
			return err // Connection error
		}

		n.setConsumerOffset(consumerID, topic, offset+1)
	}

	return nil
}

// ============== Data Processing Pipeline ==============

// ProcessedResult contains analytics/aggregation results
type ProcessedResult struct {
	Topic          string  `json:"topic"`
	TotalCount     int64   `json:"total_count"`
	WindowSeconds  int     `json:"window_seconds"`
	WindowCount    int64   `json:"window_count"`
	WindowRate     float64 `json:"window_rate"`
	LatestOffset   uint64  `json:"latest_offset"`
	Timestamp      int64   `json:"timestamp"`
	RawData        []byte  `json:"raw_data,omitempty"` // Include raw data if needed
}

// streamResultsToConsumerWithProcessing streams data with optional processing
func (n *Node) streamResultsToConsumerWithProcessing(consumerID, topic string, conn net.Conn, enableProcessing bool, analyticsWindowSeconds, analyticsIntervalMs int32) {
	processingStr := "raw"
	if enableProcessing {
		processingStr = "processed"
	}
	consumerAddr := conn.RemoteAddr().String()
	fmt.Printf("[Broker %s] Started %s result stream to consumer %s @ %s (topic: %s)\n",
		n.id[:8], processingStr, consumerID[:8], consumerAddr, topic)

	defer func() {
		conn.Close()
		fmt.Printf("[Broker %s] Closed connection to consumer %s @ %s\n", n.id[:8], consumerID[:8], consumerAddr)
	}()

	// Apply defaults for zero values with minimums
	effectiveIntervalMs := int(analyticsIntervalMs)
	if effectiveIntervalMs <= 0 {
		effectiveIntervalMs = 1000 // default 1000ms
	} else if effectiveIntervalMs < 100 {
		effectiveIntervalMs = 100 // minimum 100ms
	}

	effectiveWindowSeconds := int(analyticsWindowSeconds)
	if effectiveWindowSeconds <= 0 {
		effectiveWindowSeconds = n.config.AnalyticsWindowSeconds // broker default
	} else if effectiveWindowSeconds < 1 {
		effectiveWindowSeconds = 1 // minimum 1s
	}

	effectiveWindow := time.Duration(effectiveWindowSeconds) * time.Second

	// First, send any existing data (catch-up)
	if enableProcessing {
		n.sendCatchUpDataProcessed(consumerID, topic, conn)
	} else {
		n.sendCatchUpData(consumerID, topic, conn)
	}

	// Then keep streaming new data as it arrives
	pollTicker := time.NewTicker(100 * time.Millisecond) // Poll for new data
	heartbeatTicker := time.NewTicker(30 * time.Second)
	// For processed mode, also send periodic analytics updates
	analyticsTicker := time.NewTicker(time.Duration(effectiveIntervalMs) * time.Millisecond)
	defer pollTicker.Stop()
	defer heartbeatTicker.Stop()
	defer analyticsTicker.Stop()

	for {
		select {
		case <-n.shutdownCtx.Done():
			fmt.Printf("[Broker %s] Shutdown requested, closing consumer stream for %s\n", n.id[:8], consumerID[:8])
			return

		case <-heartbeatTicker.C:
			// Update consumer heartbeat
			n.clusterState.UpdateConsumerHeartbeat(consumerID)

		case <-analyticsTicker.C:
			// Send periodic analytics update if processing is enabled
			if enableProcessing {
				if err := n.sendAnalyticsUpdate(consumerID, topic, conn, effectiveWindow); err != nil {
					fmt.Printf("[Broker %s] Error sending analytics to consumer %s: %v\n",
						n.id[:8], consumerID[:8], err)
					return
				}
			}

		case <-pollTicker.C:
			// Check for new data and send it
			var err error
			if enableProcessing {
				err = n.sendNewDataToConsumerProcessed(consumerID, topic, conn, effectiveWindow)
			} else {
				err = n.sendNewDataToConsumer(consumerID, topic, conn)
			}
			if err != nil {
				fmt.Printf("[Broker %s] Error sending data to consumer %s: %v\n",
					n.id[:8], consumerID[:8], err)
				return // Connection likely closed
			}
		}
	}
}

// sendCatchUpDataProcessed sends processed catch-up data to consumer
func (n *Node) sendCatchUpDataProcessed(consumerID, topic string, conn net.Conn) {
	topicLog, exists := n.GetTopicLog(topic)
	if !exists {
		fmt.Printf("[Broker %s] No existing data for topic %s, consumer %s will receive new data only\n",
			n.id[:8], topic, consumerID[:8])
		return
	}

	startOffset := n.getConsumerOffset(consumerID, topic)
	highestOffset, err := topicLog.HighestOffset()
	if err != nil {
		return
	}

	if startOffset > highestOffset {
		return
	}

	fmt.Printf("[Broker %s] Sending processed catch-up data to consumer %s (offsets %d to %d)\n",
		n.id[:8], consumerID[:8], startOffset, highestOffset)

	// Send each record with processing info
	for offset := startOffset; offset <= highestOffset; offset++ {
		raw, err := topicLog.Read(offset)
		if err != nil {
			continue
		}

		timestamp, data, err := storage.DecodeRecord(raw)
		if err != nil {
			continue
		}

		// Create processed result with raw data included
		processed := &ProcessedResult{
			Topic:        topic,
			LatestOffset: offset,
			Timestamp:    timestamp,
			RawData:      data,
		}

		// Compute running analytics up to this point
		agg := analytics.NewLogAggregator(topicLog)
		processed.TotalCount = agg.TotalCount()

		// Serialize processed result
		processedJSON, err := json.Marshal(processed)
		if err != nil {
			continue
		}

		resultMsg := protocol.NewResultMsg(
			n.id,
			topic,
			processedJSON,
			int64(offset),
			timestamp,
		)

		if err := protocol.WriteTCPMessage(conn, resultMsg); err != nil {
			fmt.Printf("[Broker %s] Failed to send processed catch-up data to consumer %s: %v\n",
				n.id[:8], consumerID[:8], err)
			return
		}

		n.setConsumerOffset(consumerID, topic, offset+1)
	}

	fmt.Printf("[Broker %s] Processed catch-up complete for consumer %s on topic %s\n",
		n.id[:8], consumerID[:8], topic)
}

// sendNewDataToConsumerProcessed sends new data with processing to consumer
func (n *Node) sendNewDataToConsumerProcessed(consumerID, topic string, conn net.Conn, windowDuration time.Duration) error {
	topicLog, exists := n.GetTopicLog(topic)
	if !exists {
		return nil
	}

	currentOffset := n.getConsumerOffset(consumerID, topic)
	highestOffset, err := topicLog.HighestOffset()
	if err != nil {
		return nil
	}

	if currentOffset > highestOffset {
		return nil
	}

	// Send new data with processing
	for offset := currentOffset; offset <= highestOffset; offset++ {
		raw, err := topicLog.Read(offset)
		if err != nil {
			continue
		}

		timestamp, data, err := storage.DecodeRecord(raw)
		if err != nil {
			continue
		}

		// Create processed result with raw data
		processed := &ProcessedResult{
			Topic:         topic,
			LatestOffset:  offset,
			Timestamp:     timestamp,
			RawData:       data,
			WindowSeconds: int(windowDuration.Seconds()),
		}

		// Add running analytics using per-consumer window
		agg := analytics.NewLogAggregator(topicLog)
		processed.TotalCount = agg.TotalCount()
		processed.WindowCount = agg.CountInWindow(time.Now().Add(-windowDuration), time.Now())
		processed.WindowRate = agg.GetRate(windowDuration)

		// Serialize processed result
		processedJSON, err := json.Marshal(processed)
		if err != nil {
			continue
		}

		resultMsg := protocol.NewResultMsg(
			n.id,
			topic,
			processedJSON,
			int64(offset),
			timestamp,
		)

		if err := protocol.WriteTCPMessage(conn, resultMsg); err != nil {
			return err
		}

		n.setConsumerOffset(consumerID, topic, offset+1)
	}

	return nil
}

// sendAnalyticsUpdate sends periodic analytics update to consumer
func (n *Node) sendAnalyticsUpdate(consumerID, topic string, conn net.Conn, windowDuration time.Duration) error {
	topicLog, exists := n.GetTopicLog(topic)
	if !exists {
		return nil // No data yet, skip analytics
	}

	// Compute analytics using per-consumer window
	topicAnalytics := analytics.ComputeTopicAnalytics(topic, topicLog, windowDuration)

	highestOffset, err := topicLog.HighestOffset()
	if err != nil {
		highestOffset = 0
	}

	// Create analytics result
	analyticsResult := &ProcessedResult{
		Topic:         topic,
		TotalCount:    topicAnalytics.TotalCount,
		WindowSeconds: int(windowDuration.Seconds()),
		WindowCount:   topicAnalytics.WindowCount,
		WindowRate:    topicAnalytics.WindowRate,
		LatestOffset:  highestOffset,
		Timestamp:     time.Now().UnixNano(),
	}

	// Serialize analytics result
	analyticsJSON, err := json.Marshal(analyticsResult)
	if err != nil {
		return err
	}

	resultMsg := protocol.NewResultMsg(
		n.id,
		topic,
		analyticsJSON,
		int64(highestOffset),
		time.Now().UnixNano(),
	)

	return protocol.WriteTCPMessage(conn, resultMsg)
}
