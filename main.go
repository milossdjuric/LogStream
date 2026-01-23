package main

import (
	"fmt"
	"log"
	"time"

	"github.com/milossdjuric/logstream/internal/analytics"
	"github.com/milossdjuric/logstream/internal/failure"
	"github.com/milossdjuric/logstream/internal/loadbalance"
	"github.com/milossdjuric/logstream/internal/protocol"
	"github.com/milossdjuric/logstream/internal/storage"
)

const (
	BroadcastPort        = 8888
	BrokerMulticastGroup = "239.0.0.1:9999"
)

type BrokerNode struct {
	id                string
	address           string
	isLeader          bool
	multicastReceiver *protocol.MulticastConnection
	multicastSender   *protocol.MulticastConnection
	broadcastListener *protocol.BroadcastConnection
	log               *storage.MemoryLog
	failureDetector   *failure.AccrualFailureDetector
	ring              *loadbalance.ConsistentHashRing
}

func NewBrokerNode(address string, isLeader bool) *BrokerNode {
	return &BrokerNode{
		id:       protocol.GenerateNodeID(address),
		address:  address,
		isLeader: isLeader,
		ring:     loadbalance.NewConsistentHashRing(),
	}
}

func (b *BrokerNode) Start() error {
	receiver, err := protocol.JoinMulticastGroup(BrokerMulticastGroup, "0.0.0.0")
	if err != nil {
		return fmt.Errorf("failed to join multicast: %w", err)
	}
	b.multicastReceiver = receiver

	sender, err := protocol.CreateMulticastSender("0.0.0.0:0")
	if err != nil {
		b.multicastReceiver.Close()
		return fmt.Errorf("failed to create multicast sender: %w", err)
	}
	b.multicastSender = sender

	if b.isLeader {
		listener, err := protocol.CreateBroadcastListener(BroadcastPort)
		if err != nil {
			b.multicastReceiver.Close()
			b.multicastSender.Close()
			return fmt.Errorf("failed to create broadcast listener: %w", err)
		}
		b.broadcastListener = listener
		go b.listenForBroadcastJoins()
	}

	go b.listenMulticast()

	l := storage.NewMemoryLog()
	b.log = l

	// Initialize Failure Detector
	b.failureDetector = failure.NewAccrualFailureDetector(1000)
	go b.monitorFailures()

	// Start analytics reporter
	agg := analytics.NewLogAggregator(l)
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		for range ticker.C {
			total := agg.TotalCount()
			// Rate for the last 5 seconds (to smooth it out slightly)
			rate := agg.GetRate(5 * time.Second)
			fmt.Printf("\n[Analytics %s] Total Logs: %d | Rate (last 5s): %.2f msg/s\n", b.id, total, rate)
		}
	}()

	b.ring.AddNode(b.id) // Add self to ring
	fmt.Printf("[Broker %s] Started (leader=%v)\n", b.id, b.isLeader)
	return nil
}

func (b *BrokerNode) listenMulticast() {
	for {
		msg, sender, err := b.multicastReceiver.ReadMessage()
		if err != nil {
			log.Printf("[Broker %s] Multicast read error: %v\n", b.id, err)
			continue
		}

		switch m := msg.(type) {
		case *protocol.HeartbeatMsg:
			senderID := protocol.GetSenderID(msg)
			fmt.Printf("[Broker %s] ← HEARTBEAT from %s\n", b.id, senderID)
			b.ring.AddNode(senderID) // add node to ring

		case *protocol.ReplicateMsg:
			fmt.Printf("[Broker %s] ← REPLICATE seq=%d type=%s from %s\n",
				b.id, protocol.GetSequenceNum(msg), m.UpdateType, protocol.GetSenderID(msg))

		case *protocol.ElectionMsg:
			fmt.Printf("[Broker %s] ← ELECTION candidate=%s phase=%v from %s\n",
				b.id, m.CandidateId, m.Phase, protocol.GetSenderID(msg))

		case *protocol.NackMsg:
			fmt.Printf("[Broker %s] ← NACK seq=%d-%d from %s\n",
				b.id, m.FromSeq, m.ToSeq, protocol.GetSenderID(msg))

		case *protocol.DataMsg:
			senderID := protocol.GetSenderID(msg)
			// Check if we are the owner of this topic
			owner := b.ring.GetNode(m.Topic)
			if owner == b.id {
				fmt.Printf("[Broker %s] ← DATA topic=%s from %s payload=%s (ACCEPTED)\n",
					b.id, m.Topic, senderID, string(m.Data))
				// Persist to local log
				if b.log != nil {
					// Use the timestamp from the message header
					ts := protocol.GetTimestamp(msg)
					encoded := storage.EncodeRecord(ts, m.Data)
					if off, err := b.log.Append(encoded); err == nil {
						// Verify by decoding immediately for display
						t := time.Unix(0, ts)
						fmt.Printf("[Broker %s] persisted data at offset=%d | Timestamp: %s\n",
							b.id, off, t.Format(time.RFC3339))
					} else {
						fmt.Printf("[Broker %s] failed to persist data: %v\n", b.id, err)
					}
				}
			} else {
				fmt.Printf("[Broker %s] ← DATA topic=%s from %s (IGNORED, owner=%s)\n",
					b.id, m.Topic, senderID, owner)
			}
		}

		_ = sender
	}
}

func (b *BrokerNode) listenForBroadcastJoins() {
	for {
		msg, sender, err := b.broadcastListener.ReceiveMessage()
		if err != nil {
			log.Printf("[Leader %s] Broadcast read error: %v\n", b.id, err)
			continue
		}

		if joinMsg, ok := msg.(*protocol.JoinMsg); ok {
			senderID := protocol.GetSenderID(msg)
			fmt.Printf("[Leader %s] ← JOIN from %s (addr=%s)\n",
				b.id, senderID, joinMsg.Address)

			b.ring.AddNode(senderID)

			responseAddr := fmt.Sprintf("%s", sender)
			err := b.broadcastListener.SendJoinResponse(
				b.id,
				b.address,
				BrokerMulticastGroup,
				[]string{b.address},
				responseAddr,
			)
			if err != nil {
				log.Printf("[Leader %s] Failed to send JOIN_RESPONSE: %v\n", b.id, err)
			} else {
				fmt.Printf("[Leader %s] → JOIN_RESPONSE to %s\n", b.id, sender)
			}
		}
	}
}

func (b *BrokerNode) SendHeartbeat() error {
	// Any node can send heartbeat to announce presence
	nodeType := protocol.NodeType_BROKER
	if b.isLeader {
		nodeType = protocol.NodeType_LEADER
	}

	err := protocol.SendHeartbeatMulticast(
		b.multicastSender,
		b.id,
		nodeType,
		BrokerMulticastGroup,
	)

	if err == nil {
		fmt.Printf("[%s %s] → HEARTBEAT to multicast group\n", nodeType, b.id)
	}
	return err
}

func (b *BrokerNode) ReplicateState(updateType string, data []byte, seqNum int64) error {
	if !b.isLeader {
		return fmt.Errorf("only leader can replicate state")
	}

	err := protocol.SendReplicationMulticast(
		b.multicastSender,
		b.id,
		data,
		updateType,
		seqNum,
		BrokerMulticastGroup,
	)

	if err == nil {
		fmt.Printf("[Leader %s] → REPLICATE seq=%d type=%s\n", b.id, seqNum, updateType)
	}
	// persist the replicated data to local log as well
	if b.log != nil {
		ts := time.Now().UnixNano()
		encoded := storage.EncodeRecord(ts, data)
		if off, err2 := b.log.Append(encoded); err2 == nil {
			t := time.Unix(0, ts)
			fmt.Printf("[Leader %s] persisted replicate at offset=%d | Timestamp: %s\n",
				b.id, off, t.Format(time.RFC3339))
		} else {
			fmt.Printf("[Leader %s] failed to persist replicate: %v\n", b.id, err2)
		}
	}
	return err
}

func (b *BrokerNode) monitorFailures() {
	ticker := time.NewTicker(500 * time.Millisecond)
	// Track reported dead nodes to avoid spamming logs (simple set)
	reportedDead := make(map[string]bool)

	for range ticker.C {
		// Threshold Phi=8 (10^-8 probability), very high confidence
		suspects := b.failureDetector.GetSuspects(8.0)

		for _, nodeID := range suspects {
			if !reportedDead[nodeID] {
				phi := b.failureDetector.Status(nodeID)
				fmt.Printf("[Detector] Node %s seems DOWN (Phi=%.2f)\n", nodeID, phi)
				reportedDead[nodeID] = true
			}
		}
	}
}

func JoinExistingCluster(address string) error {
	nodeID := protocol.GenerateNodeID(address)

	fmt.Printf("[New Node %s] Broadcasting JOIN...\n", nodeID)

	response, err := protocol.DiscoverClusterWithRetry(
		nodeID,
		protocol.NodeType_BROKER,
		address,
		nil,
	)

	if err != nil {
		return fmt.Errorf("failed to discover cluster: %w", err)
	}

	fmt.Printf("[New Node %s] ← JOIN_RESPONSE from leader\n", nodeID)
	fmt.Printf("  Leader: %s\n", response.LeaderAddress)
	fmt.Printf("  Multicast Group: %s\n", response.MulticastGroup)
	fmt.Printf("  Brokers: %v\n", response.BrokerAddresses)

	return nil
}

func main() {
	leader := NewBrokerNode("192.168.1.10:8001", true)
	if err := leader.Start(); err != nil {
		log.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)

	broker := NewBrokerNode("192.168.1.11:8002", false)
	if err := broker.Start(); err != nil {
		log.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)

	leader.SendHeartbeat()
	broker.SendHeartbeat() //brokers now also send heartbeats

	time.Sleep(500 * time.Millisecond)

	stateData := []byte(`{"consumers":["c1","c2"]}`)
	leader.ReplicateState("REGISTRY", stateData, 1)

	time.Sleep(500 * time.Millisecond)

	time.Sleep(5 * time.Second)
	// clean up storage logs
	if leader.log != nil {
		leader.log.Close()
	}
	if broker.log != nil {
		broker.log.Close()
	}
}
