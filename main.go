package main

import (
	"fmt"
	"log"
	"time"

	"github.com/milossdjuric/logstream/internal/protocol"
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
}

func NewBrokerNode(address string, isLeader bool) *BrokerNode {
	return &BrokerNode{
		id:       protocol.GenerateNodeID(address),
		address:  address,
		isLeader: isLeader,
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
			fmt.Printf("[Broker %s] ← HEARTBEAT from %s\n",
				b.id, protocol.GetSenderID(msg))

		case *protocol.ReplicateMsg:
			fmt.Printf("[Broker %s] ← REPLICATE seq=%d type=%s from %s\n",
				b.id, protocol.GetSequenceNum(msg), m.UpdateType, protocol.GetSenderID(msg))

		case *protocol.ElectionMsg:
			fmt.Printf("[Broker %s] ← ELECTION candidate=%s phase=%v from %s\n",
				b.id, m.CandidateId, m.Phase, protocol.GetSenderID(msg))

		case *protocol.NackMsg:
			fmt.Printf("[Broker %s] ← NACK seq=%d-%d from %s\n",
				b.id, m.FromSeq, m.ToSeq, protocol.GetSenderID(msg))
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
			fmt.Printf("[Leader %s] ← JOIN from %s (addr=%s)\n",
				b.id, protocol.GetSenderID(msg), joinMsg.Address)

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
	if !b.isLeader {
		return fmt.Errorf("only leader can send heartbeat")
	}

	err := protocol.SendHeartbeatMulticast(
		b.multicastSender,
		b.id,
		protocol.NodeType_LEADER,
		BrokerMulticastGroup,
	)

	if err == nil {
		fmt.Printf("[Leader %s] → HEARTBEAT to multicast group\n", b.id)
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
	return err
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

	time.Sleep(500 * time.Millisecond)

	stateData := []byte(`{"consumers":["c1","c2"]}`)
	leader.ReplicateState("REGISTRY", stateData, 1)

	time.Sleep(500 * time.Millisecond)

	time.Sleep(5 * time.Second)
}
