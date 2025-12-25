package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/milossdjuric/logstream/internal/config"
	"github.com/milossdjuric/logstream/internal/protocol"
)

type BrokerNode struct {
	id                string
	config            *config.Config
	multicastReceiver *protocol.MulticastConnection
	multicastSender   *protocol.MulticastConnection
	broadcastListener *protocol.BroadcastConnection
}

func NewBrokerNode(cfg *config.Config) *BrokerNode {
	return &BrokerNode{
		id:     protocol.GenerateNodeID(cfg.NodeAddress),
		config: cfg,
	}
}

func (b *BrokerNode) Start() error {
	receiver, err := protocol.JoinMulticastGroup(
		b.config.MulticastGroup,
		b.config.NetworkInterface,
	)
	if err != nil {
		return fmt.Errorf("failed to join multicast: %w", err)
	}
	b.multicastReceiver = receiver

	senderAddr := fmt.Sprintf("%s:0", b.config.NetworkInterface)
	sender, err := protocol.CreateMulticastSender(senderAddr)
	if err != nil {
		b.multicastReceiver.Close()
		return fmt.Errorf("failed to create multicast sender: %w", err)
	}
	b.multicastSender = sender

	if b.config.IsLeader {
		listener, err := protocol.CreateBroadcastListener(b.config.BroadcastPort)
		if err != nil {
			b.multicastReceiver.Close()
			b.multicastSender.Close()
			return fmt.Errorf("failed to create broadcast listener: %w", err)
		}
		b.broadcastListener = listener
		go b.listenForBroadcastJoins()
	}

	go b.listenMulticast()

	fmt.Printf("[Broker %s] Started at %s (leader=%v)\n",
		b.id[:8], b.config.NodeAddress, b.config.IsLeader)
	return nil
}

func (b *BrokerNode) listenMulticast() {
	for {
		msg, sender, err := b.multicastReceiver.ReadMessage()
		if err != nil {
			log.Printf("[Broker %s] Multicast read error: %v\n", b.id[:8], err)
			continue
		}

		switch m := msg.(type) {
		case *protocol.HeartbeatMsg:
			fmt.Printf("[%s] ← HEARTBEAT from %s (sender: %s)\n",
				b.id[:8], protocol.GetSenderID(msg)[:8], sender)

		case *protocol.ReplicateMsg:
			fmt.Printf("[%s] ← REPLICATE seq=%d type=%s from %s (sender: %s)\n",
				b.id[:8], protocol.GetSequenceNum(msg), m.UpdateType,
				protocol.GetSenderID(msg)[:8], sender)

		case *protocol.ElectionMsg:
			fmt.Printf("[%s] ← ELECTION candidate=%s phase=%v\n",
				b.id[:8], m.CandidateId[:8], m.Phase)

		case *protocol.NackMsg:
			fmt.Printf("[%s] ← NACK seq=%d-%d from %s\n",
				b.id[:8], m.FromSeq, m.ToSeq, protocol.GetSenderID(msg)[:8])
		}
	}
}

func (b *BrokerNode) listenForBroadcastJoins() {
	for {
		msg, sender, err := b.broadcastListener.ReceiveMessage()
		if err != nil {
			log.Printf("[Leader %s] Broadcast read error: %v\n", b.id[:8], err)
			continue
		}

		if joinMsg, ok := msg.(*protocol.JoinMsg); ok {
			fmt.Printf("[Leader %s] ← JOIN from %s (addr=%s)\n",
				b.id[:8], protocol.GetSenderID(msg)[:8], joinMsg.Address)

			responseAddr := fmt.Sprintf("%s", sender)
			err := b.broadcastListener.SendJoinResponse(
				b.id,
				b.config.NodeAddress,
				b.config.MulticastGroup,
				[]string{b.config.NodeAddress},
				responseAddr,
			)
			if err != nil {
				log.Printf("[Leader %s] Failed to send JOIN_RESPONSE: %v\n", b.id[:8], err)
			} else {
				fmt.Printf("[Leader %s] → JOIN_RESPONSE to %s\n", b.id[:8], sender)
			}
		}
	}
}

func (b *BrokerNode) SendHeartbeat() error {
	if !b.config.IsLeader {
		return fmt.Errorf("only leader can send heartbeat")
	}

	err := protocol.SendHeartbeatMulticast(
		b.multicastSender,
		b.id,
		protocol.NodeType_LEADER,
		b.config.MulticastGroup,
	)

	if err == nil {
		fmt.Printf("[Leader %s] → HEARTBEAT\n", b.id[:8])
	}
	return err
}

func (b *BrokerNode) ReplicateState(updateType string, data []byte, seqNum int64) error {
	if !b.config.IsLeader {
		return fmt.Errorf("only leader can replicate state")
	}

	err := protocol.SendReplicationMulticast(
		b.multicastSender,
		b.id,
		data,
		updateType,
		seqNum,
		b.config.MulticastGroup,
	)

	if err == nil {
		fmt.Printf("[Leader %s] → REPLICATE seq=%d type=%s\n", b.id[:8], seqNum, updateType)
	}
	return err
}

func (b *BrokerNode) Shutdown() {
	fmt.Printf("\n[Broker %s] Shutting down...\n", b.id[:8])

	if b.multicastReceiver != nil {
		b.multicastReceiver.Close()
	}
	if b.multicastSender != nil {
		b.multicastSender.Close()
	}
	if b.broadcastListener != nil {
		b.broadcastListener.Close()
	}
}

func main() {
	if os.Getenv("NODE_ADDRESS") == "" {
		protocol.ShowNetworkInfo()
	}

	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Configuration error: %v\n", err)
	}

	if err := cfg.Validate(); err != nil {
		log.Fatalf("Invalid configuration: %v\n", err)
	}

	cfg.Print()

	broker := NewBrokerNode(cfg)
	if err := broker.Start(); err != nil {
		log.Fatal(err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	if cfg.IsLeader {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		seqNum := int64(1)

		for {
			select {
			case <-ticker.C:
				broker.SendHeartbeat()

				stateData := []byte(fmt.Sprintf(`{"seq":%d,"time":"%s"}`,
					seqNum, time.Now().Format(time.RFC3339)))
				broker.ReplicateState("REGISTRY", stateData, seqNum)
				seqNum++

			case <-sigChan:
				broker.Shutdown()
				return
			}
		}
	} else {
		<-sigChan
		broker.Shutdown()
	}
}
