package node

import (
	"context"
	"fmt"
	"log"
	"net"
	"runtime"
	"sync"
	"time"

	"github.com/milossdjuric/logstream/internal/config"
	"github.com/milossdjuric/logstream/internal/failure"
	"github.com/milossdjuric/logstream/internal/loadbalance"
	"github.com/milossdjuric/logstream/internal/protocol"
	"github.com/milossdjuric/logstream/internal/state"
	"github.com/milossdjuric/logstream/internal/storage"
)

type Node struct {
	id      string
	address string
	config  *config.Config

	mu       sync.RWMutex
	isLeader bool

	clusterState  *state.ClusterState
	holdbackQueue *state.RegistryHoldbackQueue

	lastReplicatedSeq int64
	lastAppliedSeqNum int64

	election     *state.ElectionState
	ringPosition int
	nextNode     string

	leaderAddress string

	failureDetector       *failure.AccrualFailureDetector
	phiSuspicionThreshold float64
	phiFailureThreshold   float64
	lastLeaderHeartbeat   time.Time
	lastLeaderHeartbeatMu sync.RWMutex

	multicastReceiver *protocol.MulticastConnection
	multicastSender   *protocol.MulticastConnection
	broadcastListener *protocol.BroadcastConnection
	tcpListener       net.Listener
	udpDataListener   *net.UDPConn // UDP listener for DATA messages from producers

	stopLeaderDuties   chan struct{}
	stopFollowerDuties chan struct{}

	dataLogs   map[string]*storage.MemoryLog
	dataLogsMu sync.RWMutex

	consumerOffsets *state.ConsumerOffsetTracker

	dataHoldbackQueue *state.DataHoldbackQueue

	brokerRing *loadbalance.ConsistentHashRing

	viewState       *state.ViewState
	recoveryManager *state.ViewRecoveryManager

	frozenQueue   []frozenMessage
	frozenQueueMu sync.Mutex

	replicateAcks  map[int64]map[string]bool
	replicateAckMu sync.Mutex

	// Rate limiting for JOIN rejections to prevent flooding
	joinRejections   map[string]time.Time // nodeID -> last rejection time
	joinRejectionsMu sync.Mutex

	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
}

type frozenMessage struct {
	msgType string
	msg     protocol.Message
	conn    net.Conn
}

func NewNode(cfg *config.Config) *Node {
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())

	node := &Node{
		id:                    protocol.GenerateNodeID(cfg.NodeAddress),
		address:               cfg.NodeAddress,
		config:                cfg,
		isLeader:              false,
		clusterState:          state.NewClusterState(),
		lastReplicatedSeq:     0,
		lastAppliedSeqNum:     0,
		election:              state.NewElectionState(),
		stopLeaderDuties:      make(chan struct{}),
		stopFollowerDuties:    make(chan struct{}),
		shutdownCtx:           shutdownCtx,
		shutdownCancel:        shutdownCancel,
		failureDetector:       failure.NewAccrualFailureDetector(1000),
		phiSuspicionThreshold: 8.0,
		phiFailureThreshold:   10.0,
		dataLogs:              make(map[string]*storage.MemoryLog),
		consumerOffsets:       state.NewConsumerOffsetTracker(),
		brokerRing:            loadbalance.NewConsistentHashRing(),
		viewState:             state.NewViewState(),
		recoveryManager:       state.NewViewRecoveryManager(),
		frozenQueue:           make([]frozenMessage, 0),
		joinRejections:        make(map[string]time.Time),
	}

	node.holdbackQueue = state.NewRegistryHoldbackQueue(node.applyRegistryUpdate)
	node.dataHoldbackQueue = state.NewDataHoldbackQueue(node.storeDataFromHoldback)

	return node
}

func (n *Node) Start() error {
	fmt.Printf("[Node %s] Starting node at %s\n", n.id[:8], n.address)

	// Setup network listeners before discovery
	if err := n.setupMulticast(); err != nil {
		return fmt.Errorf("multicast setup failed: %w", err)
	}

	if err := n.startTCPListener(); err != nil {
		return fmt.Errorf("TCP listener failed: %w", err)
	}

	if err := n.startUDPDataListener(); err != nil {
		return fmt.Errorf("UDP DATA listener failed: %w", err)
	}

	// Broadcast listener started only by leader in becomeLeader()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				buf := make([]byte, 4096)
				stackLen := runtime.Stack(buf, false)
				log.Printf("[Node %s] Multicast listener panic: %v\n%s\n", n.id[:8], r, buf[:stackLen])
			}
		}()
		n.listenMulticast()
	}()

	time.Sleep(100 * time.Millisecond)

	// Discover existing cluster via UDP broadcast
	err := n.discoverCluster()
	if err != nil {
		clusterFound := false
		maxExtendedRetries := 1
		for i := 0; i < maxExtendedRetries && !clusterFound; i++ {
			jitterMs := int(n.id[0]) * 2 % 500
			baseDelayMs := 500
			totalDelay := time.Duration(baseDelayMs+jitterMs) * time.Millisecond

			fmt.Printf("[Node %s] Extended discovery attempt %d/%d (delay: %v)\n",
				n.id[:8], i+1, maxExtendedRetries, totalDelay)
			time.Sleep(totalDelay)

			err = n.discoverCluster()
			if err == nil {
				clusterFound = true
			}
		}

		if clusterFound {
			fmt.Printf("[Node %s] Joined existing cluster as follower\n", n.id[:8])
			n.becomeFollower()
		} else {
			fmt.Printf("[Node %s] No cluster found, becoming leader\n", n.id[:8])
			n.becomeLeader()
		}
	} else {
		fmt.Printf("[Node %s] Joined existing cluster as follower\n", n.id[:8])
		n.becomeFollower()
	}

	// Start role-specific duties
	if n.IsLeader() {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					buf := make([]byte, 4096)
					stackLen := runtime.Stack(buf, false)
					log.Printf("[Leader %s] Leader duties panic: %v\n%s\n", n.id[:8], r, buf[:stackLen])
				}
			}()
			n.runLeaderDuties()
		}()
	} else {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					buf := make([]byte, 4096)
					stackLen := runtime.Stack(buf, false)
					log.Printf("[Follower %s] Follower duties panic: %v\n%s\n", n.id[:8], r, buf[:stackLen])
				}
			}()
			n.runFollowerDuties()
		}()
	}

	role := map[bool]string{true: "LEADER", false: "FOLLOWER"}[n.IsLeader()]
	fmt.Printf("[Node %s] Startup complete as %s\n", n.id[:8], role)
	return nil
}

func (n *Node) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.isLeader
}

func (n *Node) becomeLeader() {
	n.becomeLeaderInternal(false, 0)
}

func (n *Node) becomeLeaderAfterElection(electionID int64) {
	n.becomeLeaderInternal(true, electionID)
}

func (n *Node) becomeLeaderInternal(fromElection bool, electionID int64) {
	n.mu.Lock()
	wasLeader := n.isLeader
	n.isLeader = true
	n.mu.Unlock()

	if !wasLeader {
		fmt.Printf("[Node %s] Becoming LEADER\n", n.id[:8])

		select {
		case n.stopFollowerDuties <- struct{}{}:
		case <-time.After(100 * time.Millisecond):
		}

		n.clusterState.RegisterBroker(n.id, n.address, true)
		n.leaderAddress = n.address
		n.syncBrokerRing()
		n.lastReplicatedSeq = n.clusterState.GetSequenceNum()

		if fromElection {
			if err := n.initiateStateExchange(electionID); err != nil {
				log.Printf("[Leader %s] State exchange failed: %v\n", n.id[:8], err)
				// Safety: ensure operations are unfrozen even if state exchange failed
				// (initiateStateExchange/installNewView should unfreeze on error paths,
				// but this is a final safety net to prevent permanent freeze)
				if n.IsFrozen() {
					log.Printf("[Leader %s] Safety unfreeze after state exchange failure\n", n.id[:8])
					n.unfreezeOperations()
				}
			}
		} else {
			if err := n.replicateAllState(); err != nil {
				log.Printf("[Leader %s] Failed to replicate initial state: %v\n", n.id[:8], err)
			}
			n.unfreezeOperations()
		}

		// Start broadcast listener for JOIN messages (only leader needs this)
		n.startBroadcastListener()

		if fromElection {
			go func() {
				defer func() {
					if r := recover(); r != nil {
						buf := make([]byte, 4096)
						stackLen := runtime.Stack(buf, false)
						log.Printf("[Leader %s] Leader duties panic: %v\n%s\n", n.id[:8], r, buf[:stackLen])
					}
				}()
				n.runLeaderDuties()
			}()
		}
	} else if fromElection {
		// Existing leader won a re-election (e.g., split-brain scenario where
		// another node sent an ELECTION ANNOUNCE to us, we froze, then won).
		// We're already the leader, so no state exchange or view install needed -
		// just unfreeze operations that were frozen during the election.
		fmt.Printf("[Leader %s] Re-elected as leader (was already leader), unfreezing operations\n", n.id[:8])
		if n.IsFrozen() {
			n.unfreezeOperations()
		}
	}
}

func (n *Node) becomeFollower() {
	n.mu.Lock()
	wasLeader := n.isLeader
	n.isLeader = false
	n.mu.Unlock()

	if wasLeader {
		fmt.Printf("[Node %s] Becoming FOLLOWER\n", n.id[:8])

		select {
		case n.stopLeaderDuties <- struct{}{}:
		default:
		}

		n.clusterState.RegisterBroker(n.id, n.address, false)

		if n.broadcastListener != nil {
			n.broadcastListener.Close()
			n.broadcastListener = nil
		}

		go n.runFollowerDuties()
	}

	// Unfreeze regardless of previous role. A follower that starts an election
	// also freezes, and needs to unfreeze when it loses. Without this, a follower
	// that loses stays frozen until VIEW_INSTALL arrives (which can take minutes).
	if n.IsFrozen() {
		fmt.Printf("[Node %s] Unfreezing operations after election loss\n", n.id[:8])
		n.unfreezeOperations()
	}
}

// PrintStatus displays current node status
func (n *Node) PrintStatus() {
	fmt.Printf("\n=== Node Status ===\n")
	fmt.Printf("Node ID:       %s\n", n.id[:8])
	fmt.Printf("Address:       %s\n", n.address)
	fmt.Printf("Is Leader:     %v\n", n.IsLeader())
	fmt.Printf("Ring Position: %d\n", n.ringPosition)
	fmt.Printf("Next Node:     %s\n", n.nextNode)

	if n.election.IsInProgress() {
		fmt.Printf("Election:      IN PROGRESS (candidate: %s)\n", n.election.GetCandidate()[:8])
	} else if leader := n.election.GetLeader(); leader != "" {
		fmt.Printf("Election:      Complete (leader: %s)\n", leader[:8])
	} else {
		fmt.Printf("Election:      Not started\n")
	}

	fmt.Printf("\nBroker Count:  %d\n", n.clusterState.GetBrokerCount())
	fmt.Printf("Producer Count: %d\n", n.clusterState.CountProducers())
	fmt.Printf("Consumer Count: %d\n", n.clusterState.CountConsumers())
	n.clusterState.PrintStatus()

	// Print storage and analytics status
	n.PrintStorageStatus()

	// Print phi accrual failure detector status if we're a follower
	if !n.IsLeader() {
		fmt.Println()
		n.printPhiDetectorStatus()
	}
}

func (n *Node) Shutdown() {
	fmt.Printf("[Node %s] Shutting down\n", n.id[:8])

	n.shutdownCancel()

	if n.IsLeader() {
		n.clusterState.RemoveBroker(n.id)
		n.replicateAllState()
	}

	time.Sleep(2 * time.Second)

	close(n.stopLeaderDuties)
	close(n.stopFollowerDuties)

	if n.multicastReceiver != nil {
		n.multicastReceiver.Close()
	}
	if n.multicastSender != nil {
		n.multicastSender.Close()
	}
	if n.broadcastListener != nil {
		n.broadcastListener.Close()
	}
	if n.tcpListener != nil {
		n.tcpListener.Close()
	}
	if n.udpDataListener != nil {
		n.udpDataListener.Close()
	}

	n.closeAllDataLogs()
	fmt.Printf("[Node %s] Shutdown complete\n", n.id[:8])
}

func (n *Node) GetID() string {
	return n.id
}

func (n *Node) GetAddress() string {
	return n.address
}

func (n *Node) GetClusterState() *state.ClusterState {
	return n.clusterState
}

func (n *Node) syncBrokerRing() {
	brokerIDs := n.clusterState.ListBrokers()
	// Rebuild ring from scratch to remove stale entries
	newRing := loadbalance.NewConsistentHashRing()
	for _, brokerID := range brokerIDs {
		newRing.AddNode(brokerID)
	}
	n.brokerRing = newRing
	fmt.Printf("[Node %s] Broker ring synced with %d brokers\n", n.id[:8], len(brokerIDs))
}

func (n *Node) getLeaderIDFromRegistry() string {
	brokers := n.clusterState.ListBrokers()
	for _, brokerID := range brokers {
		broker, ok := n.clusterState.GetBroker(brokerID)
		if !ok {
			continue
		}
		if broker.IsLeader {
			return brokerID
		}
	}
	return ""
}

func (n *Node) printPhiDetectorStatus() {
	leaderID := n.getLeaderIDFromRegistry()

	if leaderID == "" {
		fmt.Println("[Phi Detector] No leader being monitored")
		return
	}

	phi := n.failureDetector.Status(leaderID)
	
	fmt.Printf("[Phi Detector] Leader: %s, Phi: %.4f\n", leaderID[:8], phi)
	fmt.Printf("[Phi Detector] Thresholds - Suspicion: %.1f, Failure: %.1f\n", 
		n.phiSuspicionThreshold, n.phiFailureThreshold)
	
	if phi >= n.phiFailureThreshold {
		fmt.Printf("[Phi Detector] Status: FAILED\n")
	} else if phi >= n.phiSuspicionThreshold {
		fmt.Printf("[Phi Detector] Status: SUSPECTED\n")
	} else {
		fmt.Printf("[Phi Detector] Status: HEALTHY\n")
	}
}

func (n *Node) GetBrokerForTopic(topic string) (brokerID string, brokerAddress string, err error) {
	if stream, exists := n.clusterState.GetStreamAssignment(topic); exists {
		fmt.Printf("[Node %s] GetBrokerForTopic(%s): Using existing stream assignment -> broker=%s, addr=%s\n",
			n.id[:8], topic, stream.AssignedBrokerId[:8], stream.BrokerAddress)
		return stream.AssignedBrokerId, stream.BrokerAddress, nil
	}

	// If this node is the leader and there are followers, delegate data processing
	// to followers by excluding the leader from selection.
	// Leader only handles data when it's the sole node in the cluster.
	if n.IsLeader() && n.brokerRing.NodeCount() > 1 {
		brokerID = n.brokerRing.GetNodeExcluding(topic, n.id)
		fmt.Printf("[Node %s] GetBrokerForTopic(%s): Leader delegating to follower (ring has %d nodes)\n",
			n.id[:8], topic, n.brokerRing.NodeCount())
	} else {
		brokerID = n.brokerRing.GetNode(topic)
	}

	if brokerID == "" {
		return "", "", fmt.Errorf("no brokers available for topic assignment")
	}

	broker, ok := n.clusterState.GetBroker(brokerID)
	if !ok {
		return "", "", fmt.Errorf("broker %s not found in cluster state", brokerID[:8])
	}

	fmt.Printf("[Node %s] GetBrokerForTopic(%s): Ring selected broker=%s, addr=%s\n",
		n.id[:8], topic, brokerID[:8], broker.Address)
	return brokerID, broker.Address, nil
}
