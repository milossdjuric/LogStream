package node

import (
	"context"
	"fmt"
	"log"
	"net"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/milossdjuric/logstream/internal/config"
	"github.com/milossdjuric/logstream/internal/protocol"
	"github.com/milossdjuric/logstream/internal/state"
	"github.com/milossdjuric/logstream/internal/storage"
	"golang.org/x/sys/unix"
)

// Node represents a node in the cluster (can be leader or follower)
type Node struct {
	// Identity
	id      string
	address string
	config  *config.Config

	// Role (can change during runtime via election)
	mu       sync.RWMutex
	isLeader bool

	// State (replicated across all nodes) - Combined state for brokers + producers + consumers
	clusterState  *state.ClusterState
	holdbackQueue *state.RegistryHoldbackQueue

	// Track last replicated sequence to avoid sending duplicates
	lastReplicatedSeq int64

	// Track last applied sequence (for view-sync recovery)
	lastAppliedSeqNum int64

	// Election state
	election     *state.ElectionState
	ringPosition int    // Position in logical ring
	nextNode     string // Address of next node in ring

	// Leader address (stored from JOIN_RESPONSE for immediate heartbeats)
	leaderAddress string // Leader address known from discovery, used before registry sync

	// Leader failure detection (only used by followers)
	failureDetector *state.LeaderFailureDetector

	// Network connections (protocol package provides these)
	multicastReceiver *protocol.MulticastConnection
	multicastSender   *protocol.MulticastConnection
	broadcastListener *protocol.BroadcastConnection
	tcpListener       net.Listener

	// Control channels for role-specific goroutines
	stopLeaderDuties   chan struct{}
	stopFollowerDuties chan struct{}

	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc

	// Persistent log storage
	storageLog *storage.Log
}

// NewNode creates a new node
func NewNode(cfg *config.Config) *Node {
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())

	node := &Node{
		id:                 protocol.GenerateNodeID(cfg.NodeAddress),
		address:            cfg.NodeAddress,
		config:             cfg,
		isLeader:           false, // Default to follower, determined during discovery
		clusterState:       state.NewClusterState(),
		lastReplicatedSeq:  0,
		lastAppliedSeqNum:  0,
		election:           state.NewElectionState(),
		stopLeaderDuties:   make(chan struct{}),
		stopFollowerDuties: make(chan struct{}),
		shutdownCtx:        shutdownCtx,
		shutdownCancel:     shutdownCancel,
		// Initialize failure detector with timeouts:
		// - Suspicion after 15 seconds without heartbeat
		// - Failure after 20 seconds without heartbeat
		failureDetector: state.NewLeaderFailureDetector(15*time.Second, 20*time.Second),
	}

	// Initialize holdback queue for FIFO registry updates
	node.holdbackQueue = state.NewRegistryHoldbackQueue(node.applyRegistryUpdate)

	return node
}

// Start initializes the node (discovers cluster and starts appropriate role)
// First node automatically declares itself leader if no cluster exists
func (n *Node) Start() error {
	fmt.Printf("\n[Node %s] ========================================\n", n.id[:8])
	fmt.Printf("[Node %s] NODE STARTUP INITIATED\n", n.id[:8])
	fmt.Printf("[Node %s] Address: %s\n", n.id[:8], n.address)
	fmt.Printf("[Node %s] Node ID: %s\n", n.id[:8], n.id)
	fmt.Printf("[Node %s] ========================================\n\n", n.id[:8])

	// Initialize persistent storage log
	fmt.Printf("[Node %s] Initializing persistent storage log...\n", n.id[:8])
	logDir := fmt.Sprintf("./logs/%s", n.id[:8])
	storageLog, err := storage.NewLog(logDir, 0) // 0 = use default max segment size
	if err != nil {
		fmt.Printf("[Node %s] WARNING: Failed to initialize storage log: %v\n", n.id[:8], err)
		fmt.Printf("[Node %s] Continuing without persistent storage\n", n.id[:8])
	} else {
		n.storageLog = storageLog
		fmt.Printf("[Node %s] Storage log initialized at %s\n", n.id[:8], logDir)
	}

	// Step 1: Try to discover existing cluster
	fmt.Printf("[Node %s] STEP 1: Discovering cluster...\n", n.id[:8])
	fmt.Printf("[Node %s] [Step-1] Calling discoverCluster()...\n", n.id[:8])
	err = n.discoverCluster()
	if err != nil {
		// No cluster found - we're the first node, automatically become leader
		fmt.Printf("[Node %s] [Step-1] discoverCluster() returned error: %v\n", n.id[:8], err)
		fmt.Printf("[Node %s] [Step-1] No existing cluster found, automatically declaring myself leader\n", n.id[:8])
		fmt.Printf("[Node %s] [Step-1] Calling becomeLeader()...\n", n.id[:8])
		n.becomeLeader()
		fmt.Printf("[Node %s] [Step-1] becomeLeader() completed\n", n.id[:8])
	} else {
		// Cluster found - we're a follower
		fmt.Printf("[Node %s] [Step-1] discoverCluster() succeeded - cluster found\n", n.id[:8])
		fmt.Printf("[Node %s] [Step-1] Joined existing cluster as follower\n", n.id[:8])
		fmt.Printf("[Node %s] [Step-1] Calling becomeFollower()...\n", n.id[:8])
		n.becomeFollower()
		fmt.Printf("[Node %s] [Step-1] becomeFollower() completed\n", n.id[:8])
	}
	fmt.Printf("[Node %s] [Step-1] Step 1 complete\n", n.id[:8])

	// Step 2: Setup network connections (same for both roles)
	fmt.Printf("[Node %s] STEP 2: Setting up multicast connections...\n", n.id[:8])
	if err := n.setupMulticast(); err != nil {
		fmt.Printf("[Node %s] ERROR: Multicast setup failed: %v\n", n.id[:8], err)
		return err
	}
	fmt.Printf("[Node %s] Multicast setup complete\n", n.id[:8])

	// Step 3: Start TCP listener (ALL nodes need this for election messages)
	fmt.Printf("[Node %s] STEP 3: Starting TCP listener...\n", n.id[:8])
	if err := n.startTCPListener(); err != nil {
		fmt.Printf("[Node %s] ERROR: TCP listener failed: %v\n", n.id[:8], err)
		return fmt.Errorf("failed to start TCP listener: %w", err)
	}
	fmt.Printf("[Node %s] TCP listener started successfully\n", n.id[:8])

	// Step 4: Start multicast listener (both roles need this)
	fmt.Printf("[Node %s] STEP 4: Starting multicast listener goroutine...\n", n.id[:8])
	go func() {
		defer func() {
			if r := recover(); r != nil {
				buf := make([]byte, 4096)
				stackLen := runtime.Stack(buf, false)
				fmt.Printf("[Node %s] [Multicast-Listener] PANIC RECOVERED: %v\nStack:\n%s\n", n.id[:8], r, buf[:stackLen])
				log.Printf("[Node %s] [Multicast-Listener] PANIC: %v\nStack:\n%s\n", n.id[:8], r, buf[:stackLen])
			}
		}()
		n.listenMulticast()
	}()
	fmt.Printf("[Node %s] Multicast listener goroutine started\n", n.id[:8])

	// Step 5: Start role-specific goroutines
	fmt.Printf("[Node %s] STEP 5: Starting role-specific duties...\n", n.id[:8])
	if n.IsLeader() {
		fmt.Printf("[Node %s] Starting leader duties goroutine...\n", n.id[:8])
		go func() {
			defer func() {
				if r := recover(); r != nil {
					buf := make([]byte, 4096)
					stackLen := runtime.Stack(buf, false)
					fmt.Printf("[Leader %s] [Leader-Duties] PANIC RECOVERED: %v\nStack:\n%s\n", n.id[:8], r, buf[:stackLen])
					log.Printf("[Leader %s] [Leader-Duties] PANIC: %v\nStack:\n%s\n", n.id[:8], r, buf[:stackLen])
				}
			}()
			n.runLeaderDuties()
		}()
		fmt.Printf("[Node %s] Leader duties goroutine started\n", n.id[:8])
	} else {
		fmt.Printf("[Node %s] Starting follower duties goroutine...\n", n.id[:8])
		go func() {
			defer func() {
				if r := recover(); r != nil {
					buf := make([]byte, 4096)
					stackLen := runtime.Stack(buf, false)
					fmt.Printf("[Follower %s] [Follower-Duties] PANIC RECOVERED: %v\nStack:\n%s\n", n.id[:8], r, buf[:stackLen])
					log.Printf("[Follower %s] [Follower-Duties] PANIC: %v\nStack:\n%s\n", n.id[:8], r, buf[:stackLen])
				}
			}()
			n.runFollowerDuties()
		}()
		fmt.Printf("[Node %s] Follower duties goroutine started\n", n.id[:8])
	}

	fmt.Printf("\n[Node %s] ========================================\n", n.id[:8])
	fmt.Printf("[Node %s] NODE STARTUP COMPLETE\n", n.id[:8])
	fmt.Printf("[Node %s] Address: %s\n", n.id[:8], n.address)
	fmt.Printf("[Node %s] Role: %s\n", n.id[:8], map[bool]string{true: "LEADER", false: "FOLLOWER"}[n.IsLeader()])
	fmt.Printf("[Node %s] ========================================\n\n", n.id[:8])
	return nil
}


// IsLeader returns current role (thread-safe)
func (n *Node) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.isLeader
}

// becomeLeader transitions this node to leader role
func (n *Node) becomeLeader() {
	fmt.Printf("[Node %s] [becomeLeader] Entering function...\n", n.id[:8])
	
	n.mu.Lock()
	wasLeader := n.isLeader
	n.isLeader = true
	n.mu.Unlock()
	fmt.Printf("[Node %s] [becomeLeader] Lock acquired, wasLeader=%v\n", n.id[:8], wasLeader)

	if !wasLeader {
		fmt.Printf("\n[Node %s] ========================================\n", n.id[:8])
		fmt.Printf("[Node %s] Becoming LEADER\n", n.id[:8])
		fmt.Printf("[Node %s] ========================================\n\n", n.id[:8])

		// Stop follower duties if running
		fmt.Printf("[Node %s] [becomeLeader] Stopping follower duties...\n", n.id[:8])
		select {
		case n.stopFollowerDuties <- struct{}{}:
			fmt.Printf("[Node %s] [becomeLeader] Follower duties stop signal sent\n", n.id[:8])
		default:
			fmt.Printf("[Node %s] [becomeLeader] No follower duties to stop\n", n.id[:8])
		}

		// Update registry
		fmt.Printf("[Node %s] [becomeLeader] Registering broker in cluster state...\n", n.id[:8])
		n.clusterState.RegisterBroker(n.id, n.address, true)
		// Update stored leader address (we are now the leader)
		n.leaderAddress = n.address
		fmt.Printf("[Node %s] [becomeLeader] Broker registered successfully\n", n.id[:8])

		// Initialize last replicated sequence
		fmt.Printf("[Node %s] [becomeLeader] Initializing replication sequence...\n", n.id[:8])
		n.lastReplicatedSeq = n.clusterState.GetSequenceNum()
		fmt.Printf("[Node %s] [becomeLeader] Replication sequence initialized: %d\n", n.id[:8], n.lastReplicatedSeq)

		// Replicate initial leader state
		fmt.Printf("[Node %s] [becomeLeader] Replicating initial state...\n", n.id[:8])
		if err := n.replicateAllState(); err != nil {
			fmt.Printf("[Leader %s] [becomeLeader] ERROR: Failed to replicate initial state: %v\n", n.id[:8], err)
			log.Printf("[Leader %s] Failed to replicate initial state: %v\n", n.id[:8], err)
		} else {
			fmt.Printf("[Node %s] [becomeLeader] Initial state replicated successfully\n", n.id[:8])
		}

		// Reset failure detector since we're now the leader
		fmt.Printf("[Node %s] [becomeLeader] Resetting failure detector...\n", n.id[:8])
		n.failureDetector.Reset(n.id)
		fmt.Printf("[Node %s] [becomeLeader] Failure detector reset\n", n.id[:8])

		// Start broadcast listener for new nodes joining
		fmt.Printf("[Node %s] [becomeLeader] Checking broadcast listener...\n", n.id[:8])
		if n.broadcastListener == nil {
			fmt.Printf("[Node %s] [becomeLeader] Creating broadcast listener on port %d...\n", n.id[:8], n.config.BroadcastPort)
			listener, err := protocol.CreateBroadcastListener(n.config.BroadcastPort)
			if err != nil {
				fmt.Printf("[Node %s] [becomeLeader] ERROR: Failed to start broadcast listener: %v\n", n.id[:8], err)
				log.Printf("[Node %s] Failed to start broadcast listener: %v\n", n.id[:8], err)
			} else {
				n.broadcastListener = listener
				fmt.Printf("[Node %s] [becomeLeader] Starting broadcast join listener goroutine...\n", n.id[:8])
				go func() {
					defer func() {
						if r := recover(); r != nil {
							buf := make([]byte, 4096)
							stackLen := runtime.Stack(buf, false)
							fmt.Printf("[Node %s] [Broadcast-Listener] PANIC RECOVERED: %v\nStack:\n%s\n", n.id[:8], r, buf[:stackLen])
							log.Printf("[Node %s] [Broadcast-Listener] PANIC: %v\nStack:\n%s\n", n.id[:8], r, buf[:stackLen])
						}
					}()
					n.listenForBroadcastJoins()
				}()
				fmt.Printf("[Leader %s] Broadcast listener ready on port %d\n", n.id[:8], n.config.BroadcastPort)
			}
		} else {
			fmt.Printf("[Node %s] [becomeLeader] Broadcast listener already exists\n", n.id[:8])
		}

		fmt.Printf("[Node %s] [becomeLeader] Complete\n", n.id[:8])
	} else {
		fmt.Printf("[Node %s] [becomeLeader] Already leader, skipping\n", n.id[:8])
	}
}

// becomeFollower transitions this node to follower role
func (n *Node) becomeFollower() {
	fmt.Printf("[Node %s] [becomeFollower] Entering function...\n", n.id[:8])
	
	n.mu.Lock()
	wasLeader := n.isLeader
	n.isLeader = false
	n.mu.Unlock()
	fmt.Printf("[Node %s] [becomeFollower] Lock acquired, wasLeader=%v\n", n.id[:8], wasLeader)

	if wasLeader {
		fmt.Printf("\n[Node %s] ========================================\n", n.id[:8])
		fmt.Printf("[Node %s] Becoming FOLLOWER\n", n.id[:8])
		fmt.Printf("[Node %s] ========================================\n\n", n.id[:8])

		// Stop leader duties
		fmt.Printf("[Node %s] [becomeFollower] Stopping leader duties...\n", n.id[:8])
		select {
		case n.stopLeaderDuties <- struct{}{}:
			fmt.Printf("[Node %s] [becomeFollower] Leader duties stop signal sent\n", n.id[:8])
		default:
			fmt.Printf("[Node %s] [becomeFollower] No leader duties to stop\n", n.id[:8])
		}

		// Update registry
		fmt.Printf("[Node %s] [becomeFollower] Registering broker as follower in cluster state...\n", n.id[:8])
		n.clusterState.RegisterBroker(n.id, n.address, false)
		fmt.Printf("[Node %s] [becomeFollower] Broker registered as follower successfully\n", n.id[:8])

		// Stop broadcast listener
		fmt.Printf("[Node %s] [becomeFollower] Stopping broadcast listener...\n", n.id[:8])
		if n.broadcastListener != nil {
			n.broadcastListener.Close()
			n.broadcastListener = nil
			fmt.Printf("[Node %s] [becomeFollower] Broadcast listener stopped\n", n.id[:8])
		} else {
			fmt.Printf("[Node %s] [becomeFollower] No broadcast listener to stop\n", n.id[:8])
		}

		fmt.Printf("[Node %s] [becomeFollower] Complete\n", n.id[:8])
	} else {
		fmt.Printf("[Node %s] [becomeFollower] Already follower, skipping\n", n.id[:8])
	}

	// NOTE: We don't initialize failure detector here because the registry
	// is not yet synchronized. It will be initialized in applyRegistryUpdate()
	// after the first REPLICATE message arrives.
	fmt.Printf("[Node %s] [becomeFollower] Failure detector will be initialized after first REPLICATE message\n", n.id[:8])
}

// initializeFailureDetector sets up the failure detector with the current leader
func (n *Node) initializeFailureDetector() {
	fmt.Printf("[FailureDetector] ========================================\n")
	fmt.Printf("[FailureDetector] Attempting to initialize failure detector...\n")
	
	// Get current leader from registry
	brokers := n.clusterState.ListBrokers()
	fmt.Printf("[FailureDetector] Registry has %d brokers: %v\n", len(brokers), brokers)

	for _, brokerID := range brokers {
		broker, ok := n.clusterState.GetBroker(brokerID)
		if !ok {
			fmt.Printf("[FailureDetector] Broker %s not found in registry map\n", brokerID[:8])
			continue
		}

		fmt.Printf("[FailureDetector] Checking broker %s: isLeader=%v, addr=%s\n",
			brokerID[:8], broker.IsLeader, broker.Address)

		if broker.IsLeader {
			fmt.Printf("[FailureDetector] Found leader: %s\n", brokerID[:8])
			fmt.Printf("[FailureDetector] Initializing failure detector with leader: %s\n", brokerID[:8])
			n.failureDetector.Reset(brokerID)
			fmt.Printf("[FailureDetector] Successfully initialized failure detector\n")
			fmt.Printf("[FailureDetector] ========================================\n")
			return
		}
	}

	fmt.Printf("[FailureDetector] WARNING: No leader found in registry!\n")
	fmt.Printf("[FailureDetector] Registry contents:\n")
	for _, brokerID := range brokers {
		if broker, ok := n.clusterState.GetBroker(brokerID); ok {
			fmt.Printf("[FailureDetector]   %s: leader=%v, addr=%s\n",
				brokerID[:8], broker.IsLeader, broker.Address)
		}
	}
	fmt.Printf("[FailureDetector] ========================================\n")
}


func (n *Node) discoverCluster() error {
	fmt.Printf("[Node %s] Discovering cluster...\n", n.id[:8])

	config := protocol.DefaultBroadcastConfig()
	config.MaxRetries = 3  // Increased from 1 to 3 for better reliability
	config.Timeout = 3 * time.Second  // Increased from 2s to 3s per attempt

	response, err := protocol.DiscoverClusterWithRetry(
		n.id,
		protocol.NodeType_BROKER,
		n.address,
		config,
	)

	if err != nil {
		return err
	}

	fmt.Printf("[Node %s] Found cluster! Leader: %s\n", n.id[:8], response.LeaderAddress)
	n.config.MulticastGroup = response.MulticastGroup
	
	// Store leader address immediately so we can send heartbeats before registry sync
	n.leaderAddress = response.LeaderAddress
	fmt.Printf("[Node %s] Stored leader address for immediate heartbeats: %s\n", n.id[:8], n.leaderAddress)

	return nil
}

func (n *Node) setupMulticast() error {
	fmt.Printf("[Node %s] [Multicast-Setup] Joining multicast group: %s\n", n.id[:8], n.config.MulticastGroup)
	fmt.Printf("[Node %s] [Multicast-Setup] Network interface: %s\n", n.id[:8], n.config.NetworkInterface)
	
	// protocol.JoinMulticastGroup provided by protocol package
	receiver, err := protocol.JoinMulticastGroup(
		n.config.MulticastGroup,
		n.config.NetworkInterface,
	)
	if err != nil {
		fmt.Printf("[Node %s] [Multicast-Setup] ERROR: Failed to join multicast group: %v\n", n.id[:8], err)
		return fmt.Errorf("failed to join multicast: %w", err)
	}
	n.multicastReceiver = receiver
	fmt.Printf("[Node %s] [Multicast-Setup] Successfully joined multicast group\n", n.id[:8])
	fmt.Printf("[Node %s] [Multicast-Setup] Receiver local address: %s\n", n.id[:8], receiver.GetLocalAddr())

	// protocol.CreateMulticastSender provided by protocol package
	senderAddr := fmt.Sprintf("%s:0", n.config.NetworkInterface)
	fmt.Printf("[Node %s] [Multicast-Setup] Creating multicast sender on: %s\n", n.id[:8], senderAddr)
	sender, err := protocol.CreateMulticastSender(senderAddr)
	if err != nil {
		fmt.Printf("[Node %s] [Multicast-Setup] ERROR: Failed to create multicast sender: %v\n", n.id[:8], err)
		n.multicastReceiver.Close()
		return fmt.Errorf("failed to create multicast sender: %w", err)
	}
	n.multicastSender = sender
	fmt.Printf("[Node %s] [Multicast-Setup] Successfully created multicast sender\n", n.id[:8])
	fmt.Printf("[Node %s] [Multicast-Setup] Sender local address: %s\n", n.id[:8], sender.GetLocalAddr())

	return nil
}

// Start TCP listener for producer/consumer connections AND election messages
func (n *Node) startTCPListener() error {
	// Extract port from node address
	host, port, err := net.SplitHostPort(n.address)
	if err != nil {
		return fmt.Errorf("invalid address: %w", err)
	}

	// Use same port for TCP
	tcpAddr := fmt.Sprintf("%s:%s", host, port)

	// Use ListenConfig to set SO_REUSEADDR for immediate port reuse after cleanup
	lc := &net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var opErr error
			err := c.Control(func(fd uintptr) {
				// SO_REUSEADDR: Allows reusing addresses in TIME_WAIT state
				// This is critical for VMs where cleanup might leave ports in TIME_WAIT
				opErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
			})
			if err != nil {
				return err
			}
			return opErr
		},
	}

	// Listen for TCP connections with reuse option
	listener, err := lc.Listen(context.Background(), "tcp", tcpAddr)
	if err != nil {
		return fmt.Errorf("failed to start TCP listener: %w", err)
	}

	n.tcpListener = listener
	fmt.Printf("[Node %s] TCP listener started on %s (SO_REUSEADDR enabled)\n", n.id[:8], listener.Addr())

	// Accept connections in background
	go func() {
		defer func() {
			if r := recover(); r != nil {
				buf := make([]byte, 4096)
				stackLen := runtime.Stack(buf, false)
				fmt.Printf("[Node %s] [TCP-Listener] PANIC RECOVERED: %v\nStack:\n%s\n", n.id[:8], r, buf[:stackLen])
				log.Printf("[Node %s] [TCP-Listener] PANIC: %v\nStack:\n%s\n", n.id[:8], r, buf[:stackLen])
			}
		}()
		n.acceptTCPConnections()
	}()

	return nil
}

func (n *Node) acceptTCPConnections() {
	fmt.Printf("[Node %s] [TCP-Listener] Starting accept loop on %s\n", n.id[:8], n.tcpListener.Addr())
	
	for {
		// Check for shutdown before accepting
		select {
		case <-n.shutdownCtx.Done():
			fmt.Printf("[Node %s] [TCP-Listener] Shutdown requested, exiting accept loop cleanly\n", n.id[:8])
			return
		default:
			// Continue with accept
		}
		
		// Set a deadline to allow periodic shutdown checks
		if tcpListener, ok := n.tcpListener.(*net.TCPListener); ok {
			tcpListener.SetDeadline(time.Now().Add(1 * time.Second))
		}
		
		conn, err := n.tcpListener.Accept()
		if err != nil {
			// Check if it's a timeout (normal when checking for shutdown)
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue // Check shutdown and try again
			}
			
			// Check if listener was closed (shutdown scenario)
			if err.Error() == "use of closed network connection" {
				fmt.Printf("[Node %s] [TCP-Listener] Listener closed, exiting accept loop cleanly\n", n.id[:8])
				return // Clean exit, not a crash
			}
			
			// Check if we've been shut down
			select {
			case <-n.shutdownCtx.Done():
				fmt.Printf("[Node %s] [TCP-Listener] Shutdown detected during accept error, exiting cleanly\n", n.id[:8])
				return
			default:
				// Not shutdown - log the error but don't crash
				fmt.Printf("[Node %s] [TCP-Listener] Accept error (will retry): %v\n", n.id[:8], err)
				log.Printf("[Node %s] TCP accept error: %v\n", n.id[:8], err)
				time.Sleep(100 * time.Millisecond) // Brief pause before retry
				continue
			}
		}

		// Clear deadline for accepted connection
		if tcpListener, ok := n.tcpListener.(*net.TCPListener); ok {
			tcpListener.SetDeadline(time.Time{})
		}

		remoteAddr := conn.RemoteAddr()
		fmt.Printf("[Node %s] [TCP-Listener] New connection accepted from %s\n", n.id[:8], remoteAddr)
		
		// Handle connection in goroutine
		go n.handleTCPConnection(conn)
	}
}

func (n *Node) handleTCPConnection(conn net.Conn) {
	remoteAddr := conn.RemoteAddr()
	fmt.Printf("[Node %s] [TCP-Connection] Handling connection from %s\n", n.id[:8], remoteAddr)
	
	// Track whether connection should be kept open (for CONSUME requests)
	keepConnectionOpen := false
	defer func() {
		if !keepConnectionOpen {
			conn.Close()
			fmt.Printf("[Node %s] [TCP-Connection] Connection from %s closed\n", n.id[:8], remoteAddr)
		} else {
			fmt.Printf("[Node %s] [TCP-Connection] Connection from %s kept open for streaming\n", n.id[:8], remoteAddr)
		}
	}()

	// Read message
	fmt.Printf("[Node %s] [TCP-Connection] Reading message from %s...\n", n.id[:8], remoteAddr)
	msg, err := protocol.ReadTCPMessage(conn)
	if err != nil {
		fmt.Printf("[Node %s] [TCP-Connection] ERROR: Failed to read TCP message from %s: %v\n", n.id[:8], remoteAddr, err)
		log.Printf("[Node %s] Failed to read TCP message: %v\n", n.id[:8], err)
		return
	}

	msgType := msg.GetHeader().Type
	senderID := msg.GetHeader().SenderId
	fmt.Printf("[Node %s] [TCP-Connection] Received %s message from %s (sender: %s)\n", 
		n.id[:8], msgType, remoteAddr, senderID[:8])

	// Route based on message type
	switch m := msg.(type) {
	case *protocol.ProduceMsg:
		fmt.Printf("[Node %s] [TCP-Connection] Routing PRODUCE message to handler\n", n.id[:8])
		n.handleProduceRequest(m, conn)

	case *protocol.ConsumeMsg:
		fmt.Printf("[Node %s] [TCP-Connection] Routing CONSUME message to handler\n", n.id[:8])
		n.handleConsumeRequest(m, conn)
		// CONSUME requests need connection to stay open for streaming results
		keepConnectionOpen = true

	case *protocol.HeartbeatMsg:
		fmt.Printf("[Node %s] [TCP-Connection] Routing HEARTBEAT message to handler\n", n.id[:8])
		n.handleTCPHeartbeat(m, conn)

	case *protocol.ElectionMsg:
		fmt.Printf("[Node %s] [TCP-Connection] Routing ELECTION message to handler\n", n.id[:8])
		fmt.Printf("[Node %s] [TCP-Connection] Election message details: candidate=%s, electionID=%d, phase=%v\n",
			n.id[:8], m.CandidateId[:8], m.ElectionId, m.Phase)
		n.handleElection(m, nil)

	default:
		fmt.Printf("[Node %s] [TCP-Connection] WARNING: Unknown TCP message type: %T\n", n.id[:8], m)
		log.Printf("[Node %s] Unknown TCP message type: %T\n", n.id[:8], m)
	}
}


// Handle PRODUCE request from producer
func (n *Node) handleProduceRequest(msg *protocol.ProduceMsg, conn net.Conn) {
	producerID := protocol.GetSenderID(msg)
	topic := msg.Topic
	address := msg.ProducerAddress

	fmt.Printf("[Leader %s] <- PRODUCE from %s (topic: %s, addr: %s)\n",
		n.id[:8], producerID[:8], topic, address)

	// Register producer
	if err := n.clusterState.RegisterProducer(producerID, address, topic); err != nil {
		log.Printf("[Leader %s] Failed to register producer: %v\n", n.id[:8], err)
		// Send failure response
		ack := &protocol.ProduceMessage{
			Header: &protocol.MessageHeader{
				Type:        protocol.MessageType_PRODUCE,
				Timestamp:   time.Now().UnixNano(),
				SenderId:    n.id,
				SequenceNum: 0,
				SenderType:  protocol.NodeType_LEADER,
			},
			Topic:           topic,
			ProducerAddress: "",
		}
		protocol.WriteTCPMessage(conn, &protocol.ProduceMsg{ProduceMessage: ack})
		return
	}

	// TODO: Select broker using rendezvous hashing
	// For now, assign to self (leader)
	assignedBroker := n.address

	// Send success response with assigned broker
	ack := &protocol.ProduceMessage{
		Header: &protocol.MessageHeader{
			Type:        protocol.MessageType_PRODUCE,
			Timestamp:   time.Now().UnixNano(),
			SenderId:    n.id,
			SequenceNum: 0,
			SenderType:  protocol.NodeType_LEADER,
		},
		Topic:           topic,
		ProducerAddress: assignedBroker,
	}

	if err := protocol.WriteTCPMessage(conn, &protocol.ProduceMsg{ProduceMessage: ack}); err != nil {
		log.Printf("[Leader %s] Failed to send PRODUCE_ACK: %v\n", n.id[:8], err)
		return
	}

	fmt.Printf("[Leader %s] -> PRODUCE_ACK to %s (assigned broker: %s)\n",
		n.id[:8], producerID[:8], assignedBroker)
}

// Handle CONSUME request from consumer
func (n *Node) handleConsumeRequest(msg *protocol.ConsumeMsg, conn net.Conn) {
	consumerID := protocol.GetSenderID(msg)
	topic := msg.Topic
	address := msg.ConsumerAddress

	fmt.Printf("[Leader %s] <- CONSUME from %s (topic: %s, addr: %s)\n",
		n.id[:8], consumerID[:8], topic, address)

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

	// Subscribe consumer to topic
	if err := n.clusterState.SubscribeConsumer(consumerID, topic); err != nil {
		log.Printf("[Leader %s] Failed to subscribe consumer: %v\n", n.id[:8], err)
		return
	}

	// Replicate state change immediately
	if err := n.replicateAllState(); err != nil {
		log.Printf("[Leader %s] Failed to replicate after consumer subscription: %v\n", n.id[:8], err)
	}

	// Send success response
	ack := &protocol.ConsumeMessage{
		Header: &protocol.MessageHeader{
			Type:        protocol.MessageType_CONSUME,
			Timestamp:   time.Now().UnixNano(),
			SenderId:    n.id,
			SequenceNum: 0,
			SenderType:  protocol.NodeType_LEADER,
		},
		Topic:           topic,
		ConsumerAddress: n.address,
	}

	if err := protocol.WriteTCPMessage(conn, &protocol.ConsumeMsg{ConsumeMessage: ack}); err != nil {
		log.Printf("[Leader %s] Failed to send CONSUME_ACK: %v\n", n.id[:8], err)
		return
	}

	fmt.Printf("[Leader %s] -> CONSUME_ACK to %s\n", n.id[:8], consumerID[:8])

	// Keep connection open and start sending results
	go n.streamResultsToConsumer(consumerID, topic, conn)
}

// Stream results to consumer
func (n *Node) streamResultsToConsumer(consumerID, topic string, conn net.Conn) {
	fmt.Printf("[Leader %s] Started result stream to consumer %s (topic: %s)\n",
		n.id[:8], consumerID[:8], topic)

	defer func() {
		conn.Close()
		fmt.Printf("[Leader %s] Closed connection to consumer %s\n", n.id[:8], consumerID[:8])
	}()

	// Listen for data messages and forward to consumer
	// For now, keep connection alive and send heartbeats
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-n.shutdownCtx.Done():
			fmt.Printf("[Leader %s] Shutdown requested, closing consumer stream for %s\n", n.id[:8], consumerID[:8])
			return

		case <-ticker.C:
			// Update consumer heartbeat
			n.clusterState.UpdateConsumerHeartbeat(consumerID)
		}
	}
}


func (n *Node) listenMulticast() {
	fmt.Printf("[Node %s] [Multicast-Listener] ========================================\n", n.id[:8])
	fmt.Printf("[Node %s] [Multicast-Listener] Starting multicast message listener...\n", n.id[:8])
	fmt.Printf("[Node %s] [Multicast-Listener] Receiver: %v\n", n.id[:8], n.multicastReceiver != nil)
	fmt.Printf("[Node %s] [Multicast-Listener] Shutdown context: %v\n", n.id[:8], n.shutdownCtx != nil)
	fmt.Printf("[Node %s] [Multicast-Listener] ========================================\n", n.id[:8])

	errorCount := 0
	maxConsecutiveErrors := 10

	for {
		// Check for shutdown
		select {
		case <-n.shutdownCtx.Done():
			fmt.Printf("[Node %s] [Multicast-Listener] Shutdown requested, exiting cleanly\n", n.id[:8])
			return
		default:
			// Continue with normal operation
		}

		// Set read timeout to allow periodic shutdown checks
		// Use ReadMessageWithTimeout for better control
		msg, sender, err := n.multicastReceiver.ReadMessageWithTimeout(1 * time.Second)

		if err != nil {
			// Check if it's just a timeout (normal when checking for shutdown)
			// Timeouts are expected when no other nodes are sending messages
			errStr := err.Error()
			
			// Check for timeout errors - use multiple methods to catch all cases
			isTimeout := false
			
			// Method 1: Check if error implements net.Error and is a timeout
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				isTimeout = true
			}
			
			// Method 2: Check error string for timeout patterns (some errors don't implement net.Error correctly)
			// Use Contains to catch wrapped errors (e.g., "failed to read UDP message: read udp4 0.0.0.0:9999: i/o timeout")
			if strings.Contains(errStr, "i/o timeout") || 
			   strings.Contains(errStr, "deadline exceeded") ||
			   strings.Contains(errStr, "read udp") && strings.Contains(errStr, "timeout") {
				isTimeout = true
			}
			
			if isTimeout {
				continue // Normal timeout, don't log - just check shutdown and try again
			}

			// Check if it's just "not a protobuf message" - this is normal network noise
			if errStr == "not a protobuf message" {
				continue
			}

			// Check if connection was closed (shutdown scenario)
			if err.Error() == "use of closed network connection" ||
				err.Error() == "read udp4 0.0.0.0:9999: use of closed network connection" {
				fmt.Printf("[Node %s] [Multicast-Listener] Connection closed, exiting listener\n", n.id[:8])
				return // Clean exit, not a crash
			}

			// Other errors - log and count
			errorCount++
			if errorCount <= maxConsecutiveErrors {
				fmt.Printf("[Node %s] [Multicast-Listener] ERROR (%d/%d): %v\n",
					n.id[:8], errorCount, maxConsecutiveErrors, err)
				time.Sleep(100 * time.Millisecond) // Brief pause before retry
				continue
			} else {
				// Too many errors - something is seriously wrong
				fmt.Printf("[Node %s] [Multicast-Listener] FATAL: Too many consecutive errors, exiting\n", n.id[:8])
				log.Printf("[Node %s] Multicast listener failed after %d errors, last error: %v\n",
					n.id[:8], errorCount, err)
				return
			}
		}

		// Successful read - reset error count
		errorCount = 0

		msgType := msg.GetHeader().Type
		senderID := msg.GetHeader().SenderId
		fmt.Printf("[Node %s] [Multicast-Listener] Received %s from %s (sender: %s, addr: %s)\n",
			n.id[:8], msgType, senderID[:8], senderID[:8], sender)

		// Route message based on type (protocol provides message types)
		switch m := msg.(type) {
		case *protocol.HeartbeatMsg:
			fmt.Printf("[Node %s] [Multicast-Listener] Routing HEARTBEAT to handler\n", n.id[:8])
			n.handleHeartbeat(msg, sender)

		case *protocol.ReplicateMsg:
			fmt.Printf("[Node %s] [Multicast-Listener] Routing REPLICATE to handler\n", n.id[:8])
			n.handleReplication(m, sender)

		case *protocol.ElectionMsg:
			fmt.Printf("[Node %s] [Multicast-Listener] Routing ELECTION to handler\n", n.id[:8])
			fmt.Printf("[Node %s] [Multicast-Listener] Election details: candidate=%s, electionID=%d, phase=%v\n",
				n.id[:8], m.CandidateId[:8], m.ElectionId, m.Phase)
			n.handleElection(m, sender)

		case *protocol.NackMsg:
			fmt.Printf("[Node %s] [Multicast-Listener] Routing NACK to handler\n", n.id[:8])
			n.handleNack(m, sender)

		case *protocol.DataMsg:
			fmt.Printf("[Node %s] [Multicast-Listener] Routing DATA to handler\n", n.id[:8])
			n.handleData(m, sender)
		}
	}
}


func (n *Node) handleHeartbeat(msg protocol.Message, sender *net.UDPAddr) {
	senderID := protocol.GetSenderID(msg)
	senderType := protocol.GetSenderType(msg)

	fmt.Printf("[%s] <- HEARTBEAT from %s (sender: %s, type: %s)\n",
		n.id[:8], senderID[:8], sender, senderType)

	// Split-brain detection: if we receive a heartbeat from another leader while we're also leader
	if n.IsLeader() && senderType == protocol.NodeType_LEADER && senderID != n.id {
		fmt.Printf("[Leader %s] SPLIT-BRAIN DETECTED: Received heartbeat from another leader %s!\n",
			n.id[:8], senderID[:8])
		fmt.Printf("[Leader %s] Triggering election to resolve split-brain...\n", n.id[:8])
		// Trigger election to resolve split-brain
		go func() {
			time.Sleep(1 * time.Second) // Small delay to ensure network is ready
			if err := n.StartElection(); err != nil {
				log.Printf("[Leader %s] Failed to start election for split-brain resolution: %v\n", n.id[:8], err)
			}
		}()
		return // Don't process heartbeat from duplicate leader
	}

	if !n.IsLeader() {
		isLeaderHeartbeat := senderType == protocol.NodeType_LEADER
		currentLeaderID := n.failureDetector.GetLeaderID()
		
		// Safe string formatting for empty leader ID
		currentLeaderIDStr := "(empty)"
		if len(currentLeaderID) >= 8 {
			currentLeaderIDStr = currentLeaderID[:8]
		} else if currentLeaderID != "" {
			currentLeaderIDStr = currentLeaderID
		}

		fmt.Printf("[%s] Follower heartbeat check: senderType=%s, isLeaderHB=%v, currentLeaderID='%s'\n",
			n.id[:8], senderType, isLeaderHeartbeat, currentLeaderIDStr)

		if isLeaderHeartbeat {
			fmt.Printf("[%s] This is a LEADER heartbeat - updating failure detector\n", n.id[:8])
		} else {
			fmt.Printf("[%s] This is NOT a leader heartbeat (type=%s) - skipping failure detector\n",
				n.id[:8], senderType)
		}
	}

	// Update heartbeat timestamp in state.Registry
	n.clusterState.UpdateBrokerHeartbeat(senderID)

	// If this is a leader heartbeat and we're a follower, update failure detector
	if !n.IsLeader() && senderType == protocol.NodeType_LEADER {
		// If failure detector is not initialized, initialize it now from heartbeat
		currentLeaderID := n.failureDetector.GetLeaderID()
		if currentLeaderID == "" {
			fmt.Printf("[%s] Failure detector not initialized, initializing from leader heartbeat: %s\n",
				n.id[:8], senderID[:8])
			n.failureDetector.Reset(senderID)
			fmt.Printf("[%s] Failure detector initialized with leader from heartbeat\n", n.id[:8])
		}
		
		fmt.Printf("[%s] Updating failure detector with leader %s heartbeat\n",
			n.id[:8], senderID[:8])
		n.failureDetector.UpdateHeartbeat(senderID)

		updatedLeaderID := n.failureDetector.GetLeaderID()
		lastHB := n.failureDetector.GetLastHeartbeat()
		
		// Safe string formatting for empty leader ID
		updatedLeaderIDStr := "(empty)"
		if len(updatedLeaderID) >= 8 {
			updatedLeaderIDStr = updatedLeaderID[:8]
		} else if updatedLeaderID != "" {
			updatedLeaderIDStr = updatedLeaderID
		}
		
		fmt.Printf("[%s] Failure detector now tracking leader: '%s', last HB: %v ago\n",
			n.id[:8], updatedLeaderIDStr, time.Since(lastHB).Round(time.Second))
	}
}

func (n *Node) handleReplication(msg *protocol.ReplicateMsg, sender *net.UDPAddr) {
	// protocol.GetSequenceNum() provided by protocol package
	seqNum := protocol.GetSequenceNum(msg)
	senderID := protocol.GetSenderID(msg)
	senderAddr := "unknown"
	if sender != nil {
		senderAddr = sender.String()
	}

	transportType := "multicast"
	if sender == nil {
		transportType = "TCP"
	}
	fmt.Printf("\n[Node %s] [Replication-Handle] ========================================\n", n.id[:8])
	fmt.Printf("[Node %s] [Replication-Handle] Received REPLICATE message (via %s)\n", n.id[:8], transportType)
	fmt.Printf("[Node %s] [Replication-Handle] Sequence: %d\n", n.id[:8], seqNum)
	fmt.Printf("[Node %s] [Replication-Handle] Update type: %s\n", n.id[:8], msg.UpdateType)
	fmt.Printf("[Node %s] [Replication-Handle] Sender ID: %s\n", n.id[:8], senderID[:8])
	if sender != nil {
		fmt.Printf("[Node %s] [Replication-Handle] Sender address: %s\n", n.id[:8], senderAddr)
	} else {
		fmt.Printf("[Node %s] [Replication-Handle] Received via TCP direct connection\n", n.id[:8])
	}
	fmt.Printf("[Node %s] [Replication-Handle] State snapshot size: %d bytes\n", n.id[:8], len(msg.StateSnapshot))

	if !n.IsLeader() {
		// Check if sender is the leader in our registry
		brokers := n.clusterState.ListBrokers()
		isLeader := false
		for _, brokerID := range brokers {
			if brokerID == senderID {
				if broker, ok := n.clusterState.GetBroker(brokerID); ok && broker.IsLeader {
					isLeader = true
					break
				}
			}
		}
		fmt.Printf("[Node %s] [Replication-Handle] Sender is leader: %v\n", n.id[:8], isLeader)
		fmt.Printf("[Node %s] [Replication-Handle] Will initialize failure detector after apply\n", n.id[:8])
	}

	// Enqueue for FIFO delivery via state.HoldbackQueue
	fmt.Printf("[Node %s] [Replication-Handle] Enqueueing in holdback queue...\n", n.id[:8])
	holdbackMsg := &state.HoldbackMessage{
		SequenceNum:   seqNum,
		StateSnapshot: msg.StateSnapshot,
		UpdateType:    msg.UpdateType,
	}

	if err := n.holdbackQueue.Enqueue(holdbackMsg); err != nil {
		fmt.Printf("[Node %s] [Replication-Handle] ERROR: Holdback enqueue failed: %v\n", n.id[:8], err)
		log.Printf("[Node %s] Holdback error: %v\n", n.id[:8], err)
	} else {
		fmt.Printf("[Node %s] [Replication-Handle] Successfully enqueued REPLICATE (seq=%d)\n", n.id[:8], seqNum)
	}
	fmt.Printf("[Node %s] [Replication-Handle] ========================================\n\n", n.id[:8])
}

func (n *Node) handleElection(msg *protocol.ElectionMsg, sender *net.UDPAddr) {
	candidateID := msg.CandidateId
	electionID := msg.ElectionId
	phase := msg.Phase

	senderStr := "TCP"
	if sender != nil {
		senderStr = sender.String()
	}

	fmt.Printf("\n[Node %s] [Election-Handle] ========================================\n", n.id[:8])
	fmt.Printf("[Node %s] [Election-Handle] Received ELECTION message\n", n.id[:8])
	fmt.Printf("[Node %s] [Election-Handle] Phase: %v\n", n.id[:8], phase)
	fmt.Printf("[Node %s] [Election-Handle] Candidate: %s\n", n.id[:8], candidateID[:8])
	fmt.Printf("[Node %s] [Election-Handle] Election ID: %d\n", n.id[:8], electionID)
	fmt.Printf("[Node %s] [Election-Handle] Sender address: %s\n", n.id[:8], senderStr)
	fmt.Printf("[Node %s] [Election-Handle] My ID: %s\n", n.id[:8], n.id[:8])
	fmt.Printf("[Node %s] [Election-Handle] ========================================\n\n", n.id[:8])

	switch phase {
	case protocol.ElectionMessage_ANNOUNCE:
		fmt.Printf("[Node %s] [Election-Handle] Processing ANNOUNCE phase...\n", n.id[:8])
		n.handleElectionAnnounce(candidateID, electionID)

	case protocol.ElectionMessage_VICTORY:
		fmt.Printf("[Node %s] [Election-Handle] Processing VICTORY phase...\n", n.id[:8])
		n.handleElectionVictory(candidateID, electionID)
	}
}

func (n *Node) handleNack(msg *protocol.NackMsg, sender *net.UDPAddr) {
	fmt.Printf("[%s] <- NACK seq=%d-%d from %s\n",
		n.id[:8], msg.FromSeq, msg.ToSeq, protocol.GetSenderID(msg)[:8])

	// TODO: Resend missing messages
}

// Handle DATA from producer
func (n *Node) handleData(msg *protocol.DataMsg, sender *net.UDPAddr) {
	producerID := protocol.GetSenderID(msg)
	topic := msg.Topic

	fmt.Printf("[%s] <- DATA from %s (topic: %s, size: %d bytes)\n",
		n.id[:8], producerID[:8], topic, len(msg.Data))

	// Update producer heartbeat
	n.clusterState.UpdateProducerHeartbeat(producerID)

	// Get consumers subscribed to this topic
	subscribers := n.clusterState.GetConsumerSubscribers(topic)
	if len(subscribers) == 0 {
		fmt.Printf("[%s] No consumers subscribed to topic: %s\n", n.id[:8], topic)
		return
	}

	fmt.Printf("[%s] Forwarding to %d consumer(s) on topic: %s\n",
		n.id[:8], len(subscribers), topic)

	// Send RESULT message to each subscribed consumer
	for _, consumerID := range subscribers {
		// Get consumer info
		consumer, ok := n.clusterState.GetConsumer(consumerID)
		if !ok {
			continue
		}

		// Use protocol helper function instead of manual creation
		resultMsg := protocol.NewResultMsg(
			n.id,     // sender ID (this node)
			topic,    // topic
			msg.Data, // data payload
			0,        // offset (TODO: track per-topic offset)
			0,        // sequence (TODO: track per-topic sequence)
		)

		// Send via TCP in a goroutine to avoid blocking
		go func(addr string, result protocol.Message) {
			conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				log.Printf("[%s] Failed to connect to consumer %s: %v\n",
					n.id[:8], addr, err)
				return
			}
			defer conn.Close()

			if err := protocol.WriteTCPMessage(conn, result); err != nil {
				log.Printf("[%s] Failed to send RESULT to %s: %v\n",
					n.id[:8], addr, err)
				return
			}

			fmt.Printf("[%s] -> RESULT to %s (topic: %s, size: %d bytes)\n",
				n.id[:8], addr, topic, len(msg.Data))
		}(consumer.Address, resultMsg)
	}
}

func (n *Node) applyRegistryUpdate(msg *state.HoldbackMessage) error {
	fmt.Printf("\n[Node %s] ========================================\n", n.id[:8])
	fmt.Printf("[Node %s] Applying registry update seq=%d type=%s\n",
		n.id[:8], msg.SequenceNum, msg.UpdateType)

	currentLeaderID := n.failureDetector.GetLeaderID()
	fmt.Printf("[Node %s] Before update: IsLeader=%v, FailureDetectorLeader='%s'\n",
		n.id[:8], n.IsLeader(), currentLeaderID)

	// Deserialize cluster state
	if err := n.clusterState.Deserialize(msg.StateSnapshot); err != nil {
		return fmt.Errorf("failed to apply update: %w", err)
	}

	// Update last applied sequence number
	if msg.SequenceNum > n.lastAppliedSeqNum {
		n.lastAppliedSeqNum = msg.SequenceNum
		fmt.Printf("[Node %s] Updated lastAppliedSeqNum to %d\n", n.id[:8], n.lastAppliedSeqNum)
	}

	// Print cluster state status after applying update
	n.clusterState.PrintStatus()
	
	// Update stored leader address from registry if we're a follower
	if !n.IsLeader() {
		brokers := n.clusterState.ListBrokers()
		for _, brokerID := range brokers {
			if broker, ok := n.clusterState.GetBroker(brokerID); ok && broker.IsLeader {
				if n.leaderAddress != broker.Address {
					fmt.Printf("[Node %s] Updated stored leader address: %s -> %s\n", 
						n.id[:8], n.leaderAddress, broker.Address)
					n.leaderAddress = broker.Address
				}
				break
			}
		}
	}

	// Initialize failure detector for followers if not already done
	if !n.IsLeader() {
		currentLeaderID := n.failureDetector.GetLeaderID()
		fmt.Printf("[Node %s] Post-update check: FailureDetectorLeader='%s'\n",
			n.id[:8], currentLeaderID)

		if currentLeaderID == "" {
			fmt.Printf("[Node %s] Failure detector NOT initialized - initializing NOW...\n", n.id[:8])
			n.initializeFailureDetector()

			newLeaderID := n.failureDetector.GetLeaderID()
			if newLeaderID != "" {
				lastHB := n.failureDetector.GetLastHeartbeat()
				// Safe string formatting
				leaderIDStr := newLeaderID
				if len(newLeaderID) >= 8 {
					leaderIDStr = newLeaderID[:8]
				}
				fmt.Printf("[Node %s] SUCCESS: Failure detector initialized with leader='%s', lastHB=%v ago\n",
					n.id[:8], leaderIDStr, time.Since(lastHB).Round(time.Second))
			} else {
				fmt.Printf("[Node %s] FAILED: Failure detector still has no leader after initialization!\n",
					n.id[:8])
				fmt.Printf("[Node %s] Registry brokers: %v\n", n.id[:8], n.clusterState.ListBrokers())
			}
		} else {
			// Safe string formatting (should not be empty here, but be safe)
			leaderIDStr := currentLeaderID
			if len(currentLeaderID) >= 8 {
				leaderIDStr = currentLeaderID[:8]
			}
			fmt.Printf("[Node %s] Failure detector already initialized with leader='%s'\n",
				n.id[:8], leaderIDStr)
		}
	} else {
		fmt.Printf("[Node %s] I'm the leader - skipping failure detector initialization\n", n.id[:8])
	}

	fmt.Printf("[Node %s] ========================================\n\n", n.id[:8])

	return nil
}


// computeRing determines this node's position in the logical ring and identifies the next node
func (n *Node) computeRing() {
	brokers := n.clusterState.ListBrokers()

	if len(brokers) == 0 {
		fmt.Printf("[Ring] No brokers in registry\n")
		return
	}

	// Sort broker IDs to form consistent ring
	sort.Strings(brokers)

	// Find our position
	myPos := -1
	for i, id := range brokers {
		if id == n.id {
			myPos = i
			break
		}
	}

	if myPos == -1 {
		fmt.Printf("[Ring] ERROR: Node %s not in registry!\n", n.id[:8])
		return
	}

	n.ringPosition = myPos

	// Next node is (myPos + 1) % len(brokers) - wrap around
	nextPos := (myPos + 1) % len(brokers)
	nextID := brokers[nextPos]

	// Get next node's address
	broker, ok := n.clusterState.GetBroker(nextID)
	if !ok {
		fmt.Printf("[Ring] ERROR: Next node %s not found in registry!\n", nextID[:8])
		return
	}

	n.nextNode = broker.Address

	fmt.Printf("[Ring] Position %d/%d, Next node: %s (addr: %s)\n",
		myPos+1, len(brokers), nextID[:8], n.nextNode)
}

// tryNextAvailableNode attempts to find and connect to the next available node in the FROZEN ring topology
// During election, the ring topology is frozen (no joins/leaves), but nodes may be unreachable
// This function tries each node in the frozen ring in order, skipping unreachable ones
// Returns true if successful, false if no other nodes in the frozen ring are available
func (n *Node) tryNextAvailableNode(electionID int64) bool {
	fmt.Printf("[Node %s] [Election] Attempting to find next available node in FROZEN ring topology...\n", n.id[:8])
	fmt.Printf("[Node %s] [Election] Note: Ring topology is frozen during election - using registry at election start\n", n.id[:8])
	
	brokers := n.clusterState.ListBrokers()
	if len(brokers) < 2 {
		fmt.Printf("[Node %s] [Election] Only %d broker(s) in frozen registry - cannot find next node\n", n.id[:8], len(brokers))
		return false
	}
	
	// Sort broker IDs to form consistent ring (frozen topology)
	sort.Strings(brokers)
	
	// Find our position in the frozen ring
	myPos := -1
	for i, id := range brokers {
		if id == n.id {
			myPos = i
			break
		}
	}
	
	if myPos == -1 {
		fmt.Printf("[Node %s] [Election] ERROR: Node not in frozen registry!\n", n.id[:8])
		return false
	}
	
	fmt.Printf("[Node %s] [Election] Frozen ring has %d nodes, my position: %d/%d\n", n.id[:8], len(brokers), myPos+1, len(brokers))
	
	// Try each node in the frozen ring starting from the immediate next one
	// This respects the ring topology - we're not skipping around arbitrarily
	for offset := 1; offset < len(brokers); offset++ {
		nextPos := (myPos + offset) % len(brokers)
		nextID := brokers[nextPos]
		
		// Get next node's address from frozen registry
		broker, ok := n.clusterState.GetBroker(nextID)
		if !ok {
			fmt.Printf("[Node %s] [Election] Node %s not found in frozen registry, skipping...\n", n.id[:8], nextID[:8])
			continue
		}
		
		// Try to connect to this node (it may be unreachable even though it's in the frozen ring)
		fmt.Printf("[Node %s] [Election] Trying to connect to next node in ring: %s at %s (position %d/%d)...\n", 
			n.id[:8], nextID[:8], broker.Address, nextPos+1, len(brokers))
		conn, err := net.DialTimeout("tcp", broker.Address, 2*time.Second)
		if err != nil {
			fmt.Printf("[Node %s] [Election] Node %s at %s is unreachable: %v, trying next in ring...\n", 
				n.id[:8], nextID[:8], broker.Address, err)
			continue
		}
		conn.Close()
		
		// Found an available node in the frozen ring - update nextNode and retry forwarding
		fmt.Printf("[Node %s] [Election] Found available node %s at %s in frozen ring\n", n.id[:8], nextID[:8], broker.Address)
		n.nextNode = broker.Address
		
		// Retry sending the election message to this available node
		// Use the current candidate ID (which may have been updated)
		candidateID := n.election.GetCandidate()
		if candidateID == "" {
			candidateID = n.id // Fallback to our own ID
		}
		if err := n.sendElectionMessage(candidateID, electionID, protocol.ElectionMessage_ANNOUNCE); err != nil {
			fmt.Printf("[Node %s] [Election] Still failed to send to %s: %v, trying next in ring...\n", n.id[:8], broker.Address, err)
			continue
		}
		
		fmt.Printf("[Node %s] [Election] Successfully forwarded election message to available node %s\n", n.id[:8], nextID[:8])
		return true
	}
	
	// Tried all nodes in the frozen ring - none are reachable
	fmt.Printf("[Node %s] [Election] No available nodes found in frozen ring (tried all %d nodes)\n", n.id[:8], len(brokers))
	return false
}

// StartElection initiates a new leader election using LCR algorithm
func (n *Node) StartElection() error {
	fmt.Printf("\n[Node %s] ======================================\n", n.id[:8])
	fmt.Printf("[Node %s] STARTING LEADER ELECTION (LCR)\n", n.id[:8])
	fmt.Printf("[Node %s] ======================================\n\n", n.id[:8])

	// Check if we have a synchronized registry
	brokerCount := n.clusterState.GetBrokerCount()
	fmt.Printf("[Node %s] Registry has %d brokers\n", n.id[:8], brokerCount)

	if brokerCount < 2 {
		// Registry not synchronized yet - can't run election
		fmt.Printf("[Node %s] Cannot start election - registry not synchronized (brokers: %d, need at least 2)\n", 
			n.id[:8], brokerCount)
		fmt.Printf("[Node %s] Resetting any existing election state\n", n.id[:8])
		n.election.Reset()
		return fmt.Errorf("registry not synchronized: only %d broker(s), need at least 2", brokerCount)
	}

	// Step 1: Compute ring topology from FROZEN registry
	// During election, the ring topology is frozen - no nodes join/leave
	// This ensures consistent election behavior per LCR algorithm
	fmt.Printf("[Node %s] Computing ring topology from FROZEN registry (election freeze in effect)...\n", n.id[:8])
	n.computeRing()

	// Verify ring computation succeeded
	if n.nextNode == "" {
		fmt.Printf("[Node %s] Cannot start election - ring computation failed (nextNode is empty)\n", n.id[:8])
		n.election.Reset()
		return fmt.Errorf("ring computation failed: nextNode is empty")
	}
	
	fmt.Printf("[Node %s] Frozen ring topology computed - next node: %s\n", n.id[:8], n.nextNode)

	// Step 2: Start election with our ID as candidate
	electionID := time.Now().UnixNano()
	n.election.StartElection(n.id, electionID)

	// Step 3: Send ANNOUNCE message to next node in ring
	fmt.Printf("[Node %s] Sending ELECTION ANNOUNCE to next node: %s\n", n.id[:8], n.nextNode)
	return n.sendElectionMessage(n.id, electionID, protocol.ElectionMessage_ANNOUNCE)
}

// sendElectionMessage sends an election message to the next node in the ring
func (n *Node) sendElectionMessage(candidateID string, electionID int64, phase protocol.ElectionMessage_Phase) error {
	phaseStr := "ANNOUNCE"
	if phase == protocol.ElectionMessage_VICTORY {
		phaseStr = "VICTORY"
	}

	fmt.Printf("[Node %s] [Election-Send] ========================================\n", n.id[:8])
	fmt.Printf("[Node %s] [Election-Send] Preparing to send ELECTION %s\n", n.id[:8], phaseStr)
	fmt.Printf("[Node %s] [Election-Send] Candidate: %s\n", n.id[:8], candidateID[:8])
	fmt.Printf("[Node %s] [Election-Send] Election ID: %d\n", n.id[:8], electionID)
	fmt.Printf("[Node %s] [Election-Send] Target: %s\n", n.id[:8], n.nextNode)
	
	if n.nextNode == "" {
		fmt.Printf("[Node %s] [Election-Send] ERROR: next node not set\n", n.id[:8])
		return fmt.Errorf("next node not set - call computeRing() first")
	}

	// Create election message using protocol helper
	fmt.Printf("[Node %s] [Election-Send] Creating election message...\n", n.id[:8])
	msg := protocol.NewElectionMsg(n.id, candidateID, electionID, phase)
	fmt.Printf("[Node %s] [Election-Send] Message created successfully\n", n.id[:8])

	// Connect to next node via TCP
	fmt.Printf("[Node %s] [Election-Send] Connecting to %s via TCP (timeout: 5s)...\n", n.id[:8], n.nextNode)
	conn, err := net.DialTimeout("tcp", n.nextNode, 5*time.Second)
	if err != nil {
		fmt.Printf("[Node %s] [Election-Send] ERROR: Failed to connect: %v\n", n.id[:8], err)
		return fmt.Errorf("failed to connect to next node %s: %w", n.nextNode, err)
	}
	fmt.Printf("[Node %s] [Election-Send] TCP connection established to %s\n", n.id[:8], n.nextNode)
	defer func() {
		conn.Close()
		fmt.Printf("[Node %s] [Election-Send] TCP connection to %s closed\n", n.id[:8], n.nextNode)
	}()

	// Send message
	fmt.Printf("[Node %s] [Election-Send] Sending ELECTION %s message...\n", n.id[:8], phaseStr)
	if err := protocol.WriteTCPMessage(conn, msg); err != nil {
		fmt.Printf("[Node %s] [Election-Send] ERROR: Failed to send message: %v\n", n.id[:8], err)
		return fmt.Errorf("failed to send ELECTION message: %w", err)
	}

	fmt.Printf("[Node %s] [Election-Send] Successfully sent ELECTION %s (candidate: %s) to %s\n",
		n.id[:8], phaseStr, candidateID[:8], n.nextNode)
	fmt.Printf("[Node %s] [Election-Send] ========================================\n", n.id[:8])

	return nil
}

// handleElectionAnnounce processes ANNOUNCE phase messages
func (n *Node) handleElectionAnnounce(candidateID string, electionID int64) {
	fmt.Printf("[Node %s] [Election-Announce] ========================================\n", n.id[:8])
	fmt.Printf("[Node %s] [Election-Announce] Processing ANNOUNCE message\n", n.id[:8])
	fmt.Printf("[Node %s] [Election-Announce] Candidate: %s\n", n.id[:8], candidateID[:8])
	fmt.Printf("[Node %s] [Election-Announce] My ID: %s\n", n.id[:8], n.id[:8])
	fmt.Printf("[Node %s] [Election-Announce] Election ID: %d\n", n.id[:8], electionID)
	
	// Check if we have a synchronized registry
	brokerCount := n.clusterState.GetBrokerCount()
	fmt.Printf("[Node %s] [Election-Announce] Registry broker count: %d\n", n.id[:8], brokerCount)

	if brokerCount < 2 {
		// Registry not synchronized yet (only have myself or nobody)
		fmt.Printf("[Node %s] [Election-Announce] WARNING: Registry not synchronized (brokers: %d)\n", n.id[:8], brokerCount)
		fmt.Printf("[Node %s] [Election-Announce] Ignoring election - waiting for registry sync\n", n.id[:8])
		return // Don't participate
	}

	// Compute ring if needed
	if n.nextNode == "" {
		fmt.Printf("[Node %s] [Election-Announce] Computing ring topology...\n", n.id[:8])
		n.computeRing()

		if n.nextNode == "" {
			// This shouldn't happen if brokerCount >= 2, but safety check
			fmt.Printf("[Node %s] [Election-Announce] ERROR: Cannot compute ring despite having %d brokers\n", n.id[:8], brokerCount)
			log.Printf("[Election] ERROR: Cannot compute ring despite having %d brokers\n", brokerCount)
			return
		}
		fmt.Printf("[Node %s] [Election-Announce] Ring computed, next node: %s\n", n.id[:8], n.nextNode)
	}

	// LCR Algorithm Logic:
	// - If candidate ID > my ID: Forward the message with the higher candidate
	// - If candidate ID < my ID: Replace candidate with my ID and forward
	// - If candidate ID == my ID: I win, declare victory
	fmt.Printf("[Node %s] [Election-Announce] Comparing IDs: candidate=%s, mine=%s\n", n.id[:8], candidateID[:8], n.id[:8])
	if candidateID > n.id {
		fmt.Printf("[Node %s] [Election-Announce] Candidate %s is HIGHER than %s - FORWARDING\n",
			n.id[:8], candidateID[:8], n.id[:8])
		n.election.UpdateCandidate(candidateID)

		if err := n.sendElectionMessage(candidateID, electionID, protocol.ElectionMessage_ANNOUNCE); err != nil {
			fmt.Printf("[Node %s] [Election-Announce] ERROR: Failed to forward: %v\n", n.id[:8], err)
			log.Printf("[Election] Failed to forward: %v\n", err)
			
			// Try to find next available node in ring
			if n.tryNextAvailableNode(electionID) {
				fmt.Printf("[Node %s] [Election-Announce] Successfully forwarded to next available node\n", n.id[:8])
			} else {
				// No other nodes in the ring are reachable
				// Since we're forwarding a higher candidate ID (not ours), we can't declare victory
				// But we should still try to propagate the election if possible
				// For now, log the situation - the election may complete when nodes come back
				fmt.Printf("[Node %s] [Election-Announce] WARNING: No other nodes reachable, but candidate %s is not me\n", n.id[:8], candidateID[:8])
				fmt.Printf("[Node %s] [Election-Announce] Election cannot complete - waiting for nodes to become available\n", n.id[:8])
			}
		} else {
			fmt.Printf("[Node %s] [Election-Announce] Successfully forwarded candidate\n", n.id[:8])
		}

	} else if candidateID < n.id {
		// LCR: Replace candidate with my ID and forward (don't drop!)
		fmt.Printf("[Node %s] [Election-Announce] Candidate %s is LOWER than %s - REPLACING with my ID and FORWARDING\n",
			n.id[:8], candidateID[:8], n.id[:8])
		
		// Update election state with my ID as the new candidate
		n.election.UpdateCandidate(n.id)
		
		// Forward the election message with my ID as the candidate
		// If next node is unreachable, try to find next available node
		if err := n.sendElectionMessage(n.id, electionID, protocol.ElectionMessage_ANNOUNCE); err != nil {
			fmt.Printf("[Node %s] [Election-Announce] ERROR: Failed to forward with my ID: %v\n", n.id[:8], err)
			log.Printf("[Election] Failed to forward with my ID: %v\n", err)
			
			// Try to find next available node in ring
			if n.tryNextAvailableNode(electionID) {
				fmt.Printf("[Node %s] [Election-Announce] Successfully forwarded to next available node\n", n.id[:8])
			} else {
				// No other nodes in the FROZEN ring are reachable - we are the only available node
				// According to LCR: During election, ring topology is frozen (no joins/leaves)
				// We've tried all nodes in the frozen ring and none are reachable
				// Since we're forwarding our own ID and no one else is available, we win!
				fmt.Printf("[Node %s] [Election-Announce] ======================================\n", n.id[:8])
				fmt.Printf("[Node %s] [Election-Announce] No other nodes in FROZEN ring are reachable - I WIN!\n", n.id[:8])
				fmt.Printf("[Node %s] [Election-Announce] Tried all nodes in frozen ring topology - all unreachable\n", n.id[:8])
				fmt.Printf("[Node %s] [Election-Announce] Declaring victory as the only available node in frozen ring\n", n.id[:8])
				fmt.Printf("[Node %s] [Election-Announce] ======================================\n\n", n.id[:8])
				
				n.election.DeclareVictory(n.id)
				fmt.Printf("[Node %s] [Election-Announce] Becoming leader...\n", n.id[:8])
				n.becomeLeader()
			}
		} else {
			fmt.Printf("[Node %s] [Election-Announce] Successfully forwarded with my ID %s as candidate\n", n.id[:8], n.id[:8])
		}

	} else {
		fmt.Printf("\n[Node %s] [Election-Announce] ======================================\n", n.id[:8])
		fmt.Printf("[Node %s] [Election-Announce] Message returned to me - I WIN!\n", n.id[:8])
		fmt.Printf("[Node %s] [Election-Announce] ======================================\n\n", n.id[:8])

		n.election.DeclareVictory(n.id)

		fmt.Printf("[Node %s] [Election-Announce] Sending VICTORY message...\n", n.id[:8])
		if err := n.sendElectionMessage(n.id, electionID, protocol.ElectionMessage_VICTORY); err != nil {
			fmt.Printf("[Node %s] [Election-Announce] ERROR: Failed to send VICTORY: %v\n", n.id[:8], err)
			log.Printf("[Election] Failed to send VICTORY: %v\n", err)
			// If we can't send VICTORY, try next available node
			// But if we're the only node, we can still become leader
			brokerCount := n.clusterState.GetBrokerCount()
			if brokerCount > 1 {
				fmt.Printf("[Node %s] [Election-Announce] Trying to send VICTORY to next available node...\n", n.id[:8])
				// Update nextNode and retry
				n.computeRing()
				if n.nextNode != "" {
					n.sendElectionMessage(n.id, electionID, protocol.ElectionMessage_VICTORY)
				}
			}
		} else {
			fmt.Printf("[Node %s] [Election-Announce] VICTORY message sent successfully\n", n.id[:8])
		}

		fmt.Printf("[Node %s] [Election-Announce] Becoming leader...\n", n.id[:8])
		n.becomeLeader()
	}
	fmt.Printf("[Node %s] [Election-Announce] ========================================\n", n.id[:8])
}

// handleElectionVictory processes VICTORY phase messages
func (n *Node) handleElectionVictory(leaderID string, electionID int64) {
	fmt.Printf("\n[Node %s] [Election-Victory] ========================================\n", n.id[:8])
	fmt.Printf("[Node %s] [Election-Victory] Processing VICTORY message\n", n.id[:8])
	fmt.Printf("[Node %s] [Election-Victory] New leader: %s\n", n.id[:8], leaderID[:8])
	fmt.Printf("[Node %s] [Election-Victory] Election ID: %d\n", n.id[:8], electionID)
	fmt.Printf("[Node %s] [Election-Victory] My ID: %s\n", n.id[:8], n.id[:8])
	
	// Same check for synchronized registry
	brokerCount := n.clusterState.GetBrokerCount()
	fmt.Printf("[Node %s] [Election-Victory] Registry broker count: %d\n", n.id[:8], brokerCount)

	if brokerCount < 2 {
		fmt.Printf("[Node %s] [Election-Victory] WARNING: Registry not synchronized (brokers: %d)\n", n.id[:8], brokerCount)
		fmt.Printf("[Node %s] [Election-Victory] Ignoring VICTORY message\n", n.id[:8])
		return
	}

	if n.nextNode == "" {
		fmt.Printf("[Node %s] [Election-Victory] Computing ring topology...\n", n.id[:8])
		n.computeRing()

		if n.nextNode == "" {
			fmt.Printf("[Node %s] [Election-Victory] ERROR: Cannot compute ring despite having %d brokers\n", n.id[:8], brokerCount)
			log.Printf("[Election] ERROR: Cannot compute ring despite having %d brokers\n", brokerCount)
			return
		}
		fmt.Printf("[Node %s] [Election-Victory] Ring computed, next node: %s\n", n.id[:8], n.nextNode)
	}

	if leaderID == n.id {
		fmt.Printf("[Election] VICTORY message completed circuit\n")
		return
	}

	fmt.Printf("[Election] Accepting %s as new leader\n", leaderID[:8])
	n.election.AcceptLeader(leaderID)

	// Reset failure detector for new leader
	n.failureDetector.Reset(leaderID)
	
	// Update stored leader address from registry
	if broker, ok := n.clusterState.GetBroker(leaderID); ok {
		n.leaderAddress = broker.Address
		fmt.Printf("[Node %s] Updated stored leader address to: %s\n", n.id[:8], n.leaderAddress)
	}

	if err := n.sendElectionMessage(leaderID, electionID, protocol.ElectionMessage_VICTORY); err != nil {
		log.Printf("[Election] Failed to forward VICTORY: %v\n", err)
		// Try next available node
		n.computeRing()
		if n.nextNode != "" {
			n.sendElectionMessage(leaderID, electionID, protocol.ElectionMessage_VICTORY)
		}
	}

	n.becomeFollower()
}


func (n *Node) runLeaderDuties() {
	fmt.Printf("[Leader %s] [Leader-Duties] ========================================\n", n.id[:8])
	fmt.Printf("[Leader %s] [Leader-Duties] Starting leader duties\n", n.id[:8])
	fmt.Printf("[Leader %s] [Leader-Duties] Node ID: %s\n", n.id[:8], n.id[:8])
	fmt.Printf("[Leader %s] [Leader-Duties] Address: %s\n", n.id[:8], n.address)
	fmt.Printf("[Leader %s] [Leader-Duties] ========================================\n", n.id[:8])

	heartbeatTicker := time.NewTicker(5 * time.Second)
	clientHeartbeatTicker := time.NewTicker(30 * time.Second) // Heartbeats to clients
	timeoutTicker := time.NewTicker(10 * time.Second)
	defer heartbeatTicker.Stop()
	defer clientHeartbeatTicker.Stop()
	defer timeoutTicker.Stop()

	for {
		select {
		case <-heartbeatTicker.C:
			// Send periodic heartbeat to brokers (UDP multicast)
			if err := n.sendHeartbeat(); err != nil {
				log.Printf("[Leader %s] Failed to send heartbeat: %v\n", n.id[:8], err)
			}

			// Note: Replication is now IMMEDIATE on state changes, not periodic

		case <-clientHeartbeatTicker.C:
			// Send heartbeats to producers and consumers (TCP unicast)
			// Proposal: "the leader node sends heartbeat messages to the consumers/producers"
			n.sendHeartbeatsToProducers()
			n.sendHeartbeatsToConsumers()

		case <-timeoutTicker.C:
			// Check for dead brokers
			removedBrokers := n.clusterState.CheckBrokerTimeouts(30 * time.Second)
			if len(removedBrokers) > 0 {
				fmt.Printf("[Leader %s] Removed %d dead brokers\n", n.id[:8], len(removedBrokers))
				// Replicate immediately to propagate removal
				n.replicateAllState()
			}

			// Check for dead producers
			deadProducers := n.clusterState.CheckProducerTimeouts(60 * time.Second)
			if len(deadProducers) > 0 {
				fmt.Printf("[Leader %s] Removed %d dead producers\n", n.id[:8], len(deadProducers))
				// Replicate immediately to propagate removal
				n.replicateAllState()
			}

			// Check for dead consumers
			deadConsumers := n.clusterState.CheckConsumerTimeouts(60 * time.Second)
			if len(deadConsumers) > 0 {
				fmt.Printf("[Leader %s] Removed %d dead consumers\n", n.id[:8], len(deadConsumers))
				// Replicate immediately to propagate removal
				n.replicateAllState()
			}

		case <-n.stopLeaderDuties:
			fmt.Printf("[Leader %s] Stopping leader duties\n", n.id[:8])
			return
		}
	}
}

func (n *Node) sendHeartbeat() error {
	if !n.IsLeader() {
		return fmt.Errorf("only leader can send heartbeat")
	}

	// protocol.SendHeartbeatMulticast() provided by protocol package
	err := protocol.SendHeartbeatMulticast(
		n.multicastSender,
		n.id,
		protocol.NodeType_LEADER,
		n.config.MulticastGroup,
	)

	if err == nil {
		fmt.Printf("[Leader %s] -> HEARTBEAT\n", n.id[:8])
	}

	return err
}

// replicateAllState performs IMMEDIATE ACTIVE replication of cluster state to all followers
// This is ACTIVE replication because:
//   1. Leader proactively sends updates IMMEDIATELY when state changes
//   2. Followers passively receive and apply updates via holdback queue
//   3. Leader doesn't wait for follower requests - it pushes updates
// Replicates immediately on every state change (brokers, producers, consumers)
func (n *Node) replicateAllState() error {
	if !n.IsLeader() {
		return fmt.Errorf("only leader can replicate registry")
	}

	// Check if state has changed since last replication
	currentSeq := n.clusterState.GetSequenceNum()
	if currentSeq == n.lastReplicatedSeq {
		// No state change, skip replication
		return nil
	}

	fmt.Printf("[Leader %s] State changed (seq: %d -> %d), replicating registry...\n",
		n.id[:8], n.lastReplicatedSeq, currentSeq)

	// Serialize cluster state
	snapshot, err := n.clusterState.Serialize()
	if err != nil {
		return err
	}

	fmt.Printf("[Leader %s] Serialized cluster state: %d bytes, %d brokers, %d producers, %d consumers\n",
		n.id[:8], len(snapshot), n.clusterState.GetBrokerCount(), n.clusterState.CountProducers(), n.clusterState.CountConsumers())

	// protocol.SendReplicationMulticast() provided by protocol package
	err = protocol.SendReplicationMulticast(
		n.multicastSender,
		n.id,
		snapshot,
		"REGISTRY",
		currentSeq,
		n.config.MulticastGroup,
	)

	if err == nil {
		// Update last replicated sequence
		n.lastReplicatedSeq = currentSeq
		fmt.Printf("[Leader %s] -> REPLICATE seq=%d type=CLUSTER_STATE (IMMEDIATE ACTIVE replication)\n", n.id[:8], currentSeq)

		// Persist replicated data to local storage log
		if n.storageLog != nil {
			if offset, logErr := n.storageLog.Append(snapshot); logErr == nil {
				fmt.Printf("[Leader %s] Persisted replicate to storage log at offset=%d\n", n.id[:8], offset)
			} else {
				fmt.Printf("[Leader %s] WARNING: Failed to persist replicate to storage: %v\n", n.id[:8], logErr)
			}
		}
	} else {
		fmt.Printf("[Leader %s] Failed to send REPLICATE: %v\n", n.id[:8], err)
	}

	return err
}

// syncFollowerWithCircuitBreaker implements circuit breaker pattern for initial multicast sync
// Uses circuit breaker states to manage retry attempts intelligently
func (n *Node) syncFollowerWithCircuitBreaker(followerID string) {
	if !n.IsLeader() {
		return
	}

	fmt.Printf("[Leader %s] [CircuitBreaker] Starting initial multicast sync for follower %s\n",
		n.id[:8], followerID[:8])

	// Serialize cluster state once
	snapshot, err := n.clusterState.Serialize()
	if err != nil {
		log.Printf("[Leader %s] [CircuitBreaker] Failed to serialize state: %v\n", n.id[:8], err)
		return
	}
	currentSeq := n.clusterState.GetSequenceNum()

	// Circuit Breaker Configuration
	const (
		maxAttempts     = 5           // Maximum multicast attempts
		initialDelay    = 1 * time.Second // Wait for follower's multicast listener to start
		retryDelay      = 500 * time.Millisecond // Delay between retries
		halfOpenDelay   = 2 * time.Second // Delay before trying half-open state
	)

	// Circuit Breaker State: CLOSED - Normal operation, try multicast
	fmt.Printf("[Leader %s] [CircuitBreaker] State: CLOSED - Attempting multicast sync...\n", n.id[:8])
	
	// Wait for follower's multicast listener to start
	time.Sleep(initialDelay)
	
	failureCount := 0
	successCount := 0
	
	// Try multicast with circuit breaker logic
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := protocol.SendReplicationMulticast(
			n.multicastSender,
			n.id,
			snapshot,
			"REGISTRY",
			currentSeq,
			n.config.MulticastGroup,
		)
		
		if err != nil {
			failureCount++
			fmt.Printf("[Leader %s] [CircuitBreaker] Attempt %d/%d failed: %v (failures: %d)\n",
				n.id[:8], attempt, maxAttempts, err, failureCount)
			
			// Circuit Breaker State: OPEN - Too many failures
			if failureCount >= 3 {
				fmt.Printf("[Leader %s] [CircuitBreaker] State: OPEN - Too many failures (%d), backing off...\n",
					n.id[:8], failureCount)
				time.Sleep(halfOpenDelay)
				
				// Circuit Breaker State: HALF-OPEN - Try one more time
				fmt.Printf("[Leader %s] [CircuitBreaker] State: HALF-OPEN - Attempting recovery...\n", n.id[:8])
				err = protocol.SendReplicationMulticast(
					n.multicastSender,
					n.id,
					snapshot,
					"REGISTRY",
					currentSeq,
					n.config.MulticastGroup,
				)
				if err != nil {
					fmt.Printf("[Leader %s] [CircuitBreaker] HALF-OPEN attempt failed: %v\n", n.id[:8], err)
					failureCount++
				} else {
					successCount++
					fmt.Printf("[Leader %s] [CircuitBreaker] HALF-OPEN attempt succeeded, closing circuit\n", n.id[:8])
					break
				}
			}
		} else {
			successCount++
			fmt.Printf("[Leader %s] [CircuitBreaker] Attempt %d/%d succeeded (successes: %d)\n",
				n.id[:8], attempt, maxAttempts, successCount)
			
			// If we have at least one success, consider it good enough
			// (multicast is best-effort, multiple sends increase delivery probability)
			if successCount >= 2 {
				fmt.Printf("[Leader %s] [CircuitBreaker] State: CLOSED - Sufficient successes (%d), sync complete\n",
					n.id[:8], successCount)
				break
			}
		}
		
		// Delay before next attempt (except on last attempt)
		if attempt < maxAttempts {
			time.Sleep(retryDelay)
		}
	}

	// Final status
	if successCount > 0 {
		fmt.Printf("[Leader %s] [CircuitBreaker] Initial sync completed: %d successful multicast sends\n",
			n.id[:8], successCount)
	} else {
		log.Printf("[Leader %s] [CircuitBreaker] WARNING: All multicast attempts failed for follower %s\n",
			n.id[:8], followerID[:8])
		fmt.Printf("[Leader %s] [CircuitBreaker] WARNING: Initial sync may have failed (all %d attempts failed)\n",
			n.id[:8], maxAttempts)
	}
}

// sendHeartbeatsToProducers sends TCP unicast heartbeats to all registered producers
// Proposal requirement: "the leader node sends heartbeat messages to the consumers/producers"
func (n *Node) sendHeartbeatsToProducers() {
	if !n.IsLeader() {
		return
	}

	producers := n.clusterState.ListProducers()
	if len(producers) == 0 {
		return
	}

	fmt.Printf("[Leader %s] Sending heartbeats to %d producer(s)...\n", n.id[:8], len(producers))

	for _, producerID := range producers {
		producer, ok := n.clusterState.GetProducer(producerID)
		if !ok {
			continue
		}

		// Send heartbeat via TCP in goroutine to avoid blocking
		go func(pid string, addr string) {
			conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
			if err != nil {
				// Producer might be down - will be detected by timeout check
				return
			}
			defer conn.Close()

			// Create heartbeat message
			heartbeat := protocol.NewHeartbeatMsg(n.id, protocol.NodeType_LEADER, 0)

			// Send heartbeat
			if err := protocol.WriteTCPMessage(conn, heartbeat); err != nil {
				return // Producer might be down
			}

			// Update producer heartbeat timestamp on success
			n.clusterState.UpdateProducerHeartbeat(pid)
			fmt.Printf("[Leader %s] -> HEARTBEAT (TCP) to producer %s\n", n.id[:8], pid[:8])
		}(producerID, producer.Address)
	}
}

// sendHeartbeatsToConsumers sends TCP unicast heartbeats to all registered consumers
// Proposal requirement: "the leader node sends heartbeat messages to the consumers/producers"
func (n *Node) sendHeartbeatsToConsumers() {
	if !n.IsLeader() {
		return
	}

	consumers := n.clusterState.ListConsumers()
	if len(consumers) == 0 {
		return
	}

	fmt.Printf("[Leader %s] Sending heartbeats to %d consumer(s)...\n", n.id[:8], len(consumers))

	for _, consumerID := range consumers {
		consumer, ok := n.clusterState.GetConsumer(consumerID)
		if !ok {
			continue
		}

		// Send heartbeat via TCP in goroutine to avoid blocking
		go func(cid string, addr string) {
			conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
			if err != nil {
				// Consumer might be down - will be detected by timeout check
				return
			}
			defer conn.Close()

			// Create heartbeat message
			heartbeat := protocol.NewHeartbeatMsg(n.id, protocol.NodeType_LEADER, 0)

			// Send heartbeat
			if err := protocol.WriteTCPMessage(conn, heartbeat); err != nil {
				return // Consumer might be down
			}

			// Update consumer heartbeat timestamp on success
			n.clusterState.UpdateConsumerHeartbeat(cid)
			fmt.Printf("[Leader %s] -> HEARTBEAT (TCP) to consumer %s\n", n.id[:8], cid[:8])
		}(consumerID, consumer.Address)
	}
}

// handleTCPHeartbeat handles TCP heartbeats from producers, consumers, or brokers
func (n *Node) handleTCPHeartbeat(msg *protocol.HeartbeatMsg, conn net.Conn) {
	senderID := protocol.GetSenderID(msg)
	senderType := protocol.GetSenderType(msg)

	fmt.Printf("[%s] <- HEARTBEAT (TCP) from %s (type: %s)\n",
		n.id[:8], senderID[:8], senderType)

	switch senderType {
	case protocol.NodeType_PRODUCER:
		// Producer heartbeat - update timestamp
		if n.IsLeader() {
			n.clusterState.UpdateProducerHeartbeat(senderID)
			fmt.Printf("[Leader %s] Updated producer %s heartbeat\n", n.id[:8], senderID[:8])
		}

	case protocol.NodeType_CONSUMER:
		// Consumer heartbeat - update timestamp
		if n.IsLeader() {
			n.clusterState.UpdateConsumerHeartbeat(senderID)
			fmt.Printf("[Leader %s] Updated consumer %s heartbeat\n", n.id[:8], senderID[:8])
		}

	case protocol.NodeType_BROKER:
		// Broker heartbeat to leader - update timestamp
		if n.IsLeader() {
			n.clusterState.UpdateBrokerHeartbeat(senderID)
			fmt.Printf("[Leader %s] Updated broker %s heartbeat (TCP)\n", n.id[:8], senderID[:8])
		}

	case protocol.NodeType_LEADER:
		// Leader heartbeat to client - just acknowledge (clients track this)
		fmt.Printf("[%s] Received leader heartbeat (TCP)\n", n.id[:8])
	}
}

func (n *Node) listenForBroadcastJoins() {
	for {
		// protocol.BroadcastConnection.ReceiveMessage() provided by protocol package
		msg, sender, err := n.broadcastListener.ReceiveMessage()
		if err != nil {
			return // Listener closed
		}

		if joinMsg, ok := msg.(*protocol.JoinMsg); ok {
			senderID := protocol.GetSenderID(msg)
			fmt.Printf("[Leader %s] <- JOIN from %s (addr=%s)\n",
				n.id[:8], senderID[:8], joinMsg.Address)

			// Filter out JOIN messages from wrong network interfaces (e.g., Vagrant NAT 192.168.121.x)
			// Only accept JOINs from the same network as the leader (192.168.100.x)
			// We check the JOIN message address (which is what the node claims to be) rather than
			// the UDP sender IP (which might be NAT'd through Vagrant)
			leaderIP := strings.Split(n.address, ":")[0]
			leaderNetwork := strings.Join(strings.Split(leaderIP, ".")[:3], ".")
			
			// Check the address in the JOIN message itself (this is what matters)
			joinIP := strings.Split(joinMsg.Address, ":")[0]
			joinNetwork := strings.Join(strings.Split(joinIP, ".")[:3], ".")
			
			if joinNetwork != leaderNetwork {
				fmt.Printf("[Leader %s] REJECTING JOIN from %s: wrong network (join=%s, expected=%s)\n",
					n.id[:8], senderID[:8], joinNetwork, leaderNetwork)
				continue // Ignore JOINs from wrong network
			}

			// Split-brain detection: if sender is also claiming to be leader, trigger election
			// Check if sender is already in our registry as a leader
			if broker, ok := n.clusterState.GetBroker(senderID); ok && broker.IsLeader {
				fmt.Printf("[Leader %s] SPLIT-BRAIN DETECTED: Node %s is also claiming to be leader!\n",
					n.id[:8], senderID[:8])
				fmt.Printf("[Leader %s] Triggering election to resolve split-brain...\n", n.id[:8])
				// Trigger election to resolve split-brain
				go func() {
					time.Sleep(1 * time.Second) // Small delay to ensure network is ready
					if err := n.StartElection(); err != nil {
						log.Printf("[Leader %s] Failed to start election for split-brain resolution: %v\n", n.id[:8], err)
					}
				}()
				continue // Don't register the duplicate leader
			}

			// Register new node in state.Registry
			newNodeID := senderID
			n.clusterState.RegisterBroker(newNodeID, joinMsg.Address, false)

			// Replicate new node join immediately
			if err := n.replicateAllState(); err != nil {
				log.Printf("[Leader %s] Failed to replicate after node join: %v\n", n.id[:8], err)
			}

			// Send response using protocol.BroadcastConnection
			responseAddr := fmt.Sprintf("%s", sender)
			err := n.broadcastListener.SendJoinResponse(
				n.id,
				n.address,
				n.config.MulticastGroup,
				[]string{n.address},
				responseAddr,
			)

			if err != nil {
				log.Printf("[Leader %s] Failed to send JOIN_RESPONSE: %v\n", n.id[:8], err)
			} else {
				fmt.Printf("[Leader %s] -> JOIN_RESPONSE to %s\n", n.id[:8], sender)
			}

			// Force replication by resetting lastReplicatedSeq
			// This ensures replicateAllState() will send REPLICATE message
			// even if sequence number hasn't changed
			n.lastReplicatedSeq = 0

			// Use circuit breaker pattern for initial multicast sync
			// This ensures reliable sync even if multicast listener isn't ready yet
			go n.syncFollowerWithCircuitBreaker(newNodeID)
		}
	}
}


func (n *Node) runFollowerDuties() {
	fmt.Printf("[Follower %s] [Follower-Duties] ========================================\n", n.id[:8])
	fmt.Printf("[Follower %s] [Follower-Duties] Starting follower duties\n", n.id[:8])
	fmt.Printf("[Follower %s] [Follower-Duties] Node ID: %s\n", n.id[:8], n.id[:8])
	fmt.Printf("[Follower %s] [Follower-Duties] Address: %s\n", n.id[:8], n.address)
	fmt.Printf("[Follower %s] [Follower-Duties] ========================================\n", n.id[:8])

	// Followers send heartbeats so leader knows they're alive
	// Increased frequency to 2 seconds to prevent timeout issues
	heartbeatTicker := time.NewTicker(2 * time.Second)
	defer heartbeatTicker.Stop()
	
	// Send immediate heartbeat right after joining (before waiting for first tick)
	fmt.Printf("[Follower %s] Sending immediate heartbeat to leader after joining...\n", n.id[:8])
	if err := n.sendFollowerHeartbeat(); err != nil {
		log.Printf("[Follower %s] Failed to send initial heartbeat: %v\n", n.id[:8], err)
	} else {
		fmt.Printf("[Follower %s] Initial heartbeat sent successfully\n", n.id[:8])
	}

	// Check for leader failure periodically
	failureCheckTicker := time.NewTicker(3 * time.Second)
	defer failureCheckTicker.Stop()

	checkCount := 0
	for {
		select {
		case <-heartbeatTicker.C:
			// Send periodic heartbeat
			if err := n.sendFollowerHeartbeat(); err != nil {
				log.Printf("[Follower %s] Failed to send heartbeat: %v\n", n.id[:8], err)
			}

		case <-failureCheckTicker.C:
			checkCount++
			// Check for leader failure
			leaderID := n.failureDetector.GetLeaderID()
			if leaderID == "" {
				// Failure detector not initialized - try to initialize it now
				fmt.Printf("[Follower %s] Failure detector not initialized, attempting initialization...\n", n.id[:8])
				n.initializeFailureDetector()
				leaderID = n.failureDetector.GetLeaderID()
				if leaderID == "" {
					fmt.Printf("[Follower %s] Still no leader in failure detector after initialization attempt\n", n.id[:8])
					continue
				}
			}
			
			suspected, failed, timeSinceHB := n.failureDetector.CheckStatus()
			
			// Log periodic status (every 5th check to avoid spam, ~15 seconds)
			if checkCount%5 == 0 {
				// Safe string formatting for leader ID
				leaderIDStr := "(empty)"
				if len(leaderID) >= 8 {
					leaderIDStr = leaderID[:8]
				} else if leaderID != "" {
					leaderIDStr = leaderID
				}
				
				fmt.Printf("[Follower %s] ========================================\n", n.id[:8])
				fmt.Printf("[Follower %s] Failure detector status check #%d:\n", n.id[:8], checkCount)
				fmt.Printf("[Follower %s]   Leader ID: %s\n", n.id[:8], leaderIDStr)
				fmt.Printf("[Follower %s]   Time since last heartbeat: %v\n", n.id[:8], timeSinceHB.Round(time.Second))
				fmt.Printf("[Follower %s]   Suspected: %v\n", n.id[:8], suspected)
				fmt.Printf("[Follower %s]   Failed: %v\n", n.id[:8], failed)
				fmt.Printf("[Follower %s] ========================================\n", n.id[:8])
			}

			if failed {
				// Leader has failed - start election
				fmt.Printf("\n[Follower %s] ========================================\n", n.id[:8])
				fmt.Printf("[Follower %s] LEADER FAILURE DETECTED!\n", n.id[:8])
				fmt.Printf("[Follower %s] Last heartbeat: %v ago\n", n.id[:8],
					timeSinceHB.Round(time.Second))
				fmt.Printf("[Follower %s] Starting leader election...\n", n.id[:8])
				fmt.Printf("[Follower %s] ========================================\n\n", n.id[:8])

				// Check if election is already in progress - if so, don't start a new one
				if !n.election.IsInProgress() {
					if err := n.StartElection(); err != nil {
						fmt.Printf("[Follower %s] Failed to start election: %v\n", n.id[:8], err)
						fmt.Printf("[Follower %s] Will retry on next failure check if registry syncs\n", n.id[:8])
						// Don't log as error - this is expected if registry isn't ready
					} else {
						fmt.Printf("[Follower %s] Election started successfully\n", n.id[:8])
					}
				} else {
					// Election already in progress - check if it's stuck (timeout)
					if n.election.IsStuck() {
						// Election has been running for more than 60 seconds - it's stuck, reset it
						elapsed := time.Since(n.election.GetStartTime())
						fmt.Printf("[Follower %s] Election stuck - has been running for %v (timeout: 60s), resetting and starting new election\n", 
							n.id[:8], elapsed.Round(time.Second))
						n.election.Reset()
						// Start a new election
						if err := n.StartElection(); err != nil {
							fmt.Printf("[Follower %s] Failed to start new election after reset: %v\n", n.id[:8], err)
						} else {
							fmt.Printf("[Follower %s] New election started after reset\n", n.id[:8])
						}
					} else {
						// Election in progress and not stuck - just wait for it to complete
						fmt.Printf("[Follower %s] Election already in progress, waiting for completion...\n", n.id[:8])
					}
				}

			} else if suspected {
				// Leader is suspected but not yet declared failed
				fmt.Printf("[Follower %s] Leader suspected (no heartbeat for %v)\n",
					n.id[:8], timeSinceHB.Round(time.Second))
				// Could implement additional verification here (ask neighbors, etc.)
			}

		case <-n.stopFollowerDuties:
			fmt.Printf("[Follower %s] Stopping follower duties\n", n.id[:8])
			return
		}
	}
}

// Send follower heartbeat to leader via TCP unicast
// Proposal requirement: "broker nodes send periodic time-to-live messages to the leader node"
func (n *Node) sendFollowerHeartbeat() error {
	if n.IsLeader() {
		return fmt.Errorf("only followers should call this")
	}

	// Try to get leader address from stored address first (from JOIN_RESPONSE)
	// This allows heartbeats even before registry is synchronized
	leaderAddr := n.leaderAddress
	
	// If not available, try to get from registry (for cases where leader changed)
	if leaderAddr == "" {
		brokers := n.clusterState.ListBrokers()
		for _, brokerID := range brokers {
			if broker, ok := n.clusterState.GetBroker(brokerID); ok && broker.IsLeader {
				leaderAddr = broker.Address
				// Update stored address for future use
				n.leaderAddress = leaderAddr
				break
			}
		}
	}

	if leaderAddr == "" {
		return fmt.Errorf("leader address not found (neither stored nor in registry)")
	}

	// Send TCP unicast heartbeat to leader (proposal: TCP for critical messages)
	conn, err := net.DialTimeout("tcp", leaderAddr, 3*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to leader: %w", err)
	}
	defer conn.Close()

	// Create heartbeat message
	heartbeat := protocol.NewHeartbeatMsg(n.id, protocol.NodeType_BROKER, 0)

	// Send heartbeat
	if err := protocol.WriteTCPMessage(conn, heartbeat); err != nil {
		return fmt.Errorf("failed to send heartbeat: %w", err)
	}

	fmt.Printf("[Follower %s] -> HEARTBEAT (TCP) to leader\n", n.id[:8])
	return nil
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

	// Print failure detector status if we're a follower
	if !n.IsLeader() {
		fmt.Println()
		n.failureDetector.PrintStatus()
	}

	fmt.Println("===================\n")
}


func (n *Node) Shutdown() {
	fmt.Printf("\n[Node %s] Shutting down...\n", n.id[:8])

	// Signal shutdown to all goroutines
	n.shutdownCancel()

	// If we're the leader, remove self from registry and replicate
	if n.IsLeader() {
		fmt.Printf("[Leader %s] Performing graceful shutdown...\n", n.id[:8])
		n.clusterState.RemoveBroker(n.id)
		n.replicateAllState()
	}

	// Give goroutines time to exit cleanly
	time.Sleep(2 * time.Second)

	// Stop role-specific goroutines
	close(n.stopLeaderDuties)
	close(n.stopFollowerDuties)

	// Close network connections (protocol package types)
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

	// Close storage log
	if n.storageLog != nil {
		if err := n.storageLog.Close(); err != nil {
			fmt.Printf("[Node %s] WARNING: Failed to close storage log: %v\n", n.id[:8], err)
		} else {
			fmt.Printf("[Node %s] Storage log closed\n", n.id[:8])
		}
	}

	fmt.Printf("[Node %s] Shutdown complete\n", n.id[:8])
}
