# LogStream Tests - Complete Guide

**Comprehensive documentation of all LogStream tests, what they verify, and how they work**

---

## Table of Contents

1. [Overview](#overview)
2. [Test Infrastructure](#test-infrastructure)
3. [Unit Tests](#unit-tests)
4. [Integration Tests](#integration-tests)
5. [Network Tests](#network-tests)
6. [Test Modes](#test-modes)
7. [How Tests Work](#how-tests-work)

---

## Overview

LogStream has a comprehensive test suite covering:

- **72 Unit Tests** - Component-level verification
- **11 Integration Tests** - End-to-end scenarios
- **2 Network Tests** - Protocol verification with packet capture
- **3 Test Modes** - Local (netns), Docker, Vagrant

**Total Coverage**: All 30 proposal features tested

---

## Test Infrastructure

### Test Modes

LogStream supports 3 testing environments:

| Mode | Technology | Isolation | Speed | Realism | Setup |
|------|------------|-----------|-------|---------|-------|
| **local** | Linux network namespaces | Network stack | Fast | Medium | `sudo` required |
| **docker** | Docker containers | Full containerization | Medium | High | Docker required |
| **vagrant** | KVM/libvirt VMs | Full virtualization | Slow | Highest | VMs required |

### Network Namespace Architecture (Local Mode)

```
Host Machine
├── Bridge: br-logstream (172.20.0.1/24)
│   ├── logstream-a (172.20.0.10) ← Leader
│   ├── logstream-b (172.20.0.20) ← Follower/Broker1
│   └── logstream-c (172.20.0.30) ← Follower/Broker2
│
├── Multicast group: 239.0.0.1:9999 (HEARTBEAT/REPLICATE)
├── Broadcast port: 8888 (JOIN discovery)
└── TCP port: 8001/8002/8003 (client connections)
```

**Why network namespaces?**
- UDP multicast/broadcast don't work on localhost (127.0.0.1)
- Each namespace gets real network interface (veth pair)
- Proper routing through bridge allows realistic testing
- No Docker/VM overhead

### Common Test Library

**File**: `tests/linux/lib/common.sh`

Provides shared functions:
- `log()`, `success()`, `error()` - Colored output
- `ensure_netns_setup()` - Network namespace verification
- `cleanup_netns_processes()` - Process cleanup
- `setup_log_directory()` - Log file management
- `kill_netns_process()` - Safe process termination
- Vagrant VM interaction utilities

All bash tests source this library for consistency.

---

## Unit Tests

**Location**: `tests/unit/`  
**Run**: `go test -v ./tests/unit/`

### Cluster State Tests (`cluster_state_test.go`)

**What they verify**: Cluster registry management

```go
TestClusterState_RegisterBroker      // Broker registration
TestClusterState_RegisterProducer    // Producer registration
TestClusterState_RegisterConsumer    // Consumer registration
TestClusterState_GetBroker          // Broker lookup
TestClusterState_ListBrokers        // Broker enumeration
TestClusterState_RemoveBroker       // Broker removal
TestClusterState_Serialize          // State serialization
TestClusterState_SequenceNum        // Sequence number tracking
```

**Validates**: Registry operations, state serialization, concurrent access

### Election Tests (`election_test.go`)

**What they verify**: LCR ring-based leader election

```go
TestElectionState_StartElection     // Election initialization
TestElectionState_UpdateMaxID       // LCR algorithm: max ID tracking
TestElectionState_DeclareWinner     // Election completion
TestElectionState_Reset             // Election state reset
```

**Validates**: LCR algorithm correctness, ring construction, election state machine

### Failure Detector Tests (`failure_detector_test.go`)

**What they verify**: Phi accrual failure detection

```go
TestPhiAccrualDetector_Basic        // Basic heartbeat tracking
TestPhiAccrualDetector_IncreasingPhi // Phi increases without heartbeats
TestPhiAccrualDetector_ThresholdDetection // Suspicion/failure thresholds
TestPhiAccrualDetector_Recovery     // Recovery after heartbeat resumes
```

**Validates**: Statistical failure detection, phi calculation, threshold-based detection

### Hash Ring Tests (`hash_ring_test.go`)

**What they verify**: Consistent hashing for load balancing

```go
TestConsistentHashRing_AddNode      // Add broker to ring
TestConsistentHashRing_RemoveNode   // Remove broker from ring
TestConsistentHashRing_GetNode      // Lookup broker for key
TestConsistentHashRing_Distribution // Even distribution
```

**Validates**: Consistent hashing algorithm, load distribution, ring operations

### Holdback Queue Tests (`holdback_test.go`)

**What they verify**: FIFO ordering with out-of-order delivery

```go
TestHoldbackQueue_InOrder           // Sequential messages
TestHoldbackQueue_OutOfOrder        // Out-of-order delivery
TestHoldbackQueue_Gap               // Missing sequence numbers
TestHoldbackQueue_Duplicate         // Duplicate detection
```

**Validates**: FIFO guarantees, buffering, sequence gap handling

### Recovery Tests (`recovery_test.go`)

**What they verify**: View-synchronous recovery

```go
TestViewRecovery_StateExchange      // State exchange protocol
TestViewRecovery_AgreedState        // UNION-based log merge
TestViewRecovery_ViewInstallation   // View installation
```

**Validates**: State exchange, UNION-based log merge, view transitions

### View State Tests (`view_test.go`)

**What they verify**: Freeze/unfreeze during view changes

```go
TestViewState_Freeze                // Freeze operations
TestViewState_Unfreeze              // Unfreeze operations
TestViewState_ViewTransition        // View number tracking
```

**Validates**: Operation freezing during membership changes

---

## Integration Tests

**Location**: `tests/linux/`  
**Run**: `cd tests/linux && ./test-NAME.sh MODE`

### Core Functionality Tests

#### 1. test-single.sh

**Purpose**: Verify single node can start and become leader

**Command**:
```bash
cd tests/linux
./test-single.sh local
```

**Flow**:
```
1. Start node in network namespace
2. Node does discovery → no response
3. Node becomes leader
4. Verify leader status logged
```

**Success Criteria**:
- Node starts without errors
- Becomes leader automatically
- TCP/UDP listeners active

**Duration**: ~20 seconds

---

#### 2. test-trio.sh

**Purpose**: Verify 3-node cluster formation with sequence progression

**Command**:
```bash
cd tests/linux
./test-trio.sh local
```

**Flow**:
```
1. Start Leader (seq=1)
2. Leader registers itself
3. Start Follower 1 (seq should increase to 2)
4. Follower 1 joins via JOIN broadcast
5. Leader replicates state (REPLICATE seq=2)
6. Start Follower 2 (seq should increase to 3)
7. Follower 2 joins
8. Leader replicates state (REPLICATE seq=3)
```

**Success Criteria**:
- 1 leader + 2 followers
- Sequence progression: seq=1 → seq=2 → seq=3
- REPLICATE messages sent for each join

**Validates**: Cluster formation, discovery, state replication, sequence tracking

**Duration**: ~30 seconds

---

#### 3. test-sequence.sh

**Purpose**: Demonstrate sequence number progression with detailed logging

**Command**:
```bash
cd tests/linux
./test-sequence.sh local
```

**Flow**:
```
1. Start leader (seq=1)
2. Verify seq=1 in logs
3. Add follower 1 (seq→2)
4. Verify REPLICATE seq=2 sent
5. Add follower 2 (seq→3)
6. Verify REPLICATE seq=3 sent
7. Display sequence progression summary
```

**Success Criteria**:
- Clear sequence progression logged
- Each state change increments sequence
- REPLICATE messages show correct sequence

**Validates**: Monotonic sequence numbers, state change tracking

**Duration**: ~45 seconds

---

#### 4. test-producer-consumer.sh

**Purpose**: Verify end-to-end data flow from producer to consumer

**Command**:
```bash
cd tests/linux
./test-producer-consumer.sh local
```

**Flow**:
```
1. Start leader
2. Start producer
   - Producer sends PRODUCE to leader (TCP)
   - Leader responds with PRODUCE_ACK + assigned broker address
3. Start consumer
   - Consumer sends CONSUME to leader (TCP)
   - Leader responds with broker address
   - Consumer sends SUBSCRIBE to broker (TCP)
4. Producer sends 5 test messages (UDP DATA)
5. Broker receives DATA, stores in log
6. Broker forwards to consumer via RESULT messages (TCP)
7. Verify all messages received
```

**Success Criteria**:
- Producer connects and gets assigned broker
- Consumer subscribes successfully
- All messages flow: Producer → Broker → Consumer
- Message count matches (sent = received)

**Validates**: Client registration, stream assignment, data flow, UDP/TCP coordination

**Duration**: ~30 seconds

---

### Advanced Feature Tests

#### 5. test-election-automatic.sh

**Purpose**: Verify automatic leader failure detection and election

**Command**:
```bash
cd tests/linux
./test-election-automatic.sh local
```

**Flow**:
```
1. Start 3-node cluster (1 leader, 2 followers)
2. Wait 30s for cluster formation
3. Kill leader
4. Followers detect failure via heartbeat timeout (~15-20s)
5. Followers start election (LCR algorithm)
6. Election message circulates ring
7. Node with highest ID becomes new leader
8. New leader performs view-synchronous recovery
```

**Success Criteria**:
- Failure detected within 20s
- Election starts automatically
- New leader elected within 30s
- Log shows: "Election complete", "LEADER ELECTED"

**Validates**: 
- Phi accrual failure detection
- Automatic election triggering
- LCR ring-based election
- View-synchronous recovery

**Duration**: ~60-90 seconds

**Key Logs to Check**:
```
[Follower xxx] LEADER FAILURE DETECTED (Timeout)!
[Follower xxx] Starting leader election...
[Node xxx] Registry has N brokers  (should be 2+, not 0!)
[Node xxx] -> ELECTION (maxID: xxx)
[Node xxx] <- ELECTION (maxID: yyy)
[Leader yyy] ELECTION COMPLETE - I am the new leader!
```

---

#### 6. test-stream-failover.sh

**Purpose**: Verify stream reassignment when broker fails

**Command**:
```bash
cd tests/linux
./test-stream-failover.sh local
```

**Flow**:
```
1. Start leader
2. Start broker1 (separate from leader)
3. Start producer
4. Producer gets assigned to a broker (via consistent hash)
5. Kill the assigned broker
6. Leader detects broker failure via heartbeat timeout (30s)
7. Leader reassigns streams to healthy brokers
8. Leader replicates new assignments
```

**Success Criteria**:
- Broker failure detected within 30-40s
- Log shows: "Timeout: Removed broker"
- Either:
  - "Reassigned stream" (if stream was on failed broker), OR
  - "No streams to reassign" (if stream was on different broker)

**Validates**: 
- Broker heartbeat monitoring
- Timeout-based failure detection
- Stream reassignment logic
- Consistent hashing

**Duration**: ~90-120 seconds (includes 30s timeout)

**Note**: Stream assignment is deterministic (consistent hash), so whether failover occurs depends on which broker gets the stream.

---

#### 7. test-view-sync.sh

**Purpose**: Verify view-synchronous protocol during leader election

**Command**:
```bash
cd tests/linux
./test-view-sync.sh local
```

**Flow**:
```
1. Start 3-node cluster
2. Kill leader
3. Followers detect failure
4. Followers FREEZE operations
5. Followers start election
6. New leader sends STATE_EXCHANGE to all
7. Followers respond with their last applied sequence AND full log entries
8. New leader performs UNION-based log merge (preserves all entries)
9. New leader sends VIEW_INSTALL with merged logs
10. Followers apply merged logs and UNFREEZE operations
11. System resumes with new view
```

**Success Criteria**:
- Operations frozen during view change
- STATE_EXCHANGE protocol executes
- UNION-based log merge performed
- VIEW_INSTALL sent with merged logs
- Operations unfrozen
- New view active

**Validates**:
- View-synchronous membership changes
- Freeze/unfreeze mechanism
- State exchange protocol
- UNION-based log merge (no data loss)

**Duration**: ~120-180 seconds

**Key Logs**:
```
[ViewState] Operations FROZEN for view change
[Leader xxx] Starting state exchange protocol...
[Leader xxx] -> STATE_EXCHANGE to follower yyy
[Leader xxx] <- STATE_RESPONSE from follower yyy
[Leader xxx] Agreed state: seq=N
[Leader xxx] -> VIEW_INSTALL (view=M)
[ViewState] Operations UNFROZEN - view M active
```

---

#### 8. test-state-exchange.sh

**Purpose**: Detailed verification of state exchange protocol with UNION-based log merge

**Command**:
```bash
cd tests/linux
./test-state-exchange.sh local
```

**Flow**:
```
1. Start 3-node cluster
2. Generate data on leader (to create non-trivial state)
3. Kill leader
4. Monitor for state exchange protocol:
   - New leader initiated
   - STATE_EXCHANGE sent
   - Responses received with full log entries
   - UNION-based log merge performed
   - Election completes
```

**Success Criteria**:
- State exchange initiated by new leader
- STATE_EXCHANGE messages sent
- Followers respond with state and full log entries
- UNION-based merge preserves all entries
- Election completes

**Validates**: State exchange protocol, UNION-based log merge, no data loss

**Duration**: ~120-180 seconds

---

### Client Feature Tests

#### 9. test-client-cleanup.sh

**Purpose**: Verify client timeout and cleanup

**Command**:
```bash
cd tests/linux
./test-client-cleanup.sh local
```

**Flow**:
```
1. Start leader
2. Start producer
3. Producer registers with leader
4. Kill producer (simulate crash)
5. Leader detects producer timeout (60s)
6. Leader removes dead producer from registry
7. Leader replicates updated state
```

**Success Criteria**:
- Leader tracks producer heartbeats
- Timeout detection works (60s)
- Dead producer removed from registry

**Validates**: Client heartbeat monitoring, timeout-based cleanup

**Duration**: ~90 seconds

---

#### 10. test-fifo-ordering.sh

**Purpose**: Verify FIFO ordering guarantees

**Command**:
```bash
cd tests/linux
./test-fifo-ordering.sh local
```

**Flow**:
```
1. Start leader
2. Send REPLICATE messages with out-of-order sequences
   - seq=1, seq=3, seq=2, seq=4
3. Verify holdback queue buffers out-of-order messages
4. Verify messages applied in correct order: 1→2→3→4
```

**Success Criteria**:
- Out-of-order messages buffered in holdback queue
- Messages applied in sequence order
- No gaps in applied sequences

**Validates**: Holdback queue, FIFO guarantees, sequence gap handling

**Duration**: ~30 seconds

---

#### 11. test-one-to-one-mapping.sh

**Purpose**: Verify producer exclusivity for topics

**Command**:
```bash
cd tests/linux
./test-one-to-one-mapping.sh local
```

**Flow**:
```
1. Start leader
2. Producer 1 registers for topic "exclusive-test"
3. Producer 2 tries to register for same topic
4. Verify: Producer 2 rejected or handled appropriately
```

**Success Criteria**:
- First producer gets topic
- Second producer cannot hijack topic
- Stream assignment respects exclusivity

**Validates**: Topic exclusivity, producer registration logic

**Duration**: ~30 seconds

---

### Network Protocol Tests

#### 12. test-protocol-compliance.sh

**Purpose**: Verify correct protocol usage with packet capture

**Command**:
```bash
cd tests/linux
./test-protocol-compliance.sh local
```

**Flow**:
```
1. Start tcpdump packet capture (on br-logstream for local mode)
2. Start single broker
3. Capture all traffic:
   - UDP port 9999 (multicast)
   - TCP port 8001 (client connections)
   - UDP port 8888 (broadcast discovery)
4. Stop capture
5. Analyze PCAP with tshark:
   - Verify UDP broadcast for JOIN
   - Verify TCP for connections
   - Verify UDP multicast for HEARTBEAT
```

**Success Criteria**:
- UDP broadcast traffic detected (port 8888)
- TCP traffic detected (port 8001)
- UDP multicast traffic detected (239.0.0.1:9999) - if not on loopback

**Validates**: Protocol correctness, proper port usage, multicast configuration

**Output**: PCAP file for Wireshark analysis

**PCAP Location**:
```bash
# Find latest capture
ls -dt /tmp/protocol_compliance_* | head -1
# File: /tmp/protocol_compliance_TIMESTAMP/protocol_capture.pcap

# Analyze with Wireshark
PCAP_DIR=$(ls -dt /tmp/protocol_compliance_* | head -1)
wireshark "$PCAP_DIR/protocol_capture.pcap"
```

**Duration**: ~30 seconds

**Note**: On loopback interface, multicast won't be captured (test limitation, not code bug)

---

#### 13. test-network-analysis.sh

**Purpose**: Detailed network behavior analysis with packet capture

**Command**:
```bash
cd tests/linux
./test-network-analysis.sh local
```

**Flow**:
```
1. Start packet capture
2. Start 3-node cluster
3. Capture cluster formation traffic
4. Analyze:
   - JOIN/JOIN_RESPONSE patterns
   - HEARTBEAT frequency and timing
   - REPLICATE messages on state changes
   - TCP connection establishment
```

**Success Criteria**:
- All expected message types captured
- Heartbeat pattern regular (~5s intervals)
- State replication occurs on joins

**Validates**: Network behavior, timing, message patterns

**Output**: PCAP file + detailed analysis

**PCAP Location**:
```bash
# Find latest capture
ls -dt /tmp/logstream_captures_* | head -1
# File: /tmp/logstream_captures_TIMESTAMP/logstream_network.pcap

# Analyze with Wireshark
PCAP_DIR=$(ls -dt /tmp/logstream_captures_* | head -1)
wireshark "$PCAP_DIR/logstream_network.pcap"
```

**Duration**: ~60 seconds

---

## Test Modes Explained

### Local Mode (Network Namespaces)

**Pros**:
- Fast (no container/VM overhead)
- Realistic networking (real interfaces)
- Easy debugging (direct log access)
- Works on any Linux system

**Cons**:
- Requires `sudo` for namespace setup
- Linux-only

**When to use**: Development, CI/CD, quick iteration

**Setup**:
```bash
sudo ./deploy/netns/setup-netns.sh  # One-time
```

**Run**:
```bash
cd tests/linux
./test-trio.sh local
```

---

### Docker Mode

**Pros**:
- Full isolation (containers)
- Reproducible environment
- No `sudo` needed (after Docker setup)
- Works on Linux/macOS/Windows

**Cons**:
- Slower than network namespaces
- Requires Docker installation
- More complex cleanup

**When to use**: Testing deployment configurations, cross-platform CI

**Setup**: Install Docker and Docker Compose

**Run**:
```bash
cd tests/linux
./test-trio.sh docker
```

**Compose Files**: `deploy/docker/compose/*.yaml`

---

### Vagrant Mode

**Pros**:
- Most realistic (full VMs)
- True multi-machine simulation
- Can test VM-specific scenarios

**Cons**:
- Slowest (VM overhead)
- Requires KVM/libvirt or VirtualBox
- Large resource usage

**When to use**: Final validation, testing deployment procedures

**Setup**:
```bash
cd deploy/vagrant
vagrant up  # First time: downloads box, provisions VMs
```

**Run**:
```bash
cd tests/linux
./test-trio.sh vagrant
```

**VM Configuration**: 
- Leader: 192.168.100.10
- Broker1: 192.168.100.20
- Broker2: 192.168.100.30

---

## How Tests Work

### Bash Integration Test Structure

Every bash test follows this pattern:

```bash
#!/bin/bash

# 1. Configuration
TEST_TIMEOUT=600
MODE="${1:-local}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# 2. Source common library
source "$SCRIPT_DIR/lib/common.sh"

# 3. Setup logging
setup_log_directory "$MODE"

# 4. Define cleanup function
cleanup() {
    # Preserve logs
    # Kill processes
    # Clean resources
}
trap cleanup EXIT

# 5. Test execution
if [ "$MODE" = "local" ]; then
    # Network namespace mode
    ensure_netns_setup
    sudo ip netns exec logstream-a env NODE_ADDRESS=172.20.0.10:8001 ./logstream &
    # ... test logic ...
elif [ "$MODE" = "docker" ]; then
    # Docker mode
    cd deploy/docker/compose
    docker compose up -d
    # ... test logic ...
elif [ "$MODE" = "vagrant" ]; then
    # Vagrant mode
    vagrant ssh leader -c './logstream &'
    # ... test logic ...
fi

# 6. Verification
# Check logs for expected output
# Verify state changes
# Return exit code
```

### Test Execution Flow

```
┌─────────────────────────────────────────────────────────────┐
│ Test Script Execution                                       │
└─────────────────────────────────────────────────────────────┘

1. Parse MODE argument (local/docker/vagrant)
   ↓
2. Source common.sh library
   ↓
3. Setup log directory (test-logs/MODE-all-TIMESTAMP/)
   ↓
4. Set up cleanup trap (ensures cleanup on exit/error)
   ↓
5. Build binaries if needed (logstream, producer, consumer)
   ↓
6. Setup environment (network namespaces/Docker/Vagrant)
   ↓
7. Start nodes in sequence (with delays for stability)
   ↓
8. Perform test actions (kill leader, send data, etc.)
   ↓
9. Monitor logs for expected behavior
   ↓
10. Verify success/failure criteria
    ↓
11. Preserve logs to permanent location
    ↓
12. Cleanup resources (via trap)
    ↓
13. Exit with code (0=pass, 1=fail)
```

### Log File Management

```
test-logs/
├── local-all-20260129-195048/     ← Timestamp of test run
│   ├── single/
│   │   ├── leader.log
│   │   ├── test.log
│   │   └── exit_code.txt
│   ├── trio/
│   │   ├── leader.log
│   │   ├── follower1.log
│   │   ├── follower2.log
│   │   └── test.log
│   ├── election-automatic/
│   │   ├── leader-node-a.log
│   │   ├── broker1-node-b.log
│   │   ├── broker2-node-c.log
│   │   └── test.log
│   └── ...
```

---

## Test Verification Patterns

### Checking for Success

Tests verify success by grepping logs for specific patterns:

**Example 1: Cluster Formation**
```bash
if grep -q "Follower.*joined" "$LEADER_LOG"; then
    success "Cluster formed"
fi
```

**Example 2: Election Complete**
```bash
if grep -q "ELECTION COMPLETE\|I am the new leader" "$FOLLOWER_LOG"; then
    success "Election completed"
fi
```

**Example 3: Data Flow**
```bash
SENT=$(grep -c "-> DATA" "$PRODUCER_LOG")
RECEIVED=$(grep -c "Received.*DATA" "$CONSUMER_LOG")
if [ "$SENT" -eq "$RECEIVED" ]; then
    success "All messages delivered"
fi
```

### Timing and Timeouts

Tests use timeouts to prevent hanging:

```bash
# Example from test-election-automatic.sh
TEST_TIMEOUT=900  # 15 minutes max

# Background timeout process
(
    sleep $TEST_TIMEOUT
    error "Test timeout reached (${TEST_TIMEOUT}s), forcing cleanup..."
    cleanup
    exit 1
) &
TIMEOUT_PID=$!

# Clean up timeout process on exit
trap 'kill $TIMEOUT_PID 2>/dev/null; cleanup' EXIT
```

Tests also have **internal timers** to track progress:
```bash
# Example: Wait up to 60s for election
for i in {1..60}; do
    if grep -q "Election complete" "$LOG"; then
        break
    fi
    sleep 1
done
```

---

## Common Test Issues

### Issue 1: UDP Read Timeouts in Logs

**Symptom**:
```
[Node xxx] [UDP-DATA-Listener] Read error (will retry): i/o timeout
```

**Status**: EXPECTED - not an error

**Explanation**: 
- UDP listener uses 1-second read timeout
- Allows checking for shutdown every second
- When no data arrives, timeout occurs and logs
- Listener continues normally

---

### Issue 2: Registry Empty During Election

**Symptom**:
```
[Node xxx] Registry has 0 brokers
[Node xxx] Cannot start election
```

**Status**: FIXED

**Cause**: Broadcast listener race condition

**Fix**: Only leader responds to JOIN messages

---

### Issue 3: No Multicast in Network Tests

**Symptom**:
```
[!] No UDP multicast traffic found (this may be a loopback limitation)
```

**Status**: KNOWN LIMITATION (not a code bug)

**Cause**: Capturing on loopback interface

**Fix**: Run in local mode with network namespaces (uses br-logstream)

---

## Running All Tests

### Option 1: Individual Mode

```bash
# Local mode
cd tests/linux/integration
./test-all-local.sh

# Docker mode
./test-all-docker.sh

# Vagrant mode (requires: cd deploy/vagrant && vagrant up)
./test-all-vagrant.sh
```

### Option 2: All Modes (Complete Validation)

```bash
cd tests/linux

# Create script to run all modes
cat > run-all-modes.sh << 'EOF'
#!/bin/bash
set -e

echo "=== Running Local Tests ==="
cd integration && ./test-all-local.sh

echo "=== Running Docker Tests ==="
./test-all-docker.sh

echo "=== Running Vagrant Tests ==="
cd ../deploy/vagrant && vagrant up
cd ../../tests/linux/integration
./test-all-vagrant.sh

echo "=== All Modes Complete ==="
EOF

chmod +x run-all-modes.sh
./run-all-modes.sh
```

---

## Test Results Interpretation

### Successful Test Output

```
[TEST-NAME] [OK] Test PASSED
========================================
Test completed: Thu Jan 29 19:52:00 PM CET 2026
Exit code: 0
========================================
```

### Failed Test Output

```
[TEST-NAME] [FAIL] Test FAILED
========================================
Test completed: Thu Jan 29 19:52:00 PM CET 2026
Exit code: 1
========================================
```

### Log Artifacts

After each test run:
```bash
# Test creates timestamped directory
test-logs/local-all-20260129-195048/

# Each test has subdirectory with:
- Node logs (leader.log, follower1.log, etc.)
- Test output (test.log)
- Exit code (exit_code.txt)
```

---

## Performance Testing

### Benchmarks

```bash
# Run all benchmarks
go test -v -bench=. -benchmem ./tests/unit/

# Specific benchmark
go test -v -bench=BenchmarkClusterState ./tests/unit/
```

### Stress Tests

```bash
go test -v ./tests/stress/
```

### Chaos Tests

```bash
go test -v ./tests/chaos/
```

---

## Summary

LogStream has **87 total tests**:
- 72 unit tests (Go)
- 11 integration tests (Bash)
- 2 network tests (Bash + tcpdump)
- 2 stress/chaos test suites (Go)

**Coverage**: 100% of proposal features

**Test Modes**: 3 (local, docker, vagrant)

**Typical Test Run**: 10-15 minutes for full suite

**CI/CD Ready**: All tests can run automatically in pipelines
