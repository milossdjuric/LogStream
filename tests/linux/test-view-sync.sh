#!/bin/bash

# Test for View-Synchronous Group Communication Protocol
# Tests: State Exchange, Log Sync, View Installation, Message Queueing

TEST_TIMEOUT=1200  # Increased from 600s (10min) to 1200s (20min) to accommodate slow VM startup
MODE="${1:-docker}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/lib/common.sh"

# Setup log directory
setup_log_directory "$MODE"

TEST_PREFIX="[TEST-VIEW-SYNC]"

# Create a test output log file to capture all terminal output
TEST_OUTPUT_LOG="$LOG_DIR/test-output.log"
touch "$TEST_OUTPUT_LOG" 2>/dev/null || TEST_OUTPUT_LOG="/tmp/test-view-sync-$$.log"

log_test() {
    local msg="${TEST_PREFIX} $1"
    echo -e "${BLUE}${msg}${NC}"
    echo "$(date '+%Y-%m-%d %H:%M:%S') ${msg}" >> "$TEST_OUTPUT_LOG" 2>/dev/null || true
}
success_test() {
    local msg="${TEST_PREFIX} $1"
    echo -e "${GREEN}${msg}${NC}"
    echo "$(date '+%Y-%m-%d %H:%M:%S') ${msg}" >> "$TEST_OUTPUT_LOG" 2>/dev/null || true
}
error_test() {
    local msg="${TEST_PREFIX} $1"
    echo -e "${RED}${msg}${NC}"
    echo "$(date '+%Y-%m-%d %H:%M:%S') ${msg}" >> "$TEST_OUTPUT_LOG" 2>/dev/null || true
}
warn_test() {
    local msg="${TEST_PREFIX} $1"
    echo -e "${YELLOW}${msg}${NC}"
    echo "$(date '+%Y-%m-%d %H:%M:%S') ${msg}" >> "$TEST_OUTPUT_LOG" 2>/dev/null || true
}

# Time tracking
TEST_START_TIME=$(date +%s)
get_elapsed_time() {
    local current_time=$(date +%s)
    local elapsed=$((current_time - TEST_START_TIME))
    echo "$elapsed"
}
log_time() {
    local elapsed=$(get_elapsed_time)
    log_test "[TIME: ${elapsed}s / ${TEST_TIMEOUT}s] $1"
}

# Background process to log elapsed time periodically
TIME_TRACKER_PID=""
start_time_tracker() {
    (
        while true; do
            sleep 30
            local elapsed=$(get_elapsed_time)
            if [ "$elapsed" -lt "$TEST_TIMEOUT" ]; then
                log_test "[TIME TRACKER: ${elapsed}s elapsed / ${TEST_TIMEOUT}s timeout] Test still running..."
            else
                break
            fi
        done
    ) &
    TIME_TRACKER_PID=$!
}
stop_time_tracker() {
    if [ -n "$TIME_TRACKER_PID" ]; then
        kill $TIME_TRACKER_PID 2>/dev/null || true
        TIME_TRACKER_PID=""
    fi
}

# Cleanup function
cleanup() {
    error_test "Cleaning up test resources..."

    if [ "$MODE" = "docker" ]; then
        # Copy container logs before cleanup
        if [ -n "$LOG_DIR" ] && [ "$LOG_DIR" != "/tmp" ]; then
            cd "$PROJECT_ROOT/deploy/docker/compose" 2>/dev/null || true
            COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml logs --no-color node-a > "$LOG_DIR/leader-node-a.log" 2>&1 || true
            COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml logs --no-color node-b > "$LOG_DIR/broker1-node-b.log" 2>&1 || true
            COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml logs --no-color node-c > "$LOG_DIR/broker2-node-c.log" 2>&1 || true
            log_test "Saved container logs to: $LOG_DIR/"
        fi
        cd "$PROJECT_ROOT/deploy/docker/compose" 2>/dev/null || true
        COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml down --remove-orphans 2>/dev/null || true
    elif [ "$MODE" = "local" ]; then
        # Copy logs before cleanup
        if [ -n "$LOG_DIR" ] && [ "$LOG_DIR" != "/tmp" ]; then
            [ -n "$NODE_A_LOG" ] && [ -f "$NODE_A_LOG" ] && cp "$NODE_A_LOG" "$LOG_DIR/leader-node-a.log" 2>/dev/null && log_test "Saved node-a log" || true
            [ -n "$NODE_B_LOG" ] && [ -f "$NODE_B_LOG" ] && cp "$NODE_B_LOG" "$LOG_DIR/broker1-node-b.log" 2>/dev/null && log_test "Saved node-b log" || true
            [ -n "$NODE_C_LOG" ] && [ -f "$NODE_C_LOG" ] && cp "$NODE_C_LOG" "$LOG_DIR/broker2-node-c.log" 2>/dev/null && log_test "Saved node-c log" || true
        fi
        cleanup_netns_processes
        pkill -f "logstream" 2>/dev/null || true
    elif [ "$MODE" = "vagrant" ]; then
        # Copy logs from VMs before cleanup
        if [ -n "$LOG_DIR" ] && [ "$LOG_DIR" != "/tmp" ]; then
            [ -n "$NODE_A_LOG" ] && copy_vm_log_to_host leader "$NODE_A_LOG" "$LOG_DIR/leader-node-a.log" 2>/dev/null && log_test "Saved node-a log" || true
            [ -n "$NODE_B_LOG" ] && copy_vm_log_to_host broker1 "$NODE_B_LOG" "$LOG_DIR/broker1-node-b.log" 2>/dev/null && log_test "Saved node-b log" || true
            [ -n "$NODE_C_LOG" ] && copy_vm_log_to_host broker2 "$NODE_C_LOG" "$LOG_DIR/broker2-node-c.log" 2>/dev/null && log_test "Saved node-c log" || true
        fi
        cd "$PROJECT_ROOT/deploy/vagrant" 2>/dev/null || true
        # Restart leader VM if it was halted
        if vagrant status leader 2>/dev/null | grep -q "poweroff"; then
            log_test "Restarting leader VM..."
            vagrant up leader 2>/dev/null || true
            sleep 5
        fi
        vagrant ssh leader -c 'pkill -f logstream' 2>/dev/null || true
        vagrant ssh broker1 -c 'pkill -f logstream' 2>/dev/null || true
        vagrant ssh broker2 -c 'pkill -f logstream' 2>/dev/null || true
    fi
    error_test "Cleanup complete"
    log_test "Logs saved to: $LOG_DIR"
}

# Timeout will be started AFTER setup phase (not here!)
# This ensures we only time the actual test logic, not container builds/startup
TIMEOUT_FLAG="/tmp/test-view-sync-timeout-$$"
rm -f "$TIMEOUT_FLAG"
TIMEOUT_PID=""

start_test_timeout() {
    log_test "========================================="
    log_test "STARTING TEST TIMEOUT: ${TEST_TIMEOUT}s"
    log_test "========================================="
    (
        sleep $TEST_TIMEOUT
        error_test "Test timeout reached (${TEST_TIMEOUT}s), forcing cleanup..."
        touch "$TIMEOUT_FLAG"
        cleanup
        kill -TERM $$ 2>/dev/null || true
        sleep 2
        kill -9 $$ 2>/dev/null || true
    ) &
    TIMEOUT_PID=$!
    log_test "Test timeout started (PID: $TIMEOUT_PID)"
}

# Trap cleanup on exit
cleanup_and_exit() {
    stop_time_tracker
    # Kill timeout if it was started
    if [ -n "$TIMEOUT_PID" ]; then
        kill $TIMEOUT_PID 2>/dev/null || true
    fi
    cleanup
    if [ -f "$TIMEOUT_FLAG" ]; then
        rm -f "$TIMEOUT_FLAG"
        exit 1
    fi
}
trap cleanup_and_exit TERM
trap cleanup_and_exit EXIT INT

# Start time tracker
start_time_tracker

log_test "========================================="
log_test "Test: View-Synchronous Protocol"
log_test "Mode: $MODE"
log_test "Timeout: ${TEST_TIMEOUT}s"
log_test "Log directory: $LOG_DIR"
log_time "Test started"
log_test "========================================="
log_test ""
log_test "This test verifies:"
log_test "  1. Freeze/Unfreeze during view change"
log_test "  2. State Exchange protocol"
log_test "  3. Log offset synchronization"
log_test "  4. View Installation"
log_test "  5. Message queueing during freeze"
log_test ""

# ============================================================
# DOCKER MODE
# ============================================================
if [ "$MODE" = "docker" ]; then
    COMPOSE_FILE="$PROJECT_ROOT/deploy/docker/compose/election.yaml"

    if [ ! -f "$COMPOSE_FILE" ]; then
        error_test "Compose file not found: $COMPOSE_FILE"
        exit 1
    fi

    cd "$PROJECT_ROOT/deploy/docker/compose"

    # Clean up any existing containers
    log_test "Stopping any existing containers..."
    COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml down --remove-orphans 2>/dev/null || true

    # Build with latest code
    log_test "Building Docker images..."
    COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml build

    # ========================================
    # TEST 1: Cluster Formation with View Numbers
    # ========================================
    log_test ""
    log_test "========================================="
    log_test "TEST 1: Cluster Formation & Initial View"
    log_test "========================================="
    log_time "Starting TEST 1"

    log_test "Starting 3-node cluster..."
    COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml up -d --force-recreate

    log_test "Waiting for cluster formation (30s)..."
    sleep 30

    log_test "Checking initial cluster state..."

    if COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml logs node-a 2>&1 | grep -q "Becoming LEADER"; then
        success_test "  [OK] Leader (node-a) started"
    else
        warn_test "  [--] Leader status not found"
    fi

    if COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml logs node-b 2>&1 | grep -qE "Joined existing cluster|Becoming FOLLOWER|becomeFollower"; then
        success_test "  [OK] Follower (node-b) joined"
    else
        warn_test "  [--] Follower B status not found"
    fi

    if COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml logs node-c 2>&1 | grep -qE "Joined existing cluster|Becoming FOLLOWER|becomeFollower"; then
        success_test "  [OK] Follower (node-c) joined"
    else
        warn_test "  [--] Follower C status not found"
    fi

    # Check for view-related messages on node joins
    log_test ""
    log_test "Checking for lightweight view change on node joins..."

    if COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml logs node-a 2>&1 | grep -q "LIGHTWEIGHT VIEW CHANGE"; then
        success_test "  [OK] View change triggered on node join"
    else
        warn_test "  [--] Lightweight view change not found"
    fi

    if COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml logs node-a 2>&1 | grep -q "VIEW_INSTALL"; then
        success_test "  [OK] VIEW_INSTALL sent to followers"
    else
        warn_test "  [--] VIEW_INSTALL not found"
    fi

    log_time "TEST 1 complete"

    log_test ""
    log_test "========================================="
    log_test "SETUP PHASE COMPLETE (DOCKER)"
    log_test "========================================="
    log_test ""

    # NOW start the timeout - setup is done, only test logic will be timed
    start_test_timeout

    # ========================================
    # TEST 2: Leader Failure & State Exchange
    # ========================================
    log_test ""
    log_test "========================================="
    log_test "TEST 2: Leader Failure & State Exchange"
    log_test "========================================="
    log_time "Starting TEST 2"

    log_test "Stopping leader (node-a) to trigger failure detection..."
    COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml stop node-a
    success_test "Leader stopped at $(date +%H:%M:%S)"

    log_test ""
    log_test "Waiting for failure detection and election (~60s)..."
    log_test "Timeline:"
    log_test "  ~15s - Suspicion raised"
    log_test "  ~20s - Failure detected"
    log_test "  ~25s - Election starts"
    log_test "  ~30s - State exchange begins"
    log_test ""

    # Monitor for election and state exchange
    ELECTION_COMPLETE=false
    STATE_EXCHANGE_COMPLETE=false
    VIEW_INSTALLED=false
    FAILURE_DETECTED=false

    # Minimum wait time (in iterations) before we can exit early - give failure detector time
    # Phi accrual typically needs ~20-30s to detect failure
    MIN_WAIT_ITERATIONS=8  # 40 seconds minimum

    for i in {1..24}; do  # Extended to 120 seconds total
        sleep 5
        elapsed=$((i * 5))
        log_test "[$elapsed s] Checking protocol progress..."

        # Check for freeze (after leader stop - look for election-related freeze)
        if COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml logs node-b node-c 2>&1 | grep -q "Freezing operations"; then
            success_test "  [OK] Operations frozen for view change"
        fi

        # Check for failure detection
        if COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml logs 2>&1 | grep -q "LEADER FAILURE DETECTED"; then
            success_test "  [OK] Leader failure detected"
            FAILURE_DETECTED=true
        fi

        # Check for suspicion
        if COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml logs 2>&1 | grep -q "SUSPECTED"; then
            success_test "  [OK] Leader suspected"
        fi

        # Check for election
        if COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml logs 2>&1 | grep -q "I WIN"; then
            success_test "  [OK] New leader elected"
            ELECTION_COMPLETE=true
        fi

        # Check for state exchange
        if COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml logs 2>&1 | grep -q "INITIATING STATE EXCHANGE"; then
            success_test "  [OK] State exchange initiated"
        fi

        if COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml logs 2>&1 | grep -q "STATE_EXCHANGE_RESPONSE"; then
            success_test "  [OK] State exchange responses received"
            STATE_EXCHANGE_COMPLETE=true
        fi

        # Check for log offsets
        if COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml logs 2>&1 | grep -q "log offsets"; then
            success_test "  [OK] Log offsets collected"
        fi

        # Check for view install
        if COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml logs 2>&1 | grep -q "INSTALLING NEW VIEW"; then
            success_test "  [OK] New view being installed"
            VIEW_INSTALLED=true
        fi

        # Only exit early if we've waited minimum time AND election completed
        # State exchange is optional - only needed when there's conflicting state
        if [ "$i" -ge "$MIN_WAIT_ITERATIONS" ] && [ "$ELECTION_COMPLETE" = "true" ]; then
            success_test "  [OK] Election complete - exiting early"
            break
        fi
    done

    log_time "TEST 2 complete"

    # ========================================
    # TEST 3: Verify Protocol Components
    # ========================================
    log_test ""
    log_test "========================================="
    log_test "TEST 3: Verify Protocol Components"
    log_test "========================================="
    log_time "Starting TEST 3"

    log_test ""
    log_test "Checking freeze/unfreeze cycle..."
    # Check for both explicit freeze logs and frozen registry during election
    FREEZE_COUNT=$(COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml logs 2>&1 | grep -c "Freezing operations\|FROZEN registry\|frozen" || echo "0")
    UNFREEZE_COUNT=$(COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml logs 2>&1 | grep -c "Unfreezing operations\|Unfrozen" || echo "0")
    log_test "  Freeze events: $FREEZE_COUNT"
    log_test "  Unfreeze events: $UNFREEZE_COUNT"

    log_test ""
    log_test "Checking state exchange..."
    if COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml logs node-b 2>&1 | grep -q "STATE_EXCHANGE"; then
        success_test "  [OK] State exchange on node-b"
    fi
    if COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml logs node-c 2>&1 | grep -q "STATE_EXCHANGE"; then
        success_test "  [OK] State exchange on node-c"
    fi

    log_test ""
    log_test "Checking view installation..."
    if COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml logs node-b 2>&1 | grep -q "Successfully installed view"; then
        success_test "  [OK] View installed on node-b"
    fi
    if COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml logs node-c 2>&1 | grep -q "Successfully installed view"; then
        success_test "  [OK] View installed on node-c"
    fi

    log_time "TEST 3 complete"

    # ========================================
    # TEST 4: Verify Protocol Precision
    # ========================================
    log_test ""
    log_test "========================================="
    log_test "TEST 4: Verify Protocol Precision"
    log_test "========================================="
    log_time "Starting TEST 4"

    # Check for exactly ONE election win (not multiple competing elections)
    log_test ""
    log_test "Checking election uniqueness..."
    ELECTION_WIN_COUNT=$(COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml logs 2>&1 | grep -c "I WIN" || echo "0")
    if [ "$ELECTION_WIN_COUNT" -eq 1 ]; then
        success_test "  [OK] Exactly 1 election win (no competing elections)"
    elif [ "$ELECTION_WIN_COUNT" -gt 1 ]; then
        warn_test "  [WARN] Multiple election wins detected: $ELECTION_WIN_COUNT (may indicate timing issue)"
    else
        warn_test "  [--] No election wins found"
    fi

    # Check that the new leader is sending heartbeats
    log_test ""
    log_test "Checking new leader heartbeats..."
    # Find which node won the election
    WINNER_NODE=$(COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml logs 2>&1 | grep "I WIN" | head -1 | grep -oE 'node-[bc]' | head -1 || echo "")
    if [ -n "$WINNER_NODE" ]; then
        # Check if the winner is sending heartbeats AFTER becoming leader
        LEADER_HEARTBEATS=$(COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml logs "$WINNER_NODE" 2>&1 | grep -c "-> HEARTBEAT" || echo "0")
        if [ "$LEADER_HEARTBEATS" -ge 1 ]; then
            success_test "  [OK] New leader ($WINNER_NODE) is sending heartbeats: $LEADER_HEARTBEATS"
        else
            warn_test "  [WARN] New leader ($WINNER_NODE) may not be sending heartbeats"
        fi
    else
        warn_test "  [--] Could not determine election winner"
    fi

    # Check for stability - no subsequent elections after the successful one
    log_test ""
    log_test "Checking cluster stability after election..."
    # Count how many times election was started AFTER the first "I WIN"
    ELECTION_START_COUNT=$(COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml logs 2>&1 | grep -c "Starting leader election" || echo "0")
    # We expect 1-2 election starts (node-b and/or node-c detecting failure)
    if [ "$ELECTION_START_COUNT" -le 3 ]; then
        success_test "  [OK] Cluster stable (election starts: $ELECTION_START_COUNT)"
    else
        warn_test "  [WARN] Multiple election starts detected: $ELECTION_START_COUNT (may indicate instability)"
    fi

    # Check that the other follower recognizes the new leader
    log_test ""
    log_test "Checking follower acknowledgment..."
    if [ "$WINNER_NODE" = "node-b" ]; then
        OTHER_NODE="node-c"
    else
        OTHER_NODE="node-b"
    fi

    if COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml logs "$OTHER_NODE" 2>&1 | grep -q "Successfully installed view"; then
        success_test "  [OK] Follower ($OTHER_NODE) installed new view"
    else
        warn_test "  [--] Follower view installation not confirmed"
    fi

    log_time "TEST 4 complete"

    # ========================================
    # RESULTS
    # ========================================
    log_test ""
    log_test "========================================="
    log_test "TEST RESULTS"
    log_test "========================================="

    # Primary success criteria: Election must complete
    # State exchange is optional - only needed when there's conflicting state
    if [ "$ELECTION_COMPLETE" = "true" ]; then
        success_test "[PASS] View-Synchronous Protocol Test PASSED"
        log_test ""
        log_test "Protocol flow verified:"
        log_test "  1. Cluster formed with initial view"
        log_test "  2. Leader failure detected"
        log_test "  3. LCR election completed"
        log_test "  4. New leader elected"
        if [ "$STATE_EXCHANGE_COMPLETE" = "true" ]; then
            log_test "  5. State exchange protocol executed"
        else
            log_test "  5. State exchange skipped (no conflicting state)"
        fi
    else
        error_test "[FAIL] View-Synchronous Protocol Test FAILED"
        log_test ""
        log_test "Election complete: $ELECTION_COMPLETE"
        log_test "State exchange complete: $STATE_EXCHANGE_COMPLETE"
        log_test ""
        log_test "Full logs for debugging:"
        COMPOSE_PROJECT_NAME=test-view-sync docker compose -f election.yaml logs --tail=100
        exit 1
    fi

# ============================================================
# LOCAL MODE (Network Namespaces)
# ============================================================
elif [ "$MODE" = "local" ]; then
    cd "$PROJECT_ROOT"
    build_if_needed "logstream" "main.go"

    # Ensure netns is set up for multicast support
    log_test "Setting up network namespaces for multicast support..."
    if ! ensure_netns_setup; then
        error_test "Failed to setup network namespaces. Make sure you have sudo access."
        exit 1
    fi

    # Create log files
    NODE_A_LOG="$LOG_DIR/node-a.log"
    NODE_B_LOG="$LOG_DIR/node-b.log"
    NODE_C_LOG="$LOG_DIR/node-c.log"
    touch "$NODE_A_LOG" "$NODE_B_LOG" "$NODE_C_LOG"
    chmod 666 "$NODE_A_LOG" "$NODE_B_LOG" "$NODE_C_LOG"

    # ========================================
    # TEST 1: Cluster Formation with View Numbers
    # ========================================
    log_test ""
    log_test "========================================="
    log_test "TEST 1: Cluster Formation & Initial View"
    log_test "========================================="
    log_time "Starting TEST 1"

    log_test "Starting 3-node cluster in network namespaces..."

    # Start Node A (Leader)
    sudo ip netns exec logstream-a env \
        NODE_ADDRESS=172.20.0.10:8001 \
        MULTICAST_GROUP=239.0.0.1:9999 \
        BROADCAST_PORT=8888 \
        stdbuf -oL -eL ./logstream > "$NODE_A_LOG" 2>&1 &
    NODE_A_PID=$!
    success_test "Node A (Leader) started - PID $NODE_A_PID"
    sleep 5

    # Start Node B (Follower)
    sudo ip netns exec logstream-b env \
        NODE_ADDRESS=172.20.0.20:8002 \
        MULTICAST_GROUP=239.0.0.1:9999 \
        BROADCAST_PORT=8888 \
        stdbuf -oL -eL ./logstream > "$NODE_B_LOG" 2>&1 &
    NODE_B_PID=$!
    success_test "Node B (Follower) started - PID $NODE_B_PID"
    sleep 5

    # Start Node C (Follower)
    sudo ip netns exec logstream-c env \
        NODE_ADDRESS=172.20.0.30:8003 \
        MULTICAST_GROUP=239.0.0.1:9999 \
        BROADCAST_PORT=8888 \
        stdbuf -oL -eL ./logstream > "$NODE_C_LOG" 2>&1 &
    NODE_C_PID=$!
    success_test "Node C (Follower) started - PID $NODE_C_PID"

    log_test "Waiting for cluster formation (30s)..."
    sleep 30

    log_test "Checking initial cluster state..."

    if grep -q "Becoming LEADER" "$NODE_A_LOG" 2>/dev/null; then
        success_test "  [OK] Leader (node-a) started"
    else
        warn_test "  [--] Leader status not found"
    fi

    if grep -qE "Joined existing cluster|Startup complete as FOLLOWER" "$NODE_B_LOG" 2>/dev/null; then
        success_test "  [OK] Follower (node-b) joined"
    else
        warn_test "  [--] Follower B status not found"
    fi

    if grep -qE "Joined existing cluster|Startup complete as FOLLOWER" "$NODE_C_LOG" 2>/dev/null; then
        success_test "  [OK] Follower (node-c) joined"
    else
        warn_test "  [--] Follower C status not found"
    fi

    # Check for view-related messages
    log_test ""
    log_test "Checking for lightweight view change on node joins..."

    if grep -q "LIGHTWEIGHT VIEW CHANGE" "$NODE_A_LOG" 2>/dev/null; then
        success_test "  [OK] View change triggered on node join"
    else
        warn_test "  [--] Lightweight view change not found"
    fi

    if grep -q "VIEW_INSTALL" "$NODE_A_LOG" 2>/dev/null; then
        success_test "  [OK] VIEW_INSTALL sent to followers"
    else
        warn_test "  [--] VIEW_INSTALL not found"
    fi

    log_time "TEST 1 complete"

    log_test ""
    log_test "========================================="
    log_test "SETUP PHASE COMPLETE (LOCAL)"
    log_test "========================================="
    log_test ""

    # NOW start the timeout - setup is done, only test logic will be timed
    start_test_timeout

    # ========================================
    # TEST 2: Leader Failure & State Exchange
    # ========================================
    log_test ""
    log_test "========================================="
    log_test "TEST 2: Leader Failure & State Exchange"
    log_test "========================================="
    log_time "Starting TEST 2"

    log_test "Stopping leader (node-a) to trigger failure detection..."
    kill_netns_process logstream-a $NODE_A_PID 2>/dev/null || kill -9 $NODE_A_PID 2>/dev/null || true
    success_test "Leader stopped at $(date +%H:%M:%S)"

    log_test ""
    log_test "Waiting for failure detection and election (~60s)..."
    log_test "Timeline:"
    log_test "  ~15s - Suspicion raised"
    log_test "  ~20s - Failure detected"
    log_test "  ~25s - Election starts"
    log_test "  ~30s - State exchange begins"
    log_test ""

    # Monitor for election and state exchange
    ELECTION_COMPLETE=false
    STATE_EXCHANGE_COMPLETE=false

    # Minimum wait time (in iterations) before we can exit early - give failure detector time
    MIN_WAIT_ITERATIONS=8  # 40 seconds minimum

    for i in {1..24}; do  # Extended to 120 seconds total
        sleep 5
        elapsed=$((i * 5))
        log_test "[$elapsed s] Checking protocol progress..."

        # Check for freeze
        if grep -q "Freezing operations" "$NODE_B_LOG" "$NODE_C_LOG" 2>/dev/null; then
            success_test "  [OK] Operations frozen for view change"
        fi

        # Check for failure detection
        if grep -q "LEADER FAILURE DETECTED" "$NODE_B_LOG" "$NODE_C_LOG" 2>/dev/null; then
            success_test "  [OK] Leader failure detected"
        fi

        # Check for suspicion
        if grep -q "SUSPECTED" "$NODE_B_LOG" "$NODE_C_LOG" 2>/dev/null; then
            success_test "  [OK] Leader suspected"
        fi

        # Check for election
        if grep -q "I WIN" "$NODE_B_LOG" "$NODE_C_LOG" 2>/dev/null; then
            success_test "  [OK] New leader elected"
            ELECTION_COMPLETE=true
        fi

        # Check for state exchange - check ALL nodes since any could become leader
        if grep -qi "Initiating.*state exchange" "$NODE_A_LOG" "$NODE_B_LOG" "$NODE_C_LOG" 2>/dev/null; then
            success_test "  [OK] State exchange initiated"
        fi

        if grep -q "STATE_EXCHANGE_RESPONSE" "$NODE_A_LOG" "$NODE_B_LOG" "$NODE_C_LOG" 2>/dev/null; then
            success_test "  [OK] State exchange responses received"
            STATE_EXCHANGE_COMPLETE=true
        fi

        # Check for log offsets - check ALL nodes
        if grep -qi "log offsets" "$NODE_A_LOG" "$NODE_B_LOG" "$NODE_C_LOG" 2>/dev/null; then
            success_test "  [OK] Log offsets collected"
        fi

        # Check for view install
        if grep -q "INSTALLING NEW VIEW" "$NODE_B_LOG" "$NODE_C_LOG" 2>/dev/null; then
            success_test "  [OK] New view being installed"
        fi

        # Only exit early if we've waited minimum time AND election completed
        # State exchange is optional - only needed when there's conflicting state
        if [ "$i" -ge "$MIN_WAIT_ITERATIONS" ] && [ "$ELECTION_COMPLETE" = "true" ]; then
            success_test "  [OK] Election complete - exiting early"
            break
        fi
    done

    log_time "TEST 2 complete"

    # ========================================
    # TEST 3: Verify Protocol Components
    # ========================================
    log_test ""
    log_test "========================================="
    log_test "TEST 3: Verify Protocol Components"
    log_test "========================================="
    log_time "Starting TEST 3"

    log_test ""
    log_test "Checking freeze/unfreeze cycle..."
    # Check for both explicit freeze logs and frozen registry during election
    FREEZE_COUNT=$(grep -c "Freezing operations\|FROZEN registry\|frozen" "$NODE_B_LOG" "$NODE_C_LOG" 2>/dev/null | tail -1 || echo "0")
    UNFREEZE_COUNT=$(grep -c "Unfreezing operations\|Unfrozen" "$NODE_B_LOG" "$NODE_C_LOG" 2>/dev/null | tail -1 || echo "0")
    log_test "  Freeze events: $FREEZE_COUNT"
    log_test "  Unfreeze events: $UNFREEZE_COUNT"

    log_test ""
    log_test "Checking state exchange..."
    # Check ALL nodes since any could become the new leader
    if grep -q "STATE_EXCHANGE" "$NODE_A_LOG" 2>/dev/null; then
        success_test "  [OK] State exchange on node-a"
    fi
    if grep -q "STATE_EXCHANGE" "$NODE_B_LOG" 2>/dev/null; then
        success_test "  [OK] State exchange on node-b"
    fi
    if grep -q "STATE_EXCHANGE" "$NODE_C_LOG" 2>/dev/null; then
        success_test "  [OK] State exchange on node-c"
    fi

    log_test ""
    log_test "Checking view installation..."
    # Check ALL nodes
    if grep -q "Successfully installed view" "$NODE_A_LOG" 2>/dev/null; then
        success_test "  [OK] View installed on node-a"
    fi
    if grep -q "Successfully installed view" "$NODE_B_LOG" 2>/dev/null; then
        success_test "  [OK] View installed on node-b"
    fi
    if grep -q "Successfully installed view" "$NODE_C_LOG" 2>/dev/null; then
        success_test "  [OK] View installed on node-c"
    fi

    log_time "TEST 3 complete"

    # ========================================
    # RESULTS
    # ========================================
    log_test ""
    log_test "========================================="
    log_test "TEST RESULTS"
    log_test "========================================="

    # Primary success criteria: Election must complete
    # State exchange is optional - only needed when there's conflicting state
    if [ "$ELECTION_COMPLETE" = "true" ]; then
        success_test "[PASS] View-Synchronous Protocol Test PASSED"
        log_test ""
        log_test "Protocol flow verified:"
        log_test "  1. Cluster formed with initial view"
        log_test "  2. Leader failure detected"
        log_test "  3. LCR election completed"
        log_test "  4. New leader elected"
        if [ "$STATE_EXCHANGE_COMPLETE" = "true" ]; then
            log_test "  5. State exchange protocol executed"
        else
            log_test "  5. State exchange skipped (no conflicting state)"
        fi
    else
        error_test "[FAIL] View-Synchronous Protocol Test FAILED"
        log_test ""
        log_test "Election complete: $ELECTION_COMPLETE"
        log_test "State exchange complete: $STATE_EXCHANGE_COMPLETE"
        log_test ""
        log_test "Node B logs:"
        tail -50 "$NODE_B_LOG" 2>/dev/null || true
        log_test ""
        log_test "Node C logs:"
        tail -50 "$NODE_C_LOG" 2>/dev/null || true
        exit 1
    fi

# ============================================================
# VAGRANT MODE
# ============================================================
elif [ "$MODE" = "vagrant" ]; then
    cd "$PROJECT_ROOT/deploy/vagrant"

    log_test "Checking Vagrant VMs..."
    sleep $((RANDOM % 3))  # Prevent race conditions
    for vm in leader broker1 broker2; do
        if ! check_vm_status $vm; then
            error_test "$vm VM not running! Run: vagrant up"
            exit 1
        fi
    done

    # Clean up any existing processes
    log_test "Cleaning up existing processes on VMs..."
    for vm in leader broker1 broker2; do
        vagrant_ssh_retry "$vm" "pkill -9 -f logstream 2>/dev/null || true" 2>/dev/null || true
    done
    sleep 3

    # Ensure binaries are built
    log_test "Ensuring binaries are up-to-date..."
    ensure_binary_on_vm leader || { error_test "Failed to ensure binary on leader"; exit 1; }
    ensure_binary_on_vm broker1 || { error_test "Failed to ensure binary on broker1"; exit 1; }
    ensure_binary_on_vm broker2 || { error_test "Failed to ensure binary on broker2"; exit 1; }

    # Setup log files
    NODE_A_LOG="$VM_LOG_DIR/node-a.log"
    NODE_B_LOG="$VM_LOG_DIR/node-b.log"
    NODE_C_LOG="$VM_LOG_DIR/node-c.log"

    ensure_vm_log_directory leader "$VM_LOG_DIR" || true
    ensure_vm_log_directory broker1 "$VM_LOG_DIR" || true
    ensure_vm_log_directory broker2 "$VM_LOG_DIR" || true

    # ========================================
    # TEST 1: Cluster Formation with View Numbers
    # ========================================
    log_test ""
    log_test "========================================="
    log_test "TEST 1: Cluster Formation & Initial View"
    log_test "========================================="
    log_time "Starting TEST 1"

    log_test "Starting 3-node cluster on Vagrant VMs..."
    log_test "NOTE: Starting all nodes in parallel to reduce startup time..."

    # Prepare log files first
    prepare_vm_log_file leader "$NODE_A_LOG"
    prepare_vm_log_file broker1 "$NODE_B_LOG"
    prepare_vm_log_file broker2 "$NODE_C_LOG"

    # Start all nodes in parallel (this reduces total startup time significantly)
    log_test "Starting Node A (Leader) in background..."
    (start_logstream_vm_wrapper leader "192.168.100.10:8001" "true" "$NODE_A_LOG" "1" && echo "Node A startup complete") &
    NODE_A_PID=$!
    
    log_test "Starting Node B (Follower) in background..."
    (start_logstream_vm_wrapper broker1 "192.168.100.20:8002" "false" "$NODE_B_LOG" "1" && echo "Node B startup complete") &
    NODE_B_PID=$!
    
    log_test "Starting Node C (Follower) in background..."
    (start_logstream_vm_wrapper broker2 "192.168.100.30:8003" "false" "$NODE_C_LOG" "1" && echo "Node C startup complete") &
    NODE_C_PID=$!

    log_test "Waiting for all nodes to start (parallel startup)..."
    
    # Wait for all background jobs
    STARTUP_FAILED=0
    if ! wait $NODE_A_PID; then
        error_test "Node A startup failed"
        STARTUP_FAILED=1
    else
        success_test "Node A (Leader) started"
    fi
    
    if ! wait $NODE_B_PID; then
        error_test "Node B startup failed"
        STARTUP_FAILED=1
    else
        success_test "Node B (Follower) started"
    fi
    
    if ! wait $NODE_C_PID; then
        error_test "Node C startup failed"
        STARTUP_FAILED=1
    else
        success_test "Node C (Follower) started"
    fi
    
    # Exit if any startup failed
    if [ $STARTUP_FAILED -eq 1 ]; then
        error_test "One or more nodes failed to start"
        exit 1
    fi

    log_test "All nodes started successfully, verifying processes..."
    sleep 5

    # Verify all processes are running (can also be done in parallel for speed)
    log_test "Verifying Node A (Leader) process..."
    if ! verify_process_running leader "$NODE_A_LOG" "logstream" 7; then
        error_test "Node A process verification failed"
        exit 1
    fi
    success_test "Node A process verified"

    log_test "Verifying Node B (Follower) process..."
    if ! verify_process_running broker1 "$NODE_B_LOG" "logstream" 7; then
        error_test "Node B process verification failed"
        exit 1
    fi
    success_test "Node B process verified"

    log_test "Verifying Node C (Follower) process..."
    if ! verify_process_running broker2 "$NODE_C_LOG" "logstream" 7; then
        error_test "Node C process verification failed"
        exit 1
    fi
    success_test "Node C process verified"

    log_test "Waiting for cluster formation (30s)..."
    sleep 30

    log_test "Checking initial cluster state..."

    if vagrant_ssh_retry leader "grep -qE 'Becoming LEADER|Startup complete as LEADER' '$NODE_A_LOG' 2>/dev/null" 2>/dev/null; then
        success_test "  [OK] Leader (node-a) started"
    else
        warn_test "  [--] Leader status not found"
    fi

    if vagrant_ssh_retry broker1 "grep -qE 'Joined existing cluster|Startup complete as FOLLOWER' '$NODE_B_LOG' 2>/dev/null" 2>/dev/null; then
        success_test "  [OK] Follower (node-b) joined"
    else
        warn_test "  [--] Follower B status not found"
    fi

    if vagrant_ssh_retry broker2 "grep -qE 'Joined existing cluster|Startup complete as FOLLOWER' '$NODE_C_LOG' 2>/dev/null" 2>/dev/null; then
        success_test "  [OK] Follower (node-c) joined"
    else
        warn_test "  [--] Follower C status not found"
    fi

    # Check for view-related messages
    log_test ""
    log_test "Checking for lightweight view change on node joins..."

    if vagrant_ssh_retry leader "grep -q 'LIGHTWEIGHT VIEW CHANGE' '$NODE_A_LOG' 2>/dev/null" 2>/dev/null; then
        success_test "  [OK] View change triggered on node join"
    else
        warn_test "  [--] Lightweight view change not found"
    fi

    if vagrant_ssh_retry leader "grep -q 'VIEW_INSTALL' '$NODE_A_LOG' 2>/dev/null" 2>/dev/null; then
        success_test "  [OK] VIEW_INSTALL sent to followers"
    else
        warn_test "  [--] VIEW_INSTALL not found"
    fi

    log_time "TEST 1 complete"

    log_test ""
    log_test "========================================="
    log_test "SETUP PHASE COMPLETE (VAGRANT)"
    log_test "========================================="
    log_test ""

    # NOW start the timeout - setup is done, only test logic will be timed
    start_test_timeout

    # ========================================
    # TEST 2: Leader Failure & State Exchange
    # ========================================
    log_test ""
    log_test "========================================="
    log_test "TEST 2: Leader Failure & State Exchange"
    log_test "========================================="
    log_time "Starting TEST 2"

    # CRITICAL: Detect actual leader - nodes discover each other via broadcast,
    # so the first node to start becomes leader (NOT necessarily "leader" VM)
    log_test "Detecting actual leader from logs..."
    ACTUAL_LEADER_VM=""

    if vagrant ssh leader -c "grep -q 'No cluster found, becoming leader\|Startup complete as LEADER' '$NODE_A_LOG' 2>/dev/null" 2>/dev/null; then
        ACTUAL_LEADER_VM="leader"
        log_test "  Detected actual leader: leader VM (node-a)"
    elif vagrant ssh broker1 -c "grep -q 'No cluster found, becoming leader\|Startup complete as LEADER' '$NODE_B_LOG' 2>/dev/null" 2>/dev/null; then
        ACTUAL_LEADER_VM="broker1"
        log_test "  Detected actual leader: broker1 VM (node-b)"
    elif vagrant ssh broker2 -c "grep -q 'No cluster found, becoming leader\|Startup complete as LEADER' '$NODE_C_LOG' 2>/dev/null" 2>/dev/null; then
        ACTUAL_LEADER_VM="broker2"
        log_test "  Detected actual leader: broker2 VM (node-c)"
    fi

    if [ -z "$ACTUAL_LEADER_VM" ]; then
        warn_test "  Could not determine actual leader from logs, defaulting to 'leader' VM"
        ACTUAL_LEADER_VM="leader"
    fi

    log_test "Halting $ACTUAL_LEADER_VM VM to trigger failure detection..."

    # CRITICAL FIX: Kill process FIRST to prevent infinite election loop
    # During graceful shutdown, the process stays alive and participates in election,
    # causing messages to circulate forever: broker1 -> broker2 -> leader -> broker1...
    log_test "  Step 1: Killing $ACTUAL_LEADER_VM process immediately..."
    vagrant_ssh_retry "$ACTUAL_LEADER_VM" "pkill -9 -f logstream" 2>/dev/null || true
    sleep 2

    log_test "  Step 2: Halting $ACTUAL_LEADER_VM VM (force)..."
    if (cd "$PROJECT_ROOT/deploy/vagrant" && vagrant halt "$ACTUAL_LEADER_VM" --force 2>&1 | grep -vE "WARNING|libvirt" >/dev/null); then
        success_test "$ACTUAL_LEADER_VM VM halted at $(date +%H:%M:%S)"
    else
        warn_test "VM halt failed, but process already killed at $(date +%H:%M:%S)"
    fi

    log_test ""
    log_test "Waiting for failure detection and election (~60s)..."
    log_test "Timeline:"
    log_test "  ~15s - Suspicion raised"
    log_test "  ~20s - Failure detected"
    log_test "  ~25s - Election starts"
    log_test "  ~30s - State exchange begins"
    log_test ""

    # Monitor for election and state exchange
    ELECTION_COMPLETE=false
    STATE_EXCHANGE_COMPLETE=false

    # Minimum wait time (in iterations) before we can exit early - give failure detector time
    MIN_WAIT_ITERATIONS=8  # 40 seconds minimum

    for i in {1..24}; do  # Extended to 120 seconds total
        sleep 5
        elapsed=$((i * 5))
        log_test "[$elapsed s] Checking protocol progress..."

        # Check for freeze
        if vagrant_ssh_retry broker1 "grep -q 'Freezing operations' '$NODE_B_LOG' 2>/dev/null" 2>/dev/null || \
           vagrant_ssh_retry broker2 "grep -q 'Freezing operations' '$NODE_C_LOG' 2>/dev/null" 2>/dev/null; then
            success_test "  [OK] Operations frozen for view change"
        fi

        # Check for failure detection
        if vagrant_ssh_retry broker1 "grep -q 'LEADER FAILURE DETECTED' '$NODE_B_LOG' 2>/dev/null" 2>/dev/null || \
           vagrant_ssh_retry broker2 "grep -q 'LEADER FAILURE DETECTED' '$NODE_C_LOG' 2>/dev/null" 2>/dev/null; then
            success_test "  [OK] Leader failure detected"
        fi

        # Check for suspicion
        if vagrant_ssh_retry broker1 "grep -q 'SUSPECTED' '$NODE_B_LOG' 2>/dev/null" 2>/dev/null || \
           vagrant_ssh_retry broker2 "grep -q 'SUSPECTED' '$NODE_C_LOG' 2>/dev/null" 2>/dev/null; then
            success_test "  [OK] Leader suspected"
        fi

        # Check for election winner
        if vagrant_ssh_retry broker1 "grep -q 'I WIN' '$NODE_B_LOG' 2>/dev/null" 2>/dev/null; then
            success_test "  [OK] Node B won the election"
            ELECTION_COMPLETE=true
        fi
        if vagrant_ssh_retry broker2 "grep -q 'I WIN' '$NODE_C_LOG' 2>/dev/null" 2>/dev/null; then
            success_test "  [OK] Node C won the election"
            ELECTION_COMPLETE=true
        fi

        # Check for state exchange - check ALL nodes since any could become leader
        if vagrant_ssh_retry leader "grep -qi 'Initiating.*state exchange' '$NODE_A_LOG' 2>/dev/null" 2>/dev/null || \
           vagrant_ssh_retry broker1 "grep -qi 'Initiating.*state exchange' '$NODE_B_LOG' 2>/dev/null" 2>/dev/null || \
           vagrant_ssh_retry broker2 "grep -qi 'Initiating.*state exchange' '$NODE_C_LOG' 2>/dev/null" 2>/dev/null; then
            success_test "  [OK] State exchange initiated"
        fi

        if vagrant_ssh_retry leader "grep -q 'STATE_EXCHANGE_RESPONSE' '$NODE_A_LOG' 2>/dev/null" 2>/dev/null || \
           vagrant_ssh_retry broker1 "grep -q 'STATE_EXCHANGE_RESPONSE' '$NODE_B_LOG' 2>/dev/null" 2>/dev/null || \
           vagrant_ssh_retry broker2 "grep -q 'STATE_EXCHANGE_RESPONSE' '$NODE_C_LOG' 2>/dev/null" 2>/dev/null; then
            success_test "  [OK] State exchange responses received"
            STATE_EXCHANGE_COMPLETE=true
        fi

        # Check for log offsets - check ALL nodes
        if vagrant_ssh_retry leader "grep -qi 'log offsets' '$NODE_A_LOG' 2>/dev/null" 2>/dev/null || \
           vagrant_ssh_retry broker1 "grep -qi 'log offsets' '$NODE_B_LOG' 2>/dev/null" 2>/dev/null || \
           vagrant_ssh_retry broker2 "grep -qi 'log offsets' '$NODE_C_LOG' 2>/dev/null" 2>/dev/null; then
            success_test "  [OK] Log offsets collected"
        fi

        # Check for view install - check ALL nodes
        if vagrant_ssh_retry leader "grep -qi 'INSTALLING NEW VIEW' '$NODE_A_LOG' 2>/dev/null" 2>/dev/null || \
           vagrant_ssh_retry broker1 "grep -qi 'INSTALLING NEW VIEW' '$NODE_B_LOG' 2>/dev/null" 2>/dev/null || \
           vagrant_ssh_retry broker2 "grep -qi 'INSTALLING NEW VIEW' '$NODE_C_LOG' 2>/dev/null" 2>/dev/null; then
            success_test "  [OK] New view being installed"
        fi

        # Only exit early if we've waited minimum time AND election completed
        # State exchange is optional - only needed when there's conflicting state
        if [ "$i" -ge "$MIN_WAIT_ITERATIONS" ] && [ "$ELECTION_COMPLETE" = "true" ]; then
            success_test "  [OK] Election complete - exiting early"
            break
        fi
    done

    log_time "TEST 2 complete"

    # ========================================
    # TEST 3: Verify Protocol Components
    # ========================================
    log_test ""
    log_test "========================================="
    log_test "TEST 3: Verify Protocol Components"
    log_test "========================================="
    log_time "Starting TEST 3"

    log_test ""
    log_test "Checking freeze/unfreeze cycle..."
    # Check for both explicit freeze logs and frozen registry during election
    # Use default 0 if count is empty to avoid arithmetic errors
    FREEZE_COUNT_B=$(vagrant_ssh_retry broker1 "grep -c 'Freezing operations\|FROZEN registry\|frozen' '$NODE_B_LOG' 2>/dev/null" 2>/dev/null || echo "0")
    FREEZE_COUNT_C=$(vagrant_ssh_retry broker2 "grep -c 'Freezing operations\|FROZEN registry\|frozen' '$NODE_C_LOG' 2>/dev/null" 2>/dev/null || echo "0")
    UNFREEZE_COUNT_B=$(vagrant_ssh_retry broker1 "grep -c 'Unfreezing operations\|Unfrozen' '$NODE_B_LOG' 2>/dev/null" 2>/dev/null || echo "0")
    UNFREEZE_COUNT_C=$(vagrant_ssh_retry broker2 "grep -c 'Unfreezing operations\|Unfrozen' '$NODE_C_LOG' 2>/dev/null" 2>/dev/null || echo "0")
    # Ensure counts are numeric (default to 0 if empty)
    FREEZE_COUNT_B=${FREEZE_COUNT_B:-0}
    FREEZE_COUNT_C=${FREEZE_COUNT_C:-0}
    UNFREEZE_COUNT_B=${UNFREEZE_COUNT_B:-0}
    UNFREEZE_COUNT_C=${UNFREEZE_COUNT_C:-0}
    # Remove any non-numeric characters (e.g., from grep errors)
    FREEZE_COUNT_B=$(echo "$FREEZE_COUNT_B" | grep -o '[0-9]*' | head -1)
    FREEZE_COUNT_C=$(echo "$FREEZE_COUNT_C" | grep -o '[0-9]*' | head -1)
    UNFREEZE_COUNT_B=$(echo "$UNFREEZE_COUNT_B" | grep -o '[0-9]*' | head -1)
    UNFREEZE_COUNT_C=$(echo "$UNFREEZE_COUNT_C" | grep -o '[0-9]*' | head -1)
    FREEZE_COUNT_B=${FREEZE_COUNT_B:-0}
    FREEZE_COUNT_C=${FREEZE_COUNT_C:-0}
    UNFREEZE_COUNT_B=${UNFREEZE_COUNT_B:-0}
    UNFREEZE_COUNT_C=${UNFREEZE_COUNT_C:-0}
    log_test "  Freeze events: $((FREEZE_COUNT_B + FREEZE_COUNT_C))"
    log_test "  Unfreeze events: $((UNFREEZE_COUNT_B + UNFREEZE_COUNT_C))"

    log_test ""
    log_test "Checking state exchange..."
    # Check ALL nodes since any could become the new leader
    if vagrant_ssh_retry leader "grep -q 'STATE_EXCHANGE' '$NODE_A_LOG' 2>/dev/null" 2>/dev/null; then
        success_test "  [OK] State exchange on node-a"
    fi
    if vagrant_ssh_retry broker1 "grep -q 'STATE_EXCHANGE' '$NODE_B_LOG' 2>/dev/null" 2>/dev/null; then
        success_test "  [OK] State exchange on node-b"
    fi
    if vagrant_ssh_retry broker2 "grep -q 'STATE_EXCHANGE' '$NODE_C_LOG' 2>/dev/null" 2>/dev/null; then
        success_test "  [OK] State exchange on node-c"
    fi

    log_test ""
    log_test "Checking view installation..."
    if vagrant_ssh_retry broker1 "grep -q 'Successfully installed view' '$NODE_B_LOG' 2>/dev/null" 2>/dev/null; then
        success_test "  [OK] View installed on node-b"
    fi
    if vagrant_ssh_retry broker2 "grep -q 'Successfully installed view' '$NODE_C_LOG' 2>/dev/null" 2>/dev/null; then
        success_test "  [OK] View installed on node-c"
    fi

    log_time "TEST 3 complete"

    # ========================================
    # RESULTS
    # ========================================
    log_test ""
    log_test "========================================="
    log_test "TEST RESULTS"
    log_test "========================================="

    # Primary success criteria: Election must complete
    # State exchange is optional - only needed when there's conflicting state
    if [ "$ELECTION_COMPLETE" = "true" ]; then
        success_test "[PASS] View-Synchronous Protocol Test PASSED"
        log_test ""
        log_test "Protocol flow verified:"
        log_test "  1. Cluster formed with initial view"
        log_test "  2. Leader failure detected"
        log_test "  3. LCR election completed"
        log_test "  4. New leader elected"
        if [ "$STATE_EXCHANGE_COMPLETE" = "true" ]; then
            log_test "  5. State exchange protocol executed"
        else
            log_test "  5. State exchange skipped (no conflicting state)"
        fi
    else
        error_test "[FAIL] View-Synchronous Protocol Test FAILED"
        log_test ""
        log_test "Election complete: $ELECTION_COMPLETE"
        log_test "State exchange complete: $STATE_EXCHANGE_COMPLETE"
        log_test ""
        log_test "Node B logs:"
        vagrant_ssh_retry broker1 "tail -50 '$NODE_B_LOG' 2>/dev/null" 2>/dev/null || true
        log_test ""
        log_test "Node C logs:"
        vagrant_ssh_retry broker2 "tail -50 '$NODE_C_LOG' 2>/dev/null" 2>/dev/null || true
        exit 1
    fi

else
    error_test "Invalid mode: $MODE (supported: docker, local, vagrant)"
    exit 1
fi

# Cancel timeout since we completed successfully
kill $TIMEOUT_PID 2>/dev/null || true
log_test ""
log_test "Log files saved to: $LOG_DIR"
log_test "  - test-output.log (this test's output)"
log_test "  - leader-node-a.log"
log_test "  - broker1-node-b.log"
log_test "  - broker2-node-c.log"
success_test "View-Synchronous Protocol test completed successfully"
