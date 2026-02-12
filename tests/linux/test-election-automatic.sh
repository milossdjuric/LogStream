#!/bin/bash

# Test for AUTOMATIC failure detection (no manual trigger)
# This test waits longer to see if automatic failure detection works

TEST_TIMEOUT=1200  # Increased from 900s to 1200s (20min) - Vagrant is slower
MODE="${1:-docker}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/lib/common.sh"

# Setup log directory
setup_log_directory "$MODE"

TEST_PREFIX="[TEST-ELECTION-AUTO]"
log_test() { 
    local msg="${TEST_PREFIX} $1"
    echo -e "${BLUE}${msg}${NC}"
    [ -n "$TEST_OUTPUT_LOG" ] && echo "$(date '+%Y-%m-%d %H:%M:%S') ${msg}" >> "$TEST_OUTPUT_LOG" 2>/dev/null || true
}
success_test() { 
    local msg="${TEST_PREFIX} $1"
    echo -e "${GREEN}${msg}${NC}"
    [ -n "$TEST_OUTPUT_LOG" ] && echo "$(date '+%Y-%m-%d %H:%M:%S') ${msg}" >> "$TEST_OUTPUT_LOG" 2>/dev/null || true
}
error_test() { 
    local msg="${TEST_PREFIX} $1"
    echo -e "${RED}${msg}${NC}"
    [ -n "$TEST_OUTPUT_LOG" ] && echo "$(date '+%Y-%m-%d %H:%M:%S') ${msg}" >> "$TEST_OUTPUT_LOG" 2>/dev/null || true
}

# Cleanup function
cleanup() {
    error_test "Cleaning up test resources..."
    
    # Copy process logs from VMs to host BEFORE killing processes (vagrant mode only)
    if [ "$MODE" = "vagrant" ] && [ -n "$LOG_DIR" ] && [ "$LOG_DIR" != "/tmp" ]; then
        log_test "Copying process logs from VMs to host before cleanup..."
        # Try to copy logs even if variables aren't set (defensive approach)
        # First try with variables if they exist, then try default paths
        if [ -n "$NODE_A_LOG" ]; then
            if copy_vm_log_to_host leader "$NODE_A_LOG" "$LOG_DIR/leader-node-a.log"; then
                local line_count=$(wc -l < "$LOG_DIR/leader-node-a.log" 2>/dev/null || echo "0")
                log_test "Copied node-a log ($line_count lines) to: $LOG_DIR/leader-node-a.log"
            else
                log_test "Warning: Failed to copy node-a log"
            fi
        elif [ -n "$VM_LOG_DIR" ]; then
            # Try default path if variable wasn't set
            local default_log="$VM_LOG_DIR/node-a.log"
            if copy_vm_log_to_host leader "$default_log" "$LOG_DIR/leader-node-a.log" 2>/dev/null; then
                log_test "Copied node-a log (using default path) to: $LOG_DIR/leader-node-a.log"
            fi
        fi
        if [ -n "$NODE_B_LOG" ]; then
            if copy_vm_log_to_host broker1 "$NODE_B_LOG" "$LOG_DIR/broker1-node-b.log"; then
                local line_count=$(wc -l < "$LOG_DIR/broker1-node-b.log" 2>/dev/null || echo "0")
                log_test "Copied node-b log ($line_count lines) to: $LOG_DIR/broker1-node-b.log"
            else
                log_test "Warning: Failed to copy node-b log"
            fi
        elif [ -n "$VM_LOG_DIR" ]; then
            local default_log="$VM_LOG_DIR/node-b.log"
            if copy_vm_log_to_host broker1 "$default_log" "$LOG_DIR/broker1-node-b.log" 2>/dev/null; then
                log_test "Copied node-b log (using default path) to: $LOG_DIR/broker1-node-b.log"
            fi
        fi
        if [ -n "$NODE_C_LOG" ]; then
            if copy_vm_log_to_host broker2 "$NODE_C_LOG" "$LOG_DIR/broker2-node-c.log"; then
                local line_count=$(wc -l < "$LOG_DIR/broker2-node-c.log" 2>/dev/null || echo "0")
                log_test "Copied node-c log ($line_count lines) to: $LOG_DIR/broker2-node-c.log"
            else
                log_test "Warning: Failed to copy node-c log"
            fi
        elif [ -n "$VM_LOG_DIR" ]; then
            local default_log="$VM_LOG_DIR/node-c.log"
            if copy_vm_log_to_host broker2 "$default_log" "$LOG_DIR/broker2-node-c.log" 2>/dev/null; then
                log_test "Copied node-c log (using default path) to: $LOG_DIR/broker2-node-c.log"
            fi
        fi
    fi
    
    if [ "$MODE" = "docker" ]; then
        # Copy container logs if not already copied (safety net for early exits)
        # Main script flow copies logs before cleanup, but this ensures we get logs even on early exit
        # Note: docker compose logs works even on stopped containers
        if [ -n "$LOG_DIR" ] && [ "$LOG_DIR" != "/tmp" ]; then
            # Only copy if log files don't already exist (avoid overwriting)
            if [ ! -f "$LOG_DIR/leader-node-a.log" ] || [ ! -f "$LOG_DIR/broker1-node-b.log" ] || [ ! -f "$LOG_DIR/broker2-node-c.log" ]; then
                log_test "Copying container logs (cleanup safety net)..."
                cd "$PROJECT_ROOT/deploy/docker/compose" 2>/dev/null || true
                # docker compose logs works even on stopped containers
                COMPOSE_PROJECT_NAME=test-election-auto docker compose -f election.yaml logs --no-color node-a > "$LOG_DIR/leader-node-a.log" 2>&1 || true
                COMPOSE_PROJECT_NAME=test-election-auto docker compose -f election.yaml logs --no-color node-b > "$LOG_DIR/broker1-node-b.log" 2>&1 || true
                COMPOSE_PROJECT_NAME=test-election-auto docker compose -f election.yaml logs --no-color node-c > "$LOG_DIR/broker2-node-c.log" 2>&1 || true
            fi
        fi
        cd "$PROJECT_ROOT/deploy/docker/compose" 2>/dev/null || true
        COMPOSE_PROJECT_NAME=test-election-auto docker compose -f election.yaml down --remove-orphans 2>/dev/null || true
    elif [ "$MODE" = "local" ]; then
        # Copy node log files BEFORE killing processes (so we capture final output)
        if [ -n "$LOG_DIR" ] && [ "$LOG_DIR" != "/tmp" ]; then
            log_test "Preserving node log files..."
            if [ -n "$NODE_A_LOG" ] && [ -f "$NODE_A_LOG" ]; then
                cp "$NODE_A_LOG" "$LOG_DIR/leader-node-a.log" 2>/dev/null || true
                log_test "Saved node-a log to: $LOG_DIR/leader-node-a.log"
                rm -f "$NODE_A_LOG" 2>/dev/null || true
            fi
            if [ -n "$NODE_B_LOG" ] && [ -f "$NODE_B_LOG" ]; then
                cp "$NODE_B_LOG" "$LOG_DIR/broker1-node-b.log" 2>/dev/null || true
                log_test "Saved node-b log to: $LOG_DIR/broker1-node-b.log"
                rm -f "$NODE_B_LOG" 2>/dev/null || true
            fi
            if [ -n "$NODE_C_LOG" ] && [ -f "$NODE_C_LOG" ]; then
                cp "$NODE_C_LOG" "$LOG_DIR/broker2-node-c.log" 2>/dev/null || true
                log_test "Saved node-c log to: $LOG_DIR/broker2-node-c.log"
                rm -f "$NODE_C_LOG" 2>/dev/null || true
            fi
        fi
        cleanup_netns_processes
        pkill -f "logstream" 2>/dev/null || true
    elif [ "$MODE" = "vagrant" ]; then
        cd "$PROJECT_ROOT/deploy/vagrant" 2>/dev/null || true
        # Check if leader VM is halted and restart it if needed
        if vagrant status leader 2>/dev/null | grep -q "poweroff\|not created"; then
            log_test "Leader VM was halted, restarting it..."
            vagrant up leader 2>/dev/null || true
            sleep 5  # Give VM time to boot
        fi
        # Kill processes on all VMs
        vagrant ssh leader -c 'pkill -f logstream' 2>/dev/null || true
        vagrant ssh broker1 -c 'pkill -f logstream' 2>/dev/null || true
        vagrant ssh broker2 -c 'pkill -f logstream' 2>/dev/null || true
    fi
    error_test "Cleanup complete"
}

# Timeout will be started AFTER setup phase (not here!)
# This ensures we only time the actual test logic, not VM prep/binary builds/node startup
TIMEOUT_FLAG="/tmp/test-election-auto-timeout-$$"
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
        # Exit with error code when timeout occurs
        kill -TERM $$ 2>/dev/null || true
    ) &
    TIMEOUT_PID=$!
    log_test "Test timeout started (PID: $TIMEOUT_PID)"
}

# Trap cleanup on exit - exit with error on TERM (timeout), success on normal exit
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
# Time tracking - must be before traps
TEST_START_TIME=$(date +%s)

# Create a test output log file to capture all terminal output
TEST_OUTPUT_LOG="$LOG_DIR/test-output.log"
touch "$TEST_OUTPUT_LOG" 2>/dev/null || TEST_OUTPUT_LOG="/tmp/test-election-auto-$$.log"

# Time tracking functions - must be defined before traps
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

trap cleanup_and_exit TERM
trap cleanup_and_exit EXIT INT

# Start time tracker
start_time_tracker

log_test "========================================="
log_test "Test: Leader Election (AUTOMATIC Detection)"
log_test "Mode: $MODE"
log_test "Timeout: ${TEST_TIMEOUT}s"
log_time "Test started"
log_test "========================================="
log_test ""
log_test "This test verifies AUTOMATIC failure detection"
log_test "No manual trigger will be used - waiting for automatic detection"
log_time "Test started"
log_test ""

if [ "$MODE" = "docker" ]; then
    COMPOSE_FILE="$PROJECT_ROOT/deploy/docker/compose/election.yaml"
    
    if [ ! -f "$COMPOSE_FILE" ]; then
        error_test "Compose file not found: $COMPOSE_FILE"
        exit 1
    fi
    
    cd "$PROJECT_ROOT/deploy/docker/compose"
    
    log_test "Stopping any existing containers..."
    COMPOSE_PROJECT_NAME=test-election-auto docker compose -f election.yaml down --remove-orphans 2>/dev/null || true
    
    log_test "Building Docker images (to ensure latest code is used)..."
    COMPOSE_PROJECT_NAME=test-election-auto docker compose -f election.yaml build
    
    log_test "Starting 3-node cluster..."
    COMPOSE_PROJECT_NAME=test-election-auto docker compose -f election.yaml up -d --force-recreate
    if [ $? -ne 0 ]; then
        error_test "Failed to start docker containers"
        exit 1
    fi
    
    log_test ""
    log_test "Waiting for cluster formation (30s)..."
    sleep 30
    
    log_test ""
    log_test "Checking failure detector initialization..."
    log_test "Node B failure detector status:"
    COMPOSE_PROJECT_NAME=test-election-auto docker compose -f election.yaml logs node-b 2>&1 | grep -i "failuredetector\|initializing\|leader" | tail -10 || log_test "  (no logs yet)"
    log_test ""
    log_test "Node C failure detector status:"
    COMPOSE_PROJECT_NAME=test-election-auto docker compose -f election.yaml logs node-c 2>&1 | grep -i "failuredetector\|initializing\|leader" | tail -10 || log_test "  (no logs yet)"
    
    log_test ""
    log_test "Cluster state before leader failure:"
    COMPOSE_PROJECT_NAME=test-election-auto docker compose -f election.yaml logs node-b 2>&1 | grep -E "Total Brokers|Registry|FailureDetector" | tail -5 || log_test "  (waiting for logs...)"
    
    log_test ""
    log_test "========================================="
    log_test "SIMULATING LEADER FAILURE"
    log_test "========================================="
    log_test "Stopping leader (node-a)..."
    COMPOSE_PROJECT_NAME=test-election-auto docker compose -f election.yaml stop node-a
    if [ $? -ne 0 ]; then
        error_test "Failed to stop leader container"
        exit 1
    fi
    success_test "Leader stopped at $(date +%H:%M:%S)"
    
    log_test ""
    log_test "========================================="
    log_test "WAITING FOR AUTOMATIC FAILURE DETECTION"
    log_test "========================================="
    log_test "Timeline:"
    log_test "  ~15s - Suspicion should be raised"
    log_test "  ~20s - Failure should be detected"
    log_test "  ~25s - Election should be initiated"
    log_test "  ~30s - Election should complete"
    log_test ""
    log_test "Monitoring logs every 5 seconds..."
    
    ELECTION_DETECTED=false
    for i in {1..12}; do
        sleep 5
        elapsed=$((i * 5))
        log_test "[$elapsed s] Checking for election activity..."
        
        # Check for failure detection
        if COMPOSE_PROJECT_NAME=test-election-auto docker compose -f election.yaml logs node-b 2>&1 | grep -q "LEADER FAILURE DETECTED"; then
            success_test "Failure detected on node-b at ${elapsed}s"
            ELECTION_DETECTED=true
        fi
        if COMPOSE_PROJECT_NAME=test-election-auto docker compose -f election.yaml logs node-c 2>&1 | grep -q "LEADER FAILURE DETECTED"; then
            success_test "Failure detected on node-c at ${elapsed}s"
            ELECTION_DETECTED=true
        fi
        
        # Check for election start
        if COMPOSE_PROJECT_NAME=test-election-auto docker compose -f election.yaml logs node-b 2>&1 | grep -q "STARTING LEADER ELECTION"; then
            success_test "Election started on node-b at ${elapsed}s"
        fi
        if COMPOSE_PROJECT_NAME=test-election-auto docker compose -f election.yaml logs node-c 2>&1 | grep -q "STARTING LEADER ELECTION"; then
            success_test "Election started on node-c at ${elapsed}s"
        fi
        
        # Check for winner
        if COMPOSE_PROJECT_NAME=test-election-auto docker compose -f election.yaml logs node-b 2>&1 | grep -q "Message returned to me - I WIN"; then
            success_test "Node B won the election!"
            ELECTION_WINNER="B"
            break
        fi
        if COMPOSE_PROJECT_NAME=test-election-auto docker compose -f election.yaml logs node-c 2>&1 | grep -q "Message returned to me - I WIN"; then
            success_test "Node C won the election!"
            ELECTION_WINNER="C"
            break
        fi
    done
    
    log_test ""
    log_test "========================================="
    log_test "ELECTION RESULTS"
    log_test "========================================="
    
    if [ -n "$ELECTION_WINNER" ]; then
        success_test "[OK] Election test PASSED (Node $ELECTION_WINNER won automatically)"
        
        log_test ""
        log_test "Failure detection timeline:"
        if [ "$ELECTION_WINNER" = "B" ]; then
            COMPOSE_PROJECT_NAME=test-election-auto docker compose -f election.yaml logs node-b 2>&1 | grep -E "FailureDetector|FAILURE DETECTED|SUSPICION|Starting leader election" | tail -15
        else
            COMPOSE_PROJECT_NAME=test-election-auto docker compose -f election.yaml logs node-c 2>&1 | grep -E "FailureDetector|FAILURE DETECTED|SUSPICION|Starting leader election" | tail -15
        fi
    elif [ "$ELECTION_DETECTED" = "true" ]; then
        error_test "Election was detected but no winner found"
        log_test "Checking logs for election activity..."
        COMPOSE_PROJECT_NAME=test-election-auto docker compose -f election.yaml logs --tail=100
        exit 1
    else
        error_test "Automatic failure detection FAILED - no election detected"
        log_test ""
        log_test "Failure detector logs (node-b):"
        COMPOSE_PROJECT_NAME=test-election-auto docker compose -f election.yaml logs node-b 2>&1 | grep -i "failuredetector\|suspicion\|failure\|status" | tail -20
        log_test ""
        log_test "Failure detector logs (node-c):"
        COMPOSE_PROJECT_NAME=test-election-auto docker compose -f election.yaml logs node-c 2>&1 | grep -i "failuredetector\|suspicion\|failure\|status" | tail -20
        log_test ""
        log_test "Full logs for debugging:"
        COMPOSE_PROJECT_NAME=test-election-auto docker compose -f election.yaml logs --tail=50
        exit 1
    fi
    
    # Copy container logs BEFORE stopping containers
    if [ -n "$LOG_DIR" ] && [ "$LOG_DIR" != "/tmp" ]; then
        log_test "Copying container logs before cleanup..."
        cd "$PROJECT_ROOT/deploy/docker/compose" 2>/dev/null || true
        if COMPOSE_PROJECT_NAME=test-election-auto docker compose -f election.yaml ps -q node-a 2>/dev/null | grep -q .; then
            COMPOSE_PROJECT_NAME=test-election-auto docker compose -f election.yaml logs --no-color node-a > "$LOG_DIR/leader-node-a.log" 2>&1 || true
            log_test "Saved node-a container log to: $LOG_DIR/leader-node-a.log"
        fi
        if COMPOSE_PROJECT_NAME=test-election-auto docker compose -f election.yaml ps -q node-b 2>/dev/null | grep -q .; then
            COMPOSE_PROJECT_NAME=test-election-auto docker compose -f election.yaml logs --no-color node-b > "$LOG_DIR/broker1-node-b.log" 2>&1 || true
            log_test "Saved node-b container log to: $LOG_DIR/broker1-node-b.log"
        fi
        if COMPOSE_PROJECT_NAME=test-election-auto docker compose -f election.yaml ps -q node-c 2>/dev/null | grep -q .; then
            COMPOSE_PROJECT_NAME=test-election-auto docker compose -f election.yaml logs --no-color node-c > "$LOG_DIR/broker2-node-c.log" 2>&1 || true
            log_test "Saved node-c container log to: $LOG_DIR/broker2-node-c.log"
        fi
    fi
    
    # Cleanup - containers will be stopped by cleanup function
    # Note: Don't call docker compose down here - let cleanup() handle it

elif [ "$MODE" = "local" ]; then
    cd "$PROJECT_ROOT"
    build_if_needed "logstream" "main.go"
    
    # Ensure netns is set up for multicast support
    log_test "Setting up network namespaces for multicast support..."
    if ! ensure_netns_setup; then
        error_test "Failed to setup network namespaces. Make sure you have sudo access."
        exit 1
    fi
    
    log_test "Starting 3-node cluster in network namespaces..."
    
    # Create log files in test-logs directory
    NODE_A_LOG="$LOG_DIR/node-a.log"
    NODE_B_LOG="$LOG_DIR/node-b.log"
    NODE_C_LOG="$LOG_DIR/node-c.log"
    touch "$NODE_A_LOG" "$NODE_B_LOG" "$NODE_C_LOG"
    chmod 666 "$NODE_A_LOG" "$NODE_B_LOG" "$NODE_C_LOG"
    
    # Start nodes in network namespaces with 172.20.0.x addresses (multicast works here!)
    # Node A (Leader) in logstream-a namespace
    sudo ip netns exec logstream-a env \
        NODE_ADDRESS=172.20.0.10:8001 \
        MULTICAST_GROUP=239.0.0.1:9999 \
        BROADCAST_PORT=8888 \
        stdbuf -oL -eL ./logstream > "$NODE_A_LOG" 2>&1 &
    NODE_A_PID=$!
    success_test "Node A (Leader) - PID $NODE_A_PID in logstream-a (172.20.0.10:8001)"
    sleep 5
    
    # Node B (Follower) in logstream-b namespace
    sudo ip netns exec logstream-b env \
        NODE_ADDRESS=172.20.0.20:8002 \
        MULTICAST_GROUP=239.0.0.1:9999 \
        BROADCAST_PORT=8888 \
        stdbuf -oL -eL ./logstream > "$NODE_B_LOG" 2>&1 &
    NODE_B_PID=$!
    success_test "Node B (Follower) - PID $NODE_B_PID in logstream-b (172.20.0.20:8002)"
    sleep 5
    
    # Node C (Follower) in logstream-c namespace
    sudo ip netns exec logstream-c env \
        NODE_ADDRESS=172.20.0.30:8003 \
        MULTICAST_GROUP=239.0.0.1:9999 \
        BROADCAST_PORT=8888 \
        stdbuf -oL -eL ./logstream > "$NODE_C_LOG" 2>&1 &
    NODE_C_PID=$!
    success_test "Node C (Follower) - PID $NODE_C_PID in logstream-c (172.20.0.30:8003)"
    
    log_test ""
    log_test "Waiting for cluster formation (30s)..."
    sleep 30
    
    log_test ""
    log_test "Cluster state before leader failure:"
    tail -10 "$NODE_B_LOG" 2>/dev/null | grep -E "Total Brokers|Registry|FailureDetector" | tail -5 || log_test "  (waiting for logs...)"
    
    log_test ""
    log_test "========================================="
    log_test "SIMULATING LEADER FAILURE"
    log_test "========================================="
    log_test "Stopping leader (node-a)..."
    # Kill process in namespace
    kill_netns_process logstream-a $NODE_A_PID 2>/dev/null || kill -9 $NODE_A_PID 2>/dev/null || true
    success_test "Leader stopped at $(date +%H:%M:%S)"
    
    log_test ""
    log_test "========================================="
    log_test "WAITING FOR AUTOMATIC FAILURE DETECTION"
    log_test "========================================="
    log_test "Timeline:"
    log_test "  ~15s - Suspicion should be raised"
    log_test "  ~20s - Failure should be detected"
    log_test "  ~25s - Election should be initiated"
    log_test "  ~30s - Election should complete"
    log_test ""
    log_test "Monitoring logs every 5 seconds..."
    
    ELECTION_DETECTED=false
    ELECTION_WINNER=""
    for i in {1..12}; do
        sleep 5
        elapsed=$((i * 5))
        log_test "[$elapsed s] Checking for election activity..."
        
        # Check for failure detection
        if [ -f "$NODE_B_LOG" ] && grep -q "LEADER FAILURE DETECTED" "$NODE_B_LOG" 2>/dev/null; then
            success_test "Failure detected on node-b at ${elapsed}s"
            ELECTION_DETECTED=true
        fi
        if [ -f "$NODE_C_LOG" ] && grep -q "LEADER FAILURE DETECTED" "$NODE_C_LOG" 2>/dev/null; then
            success_test "Failure detected on node-c at ${elapsed}s"
            ELECTION_DETECTED=true
        fi
        
        # Check for election start
        if [ -f "$NODE_B_LOG" ] && grep -q "STARTING LEADER ELECTION" "$NODE_B_LOG" 2>/dev/null; then
            success_test "Election started on node-b at ${elapsed}s"
        fi
        if [ -f "$NODE_C_LOG" ] && grep -q "STARTING LEADER ELECTION" "$NODE_C_LOG" 2>/dev/null; then
            success_test "Election started on node-c at ${elapsed}s"
        fi
        
        # Check for winner
        if [ -f "$NODE_B_LOG" ] && grep -q "Message returned to me - I WIN" "$NODE_B_LOG" 2>/dev/null; then
            success_test "Node B won the election!"
            ELECTION_WINNER="B"
            break
        fi
        if [ -f "$NODE_C_LOG" ] && grep -q "Message returned to me - I WIN" "$NODE_C_LOG" 2>/dev/null; then
            success_test "Node C won the election!"
            ELECTION_WINNER="C"
            break
        fi
    done
    
    log_test ""
    log_test "========================================="
    log_test "ELECTION RESULTS"
    log_test "========================================="
    
    if [ -n "$ELECTION_WINNER" ]; then
        success_test "[OK] Election test PASSED (Node $ELECTION_WINNER won automatically)"
        
        log_test ""
        log_test "Failure detection timeline:"
        if [ "$ELECTION_WINNER" = "B" ]; then
            grep -E "FailureDetector|FAILURE DETECTED|SUSPICION|Starting leader election" "$NODE_B_LOG" 2>/dev/null | tail -15 || log_test "  (no timeline)"
        else
            grep -E "FailureDetector|FAILURE DETECTED|SUSPICION|Starting leader election" "$NODE_C_LOG" 2>/dev/null | tail -15 || log_test "  (no timeline)"
        fi
    elif [ "$ELECTION_DETECTED" = "true" ]; then
        error_test "Election was detected but no winner found"
        log_test "Checking logs for election activity..."
        tail -100 "$NODE_B_LOG" "$NODE_C_LOG" 2>/dev/null || true
        exit 1
    else
        error_test "Automatic failure detection FAILED - no election detected"
        log_test ""
        log_test "Failure detector logs (node-b):"
        if [ -f "$NODE_B_LOG" ]; then
            grep -i "failuredetector\|suspicion\|failure\|status" "$NODE_B_LOG" 2>/dev/null | tail -20 || log_test "  No logs found"
        fi
        log_test ""
        log_test "Failure detector logs (node-c):"
        if [ -f "$NODE_C_LOG" ]; then
            grep -i "failuredetector\|suspicion\|failure\|status" "$NODE_C_LOG" 2>/dev/null | tail -20 || log_test "  No logs found"
        fi
        exit 1
    fi
    
    # Copy node log files BEFORE cleanup
    if [ -n "$LOG_DIR" ] && [ "$LOG_DIR" != "/tmp" ]; then
        log_test "Preserving node log files..."
        if [ -n "$NODE_A_LOG" ] && [ -f "$NODE_A_LOG" ]; then
            cp "$NODE_A_LOG" "$LOG_DIR/leader-node-a.log" 2>/dev/null || true
            log_test "Saved node-a log to: $LOG_DIR/leader-node-a.log"
            rm -f "$NODE_A_LOG" 2>/dev/null || true
        fi
        if [ -n "$NODE_B_LOG" ] && [ -f "$NODE_B_LOG" ]; then
            cp "$NODE_B_LOG" "$LOG_DIR/broker1-node-b.log" 2>/dev/null || true
            log_test "Saved node-b log to: $LOG_DIR/broker1-node-b.log"
            rm -f "$NODE_B_LOG" 2>/dev/null || true
        fi
        if [ -n "$NODE_C_LOG" ] && [ -f "$NODE_C_LOG" ]; then
            cp "$NODE_C_LOG" "$LOG_DIR/broker2-node-c.log" 2>/dev/null || true
            log_test "Saved node-c log to: $LOG_DIR/broker2-node-c.log"
            rm -f "$NODE_C_LOG" 2>/dev/null || true
        fi
    fi
    
    # Cleanup
    cleanup_netns_processes
    pkill -f "logstream" 2>/dev/null || true

elif [ "$MODE" = "vagrant" ]; then
    cd "$PROJECT_ROOT/deploy/vagrant"
    
    log_test "Checking Vagrant VMs..."
    # Add small random delay to prevent race conditions in parallel execution
    sleep $((RANDOM % 3))
    for vm in leader broker1 broker2; do
        if ! check_vm_status $vm; then
            error_test "$vm VM not running! Run: vagrant up"
            exit 1
        fi
    done
    
    log_test "Starting 3-node cluster..."
    log_time "Starting cluster setup..."
    
    # CRITICAL: Clean up ALL existing processes on ALL VMs before starting
    log_test "Performing global cleanup on all VMs..."
    for vm in leader broker1 broker2; do
        log_test "Cleaning up $vm..."
        # Kill all logstream processes and their entire process trees
        vagrant_ssh_retry "$vm" "
            # Get all PIDs first
            pids=\$(pgrep -f 'logstream' 2>/dev/null || true)
            if [ -n \"\$pids\" ]; then
                # Kill each process and its entire process tree
                for pid in \$pids; do
                    # Get process group ID and kill entire group
                    pgid=\$(ps -o pgid= -p \$pid 2>/dev/null | tr -d ' ' || echo '')
                    if [ -n \"\$pgid\" ]; then
                        kill -9 -\$pgid 2>/dev/null || true
                    fi
                    # Kill the process itself
                    kill -9 \$pid 2>/dev/null || true
                    # Kill all children of this process
                    pkill -9 -P \$pid 2>/dev/null || true
                done
            fi
            # Kill any wrapper scripts
            pkill -9 -f 'start-logstream' 2>/dev/null || true
            # Final pkill as fallback
            pkill -9 -f 'logstream' 2>/dev/null || true
        " 2>/dev/null || true
        # Kill processes using ports
        vagrant_ssh_retry "$vm" "
            for port in 8001 8002 8003 8888 9999; do
                if command -v fuser >/dev/null 2>&1; then
                    fuser -k \${port}/tcp 2>/dev/null || fuser -k \${port}/udp 2>/dev/null || true
                elif command -v lsof >/dev/null 2>&1; then
                    lsof -ti:\${port} 2>/dev/null | xargs -r kill -9 2>/dev/null || true
                fi
            done
        " 2>/dev/null || true
    done
    sleep 3  # Give processes time to fully terminate
    
    # Verify cleanup was successful
    log_test "Verifying cleanup was successful..."
    for vm in leader broker1 broker2; do
        remaining=$(vagrant_ssh_retry "$vm" "pgrep -f 'logstream' 2>/dev/null | wc -l" 2>/dev/null || echo "0")
        remaining=$(echo "$remaining" | tr -d '\n\r ' | grep -E '^[0-9]+$' || echo "0")
        remaining=$((remaining + 0))
        if [ "$remaining" -gt 0 ]; then
            log_test "Warning: $remaining process(es) still running on $vm after cleanup, attempting final kill..."
            vagrant_ssh_retry "$vm" "
                pids=\$(pgrep -f 'logstream' 2>/dev/null || true)
                for pid in \$pids; do
                    pgid=\$(ps -o pgid= -p \$pid 2>/dev/null | tr -d ' ' || echo '')
                    [ -n \"\$pgid\" ] && kill -9 -\$pgid 2>/dev/null || true
                    kill -9 \$pid 2>/dev/null || true
                    pkill -9 -P \$pid 2>/dev/null || true
                done
                pkill -9 -f 'logstream' 2>/dev/null || true
            " 2>/dev/null || true
        fi
    done
    sleep 2  # Final wait
    log_test "Global cleanup complete"
    log_test ""
    
    # Ensure binaries are built and up-to-date on all VMs
    if ! ensure_binary_on_vm leader; then
        error_test "Failed to ensure binary on leader VM"
        exit 1
    fi
    if ! ensure_binary_on_vm broker1; then
        error_test "Failed to ensure binary on broker1 VM"
        exit 1
    fi
    if ! ensure_binary_on_vm broker2; then
        error_test "Failed to ensure binary on broker2 VM"
        exit 1
    fi
    
    # Setup log files in synced folder
    NODE_A_LOG="$VM_LOG_DIR/node-a.log"
    NODE_B_LOG="$VM_LOG_DIR/node-b.log"
    NODE_C_LOG="$VM_LOG_DIR/node-c.log"
    
    # Ensure log directories exist on VMs
    ensure_vm_log_directory leader "$VM_LOG_DIR" || error_test "Failed to create log directory on leader"
    ensure_vm_log_directory broker1 "$VM_LOG_DIR" || error_test "Failed to create log directory on broker1"
    ensure_vm_log_directory broker2 "$VM_LOG_DIR" || error_test "Failed to create log directory on broker2"
    
    # Start leader with retry - create log file first and use stdbuf to disable buffering
    # Skip cleanup for leader since we just did global cleanup
    log_time "Starting Node A (Leader)..."
    if prepare_vm_log_file leader "$NODE_A_LOG" && \
       start_logstream_vm_wrapper leader "192.168.100.10:8001" "true" "$NODE_A_LOG" "1"; then
        success_test "Node A (Leader) started"
        log_time "Node A (Leader) started successfully"
        log_test ""
        log_test "Verifying Node A (Leader) process..."
        sleep 2
        if ! verify_process_running leader "$NODE_A_LOG" "logstream" 7; then
            error_test "Node A process verification failed"
            exit 1
        fi
        log_test ""
        # Give leader time to fully initialize and be ready to receive JOIN messages
        log_test "Waiting for leader to be ready to accept JOIN messages..."
        sleep 5
    else
        error_test "Failed to start Node A - SSH connection failed"
        exit 1
    fi
    
    # Prepare follower log files
    log_test "Preparing follower log files..."
    prepare_vm_log_file broker1 "$NODE_B_LOG"
    prepare_vm_log_file broker2 "$NODE_C_LOG"
    
    # Start both followers in parallel (they can discover the leader simultaneously)
    log_time "Starting Node B and C (Followers) in parallel..."
    
    (start_logstream_vm_wrapper broker1 "192.168.100.20:8002" "false" "$NODE_B_LOG" "1" && echo "Node B complete") &
    NODE_B_PID=$!
    
    (start_logstream_vm_wrapper broker2 "192.168.100.30:8003" "false" "$NODE_C_LOG" "1" && echo "Node C complete") &
    NODE_C_PID=$!
    
    log_test "Waiting for both followers to start..."
    
    # Wait for both followers
    STARTUP_FAILED=0
    if ! wait $NODE_B_PID; then
        error_test "Failed to start Node B - SSH connection failed"
        STARTUP_FAILED=1
    else
        success_test "Node B (Follower 1) started"
        log_time "Node B (Follower 1) started successfully"
    fi
    
    if ! wait $NODE_C_PID; then
        error_test "Failed to start Node C - SSH connection failed"
        STARTUP_FAILED=1
    else
        success_test "Node C (Follower 2) started"
        log_time "Node C (Follower 2) started successfully"
    fi
    
    # Exit if any startup failed
    if [ $STARTUP_FAILED -eq 1 ]; then
        error_test "One or more followers failed to start"
        exit 1
    fi
    
    log_test ""
    log_test "Verifying Node B (Follower) process..."
    sleep 2
    if ! verify_process_running broker1 "$NODE_B_LOG" "logstream" 7; then
        error_test "Node B process verification failed"
        exit 1
    fi
    
    log_test ""
    log_test "Verifying Node C (Follower) process..."
    sleep 2
    if ! verify_process_running broker2 "$NODE_C_LOG" "logstream" 7; then
        error_test "Node C process verification failed"
        # Try to get diagnostic information before exiting
        log_test "Attempting to gather diagnostic information for broker2..."
        log_test "Checking if log file exists on broker2 VM..."
        vagrant_ssh_retry broker2 "test -f '$NODE_C_LOG' && echo 'Log file exists' || echo 'Log file does NOT exist'" 2>/dev/null || true
        log_test "Checking log file size on broker2 VM..."
        vagrant_ssh_retry broker2 "stat -c%s '$NODE_C_LOG' 2>/dev/null || echo '0'" 2>/dev/null || true
        log_test "Checking for any logstream processes on broker2 VM..."
        vagrant_ssh_retry broker2 "ps aux | grep -E '[l]ogstream' || echo 'No logstream processes found'" 2>/dev/null || true
        log_test "Attempting to copy broker2 log for diagnostics..."
        if [ -n "$NODE_C_LOG" ]; then
            copy_vm_log_to_host broker2 "$NODE_C_LOG" "$LOG_DIR/broker2-node-c.log" 2>/dev/null || true
        fi
        error_test "Node C failed to start properly - see logs above for details"
        exit 1
    fi
    
    log_test ""
    # Give followers time to discover and join the cluster
    log_test "Waiting for both followers to discover and join cluster..."
    sleep 10
    
    # Verify broker2 actually joined as follower (not leader)
    if vagrant_ssh_retry broker2 "grep -q 'Joined existing cluster as follower\|becomeFollower' '$NODE_C_LOG' 2>/dev/null" 2>/dev/null; then
        success_test "Node C successfully joined cluster as follower"
    elif vagrant_ssh_retry broker2 "grep -q 'automatically declaring myself leader\|becomeLeader' '$NODE_C_LOG' 2>/dev/null" 2>/dev/null; then
        error_test "Node C incorrectly declared itself leader - cluster discovery failed!"
        log_test "This indicates broker2 could not discover the existing cluster."
        log_test "Checking leader and broker1 logs..."
        get_vm_log_content leader "$NODE_A_LOG" 30 || true
        get_vm_log_content broker1 "$NODE_B_LOG" 30 || true
        exit 1
    else
        error_test "Could not determine Node C's role from logs"
        log_test "Node C log excerpt:"
        get_vm_log_content broker2 "$NODE_C_LOG" 50 || true
        exit 1
    fi
    
    log_test ""
    log_time "Waiting for cluster formation (30s)..."
    sleep 30
    log_time "Cluster formation wait complete"
    
    log_test ""
    log_time "Verifying processes are running..."
    if ! vagrant_ssh_retry leader 'pgrep -f logstream > /dev/null' 2>/dev/null; then
        error_test "Leader process not running!"
        exit 1
    fi
    if ! vagrant_ssh_retry broker1 'pgrep -f logstream > /dev/null' 2>/dev/null; then
        error_test "Broker1 process not running!"
        exit 1
    fi
    if ! vagrant_ssh_retry broker2 'pgrep -f logstream > /dev/null' 2>/dev/null; then
        error_test "Broker2 process not running!"
        exit 1
    fi
    success_test "All processes are running"
    log_time "Process verification complete"
    
    log_test ""
    log_time "Checking cluster state before leader failure..."
    log_test "Cluster state before leader failure:"
    # Check for cluster state using code's own print statements
    # The leader prints "Serialized cluster state: X bytes, Y brokers..." when replicating
    # Followers print "=== Cluster State Status ===" when applying updates
    CLUSTER_STATE=""
    BROKER_COUNT=""
    
    # Method 1: Get broker count from leader's "Serialized cluster state" message (most reliable)
    BROKER_COUNT=$(vagrant_ssh_retry leader "tail -100 '$NODE_A_LOG' 2>/dev/null | grep 'Serialized cluster state' | tail -1 | grep -oE '[0-9]+ brokers' | grep -oE '[0-9]+'" 2>/dev/null || echo "")
    
    # Method 2: Get full cluster state from follower logs (more detailed)
    if [ -n "$BROKER_COUNT" ]; then
        log_test "Found broker count from leader: $BROKER_COUNT brokers"
        # Try to get full cluster state details
        CLUSTER_STATE=$(vagrant_ssh_retry broker1 "tail -200 '$NODE_B_LOG' 2>/dev/null | grep -A 10 'Cluster State Status' | head -20" 2>/dev/null || echo "")
        
        # If not found in broker1, try broker2
        if [ -z "$CLUSTER_STATE" ]; then
            CLUSTER_STATE=$(vagrant_ssh_retry broker2 "tail -200 '$NODE_C_LOG' 2>/dev/null | grep -A 10 'Cluster State Status' | head -20" 2>/dev/null || echo "")
        fi
        
        if [ -n "$CLUSTER_STATE" ]; then
            log_test "Full cluster state:"
            echo "$CLUSTER_STATE"
        else
            log_test "Cluster state summary: $BROKER_COUNT brokers (detailed state not found in follower logs)"
        fi
    else
        # Fallback: Try to get cluster state from follower logs directly
        CLUSTER_STATE=$(vagrant_ssh_retry broker1 "tail -200 '$NODE_B_LOG' 2>/dev/null | grep -A 10 'Cluster State Status' | head -20" 2>/dev/null || echo "")
        
        if [ -z "$CLUSTER_STATE" ]; then
            CLUSTER_STATE=$(vagrant_ssh_retry broker2 "tail -200 '$NODE_C_LOG' 2>/dev/null | grep -A 10 'Cluster State Status' | head -20" 2>/dev/null || echo "")
        fi
        
        if [ -z "$CLUSTER_STATE" ]; then
            log_test "Warning: Could not extract cluster state from logs (this is informational only)"
            log_test "All processes are running, continuing with leader failure test..."
            log_test "Last few lines from leader log for reference:"
            get_vm_log_content leader "$NODE_A_LOG" 10 || true
        else
            log_test "Found cluster state:"
            echo "$CLUSTER_STATE"
        fi
    fi
    log_time "Cluster state verification complete"
    
    log_test ""
    log_test "========================================="
    log_test "SETUP PHASE COMPLETE"
    log_test "========================================="
    log_test ""
    
    # NOW start the timeout - setup is done, only test logic will be timed
    start_test_timeout
    
    log_test ""
    log_test "========================================="
    log_test "SIMULATING LEADER FAILURE"
    log_test "========================================="
    log_time "Starting leader failure simulation..."

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

    log_test "Stopping $ACTUAL_LEADER_VM (actual leader)..."

    # Try to halt the VM first (most realistic failure scenario)
    # This simulates a complete machine crash/network partition
    log_test "Attempting to halt $ACTUAL_LEADER_VM VM (simulating machine failure)..."
    if (cd "$PROJECT_ROOT/deploy/vagrant" && vagrant halt "$ACTUAL_LEADER_VM" 2>&1); then
        success_test "$ACTUAL_LEADER_VM VM halted successfully at $(date +%H:%M:%S)"
        log_test "VM halt simulates complete machine failure - followers should detect this"
        sleep 3  # Give network time to propagate the failure
    else
        # Fallback: Kill the process if VM halt fails
        log_test "VM halt failed, falling back to process kill..."
        log_test "Leader process found, killing..."

        # First, verify the process is running
        if ! vagrant_ssh_retry "$ACTUAL_LEADER_VM" 'pgrep -f logstream > /dev/null' 2>/dev/null; then
            error_test "Leader process not found - may have already stopped"
        else
            # Kill the process - try multiple methods
            vagrant_ssh_retry "$ACTUAL_LEADER_VM" 'pkill -9 -f logstream' 2>&1 || true
            sleep 2

            # Verify the process is actually dead
            if vagrant_ssh_retry "$ACTUAL_LEADER_VM" 'pgrep -f logstream > /dev/null' 2>/dev/null; then
                error_test "Leader process still running after kill attempt!"
                log_test "Trying more aggressive kill..."
                vagrant_ssh_retry "$ACTUAL_LEADER_VM" 'pkill -9 -f "logstream|./logstream"' 2>&1 || true
                sleep 2

                # Check again
                if vagrant_ssh_retry "$ACTUAL_LEADER_VM" 'pgrep -f logstream > /dev/null' 2>/dev/null; then
                    error_test "CRITICAL: Leader process cannot be killed!"
                    log_test "Process details:"
                    vagrant_ssh_retry "$ACTUAL_LEADER_VM" 'ps aux | grep -E "[l]ogstream"' 2>&1 || true
                    exit 1
                fi
            fi
            success_test "Leader process killed at $(date +%H:%M:%S)"
        fi
    fi
    
    log_test ""
    log_test "========================================="
    log_test "WAITING FOR AUTOMATIC FAILURE DETECTION"
    log_test "========================================="
    log_test "Timeline:"
    log_test "  ~15s - Suspicion should be raised"
    log_test "  ~20s - Failure should be detected"
    log_test "  ~25s - Election should be initiated"
    log_test "  ~30s - Election should complete"
    log_test ""
    log_test "Monitoring logs every 5 seconds..."
    
    ELECTION_DETECTED=false
    ELECTION_WINNER=""
    for i in {1..12}; do
        sleep 5
        elapsed=$((i * 5))
        log_time "[Monitoring: ${elapsed}s since leader kill] Checking for election activity..."
        
        # Check for failure detection
        if vagrant_ssh_retry broker1 "grep -q 'LEADER FAILURE DETECTED' '$NODE_B_LOG' 2>/dev/null" 2>/dev/null; then
            success_test "Failure detected on node-b at ${elapsed}s"
            ELECTION_DETECTED=true
        fi
        if vagrant_ssh_retry broker2 "grep -q 'LEADER FAILURE DETECTED' '$NODE_C_LOG' 2>/dev/null" 2>/dev/null; then
            success_test "Failure detected on node-c at ${elapsed}s"
            ELECTION_DETECTED=true
        fi
        
        # Check for election start
        if vagrant_ssh_retry broker1 "grep -q 'STARTING LEADER ELECTION' '$NODE_B_LOG' 2>/dev/null" 2>/dev/null; then
            success_test "Election started on node-b at ${elapsed}s"
        fi
        if vagrant_ssh_retry broker2 "grep -q 'STARTING LEADER ELECTION' '$NODE_C_LOG' 2>/dev/null" 2>/dev/null; then
            success_test "Election started on node-c at ${elapsed}s"
        fi
        
        # Check for winner
        if vagrant_ssh_retry broker1 "grep -q 'Message returned to me - I WIN' '$NODE_B_LOG' 2>/dev/null" 2>/dev/null; then
            success_test "Node B won the election!"
            ELECTION_WINNER="B"
            log_time "Election completed - Node B won"
            break
        fi
        if vagrant_ssh_retry broker2 "grep -q 'Message returned to me - I WIN' '$NODE_C_LOG' 2>/dev/null" 2>/dev/null; then
            success_test "Node C won the election!"
            ELECTION_WINNER="C"
            log_time "Election completed - Node C won"
            break
        fi
    done
    log_time "Election monitoring loop complete (checked for 60s)"
    
    log_test ""
    log_test "========================================="
    log_test "ELECTION RESULTS"
    log_test "========================================="
    
    if [ -n "$ELECTION_WINNER" ]; then
        success_test "[OK] Election test PASSED (Node $ELECTION_WINNER won automatically)"
        
        log_test ""
        log_test "Failure detection timeline:"
        if [ "$ELECTION_WINNER" = "B" ]; then
            vagrant_ssh_retry broker1 "grep -E 'FailureDetector|FAILURE DETECTED|SUSPICION|Starting leader election' '$NODE_B_LOG' 2>/dev/null" 2>/dev/null | tail -15 || log_test "  (no timeline)"
        else
            vagrant_ssh_retry broker2 "grep -E 'FailureDetector|FAILURE DETECTED|SUSPICION|Starting leader election' '$NODE_C_LOG' 2>/dev/null" 2>/dev/null | tail -15 || log_test "  (no timeline)"
        fi
        
        log_test ""
        log_test "Election progression:"
        if [ "$ELECTION_WINNER" = "B" ]; then
            vagrant_ssh_retry broker1 "grep -E 'ELECTION|WIN|VICTORY' '$NODE_B_LOG' 2>/dev/null" 2>/dev/null | tail -10 || log_test "  (no progression)"
        else
            vagrant_ssh_retry broker2 "grep -E 'ELECTION|WIN|VICTORY' '$NODE_C_LOG' 2>/dev/null" 2>/dev/null | tail -10 || log_test "  (no progression)"
        fi
        
        log_test ""
        log_test "Full logs available at:"
        log_test "  Node A: $LOG_DIR/leader-node-a.log"
        log_test "  Node B: $LOG_DIR/broker1-node-b.log"
        log_test "  Node C: $LOG_DIR/broker2-node-c.log"
    elif [ "$ELECTION_DETECTED" = "true" ]; then
        error_test "Election was detected but no winner found"
        log_test "Checking logs for election activity..."
        get_vm_log_content broker1 "$NODE_B_LOG" 100 || log_test "  No logs from node B"
        get_vm_log_content broker2 "$NODE_C_LOG" 100 || log_test "  No logs from node C"
        exit 1
    else
        error_test "Automatic failure detection FAILED - no election detected"
        log_test ""
        log_test "Failure detector logs (node-b):"
        vagrant_ssh_retry broker1 "grep -i 'failuredetector\|suspicion\|failure\|status' '$NODE_B_LOG' 2>/dev/null" 2>/dev/null | tail -20 || log_test "  No logs found"
        log_test ""
        log_test "Failure detector logs (node-c):"
        vagrant_ssh_retry broker2 "grep -i 'failuredetector\|suspicion\|failure\|status' '$NODE_C_LOG' 2>/dev/null" 2>/dev/null | tail -20 || log_test "  No logs found"
        log_test ""
        log_test "Full logs available at:"
        log_test "  Node A: $LOG_DIR/leader-node-a.log"
        log_test "  Node B: $LOG_DIR/broker1-node-b.log"
        log_test "  Node C: $LOG_DIR/broker2-node-c.log"
        exit 1
    fi
    
    # Note: Log copying is now done in cleanup() function to ensure it happens even on timeout

else
    error_test "Invalid mode: $MODE (supported: docker, local, vagrant)"
    exit 1
fi

# Cancel timeout since we completed successfully
if [ -n "$TIMEOUT_PID" ]; then
    kill $TIMEOUT_PID 2>/dev/null || true
    wait $TIMEOUT_PID 2>/dev/null || true  # Wait for timeout process to fully exit
fi
# Remove timeout marker if it was created (race condition fix)
rm -f "$TIMEOUT_FLAG" 2>/dev/null || true
success_test "Test completed successfully"

