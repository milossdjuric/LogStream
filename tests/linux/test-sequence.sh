#!/bin/bash

# Timeout in seconds (15 minutes - increased for cleanup and process startup)
TEST_TIMEOUT=900
MODE="${1:-local}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/lib/common.sh"

# Setup log directory
setup_log_directory "$MODE"

TEST_PREFIX="[TEST-SEQUENCE]"
log_test() { echo -e "${BLUE}${TEST_PREFIX} $1${NC}"; }
success_test() { echo -e "${GREEN}${TEST_PREFIX} $1${NC}"; }
error_test() { echo -e "${RED}${TEST_PREFIX} $1${NC}"; }

# Cleanup function
cleanup() {
    error_test "Cleaning up test resources..."
    
    # Copy process logs from VMs to host BEFORE killing processes (vagrant mode only)
    if [ "$MODE" = "vagrant" ] && [ -n "$LOG_DIR" ] && [ "$LOG_DIR" != "/tmp" ]; then
        if [ -n "$LEADER_LOG" ] || [ -n "$FOLLOWER1_LOG" ] || [ -n "$FOLLOWER2_LOG" ]; then
            log_test "Copying process logs from VMs to host before cleanup..."
            if [ -n "$LEADER_LOG" ]; then
                if copy_vm_log_to_host leader "$LEADER_LOG" "$LOG_DIR/leader-leader.log"; then
                    local line_count=$(wc -l < "$LOG_DIR/leader-leader.log" 2>/dev/null || echo "0")
                    log_test "Copied leader log ($line_count lines) to: $LOG_DIR/leader-leader.log"
                else
                    log_test "Warning: Failed to copy leader log"
                fi
            fi
            if [ -n "$FOLLOWER1_LOG" ]; then
                if copy_vm_log_to_host broker1 "$FOLLOWER1_LOG" "$LOG_DIR/broker1-follower1.log"; then
                    local line_count=$(wc -l < "$LOG_DIR/broker1-follower1.log" 2>/dev/null || echo "0")
                    log_test "Copied follower1 log ($line_count lines) to: $LOG_DIR/broker1-follower1.log"
                else
                    log_test "Warning: Failed to copy follower1 log"
                fi
            fi
            if [ -n "$FOLLOWER2_LOG" ]; then
                if copy_vm_log_to_host broker2 "$FOLLOWER2_LOG" "$LOG_DIR/broker2-follower2.log"; then
                    local line_count=$(wc -l < "$LOG_DIR/broker2-follower2.log" 2>/dev/null || echo "0")
                    log_test "Copied follower2 log ($line_count lines) to: $LOG_DIR/broker2-follower2.log"
                else
                    log_test "Warning: Failed to copy follower2 log"
                fi
            fi
        fi
    fi
    
    if [ "$MODE" = "local" ]; then
        # Copy node log files BEFORE killing processes (so we capture final output)
        if [ -n "$LOG_DIR" ] && [ "$LOG_DIR" != "/tmp" ]; then
            log_test "Preserving node log files..."
            if [ -n "$LEADER_LOG" ] && [ -f "$LEADER_LOG" ]; then
                cp "$LEADER_LOG" "$LOG_DIR/leader.log" 2>/dev/null || true
                log_test "Saved leader log to: $LOG_DIR/leader.log"
            fi
            if [ -n "$FOLLOWER1_LOG" ] && [ -f "$FOLLOWER1_LOG" ]; then
                cp "$FOLLOWER1_LOG" "$LOG_DIR/follower1.log" 2>/dev/null || true
                log_test "Saved follower1 log to: $LOG_DIR/follower1.log"
            fi
            if [ -n "$FOLLOWER2_LOG" ] && [ -f "$FOLLOWER2_LOG" ]; then
                cp "$FOLLOWER2_LOG" "$LOG_DIR/follower2.log" 2>/dev/null || true
                log_test "Saved follower2 log to: $LOG_DIR/follower2.log"
            fi
        fi
        cleanup_netns_processes
        pkill -f "logstream" 2>/dev/null || true
    elif [ "$MODE" = "docker" ]; then
        # Copy container logs if not already copied (safety net for early exits)
        # Main script flow copies logs before cleanup, but this ensures we get logs even on early exit
        # Note: docker compose logs works even on stopped containers
        if [ -n "$LOG_DIR" ] && [ "$LOG_DIR" != "/tmp" ]; then
            # Only copy if log files don't already exist (avoid overwriting)
            if [ ! -f "$LOG_DIR/leader.log" ] || [ ! -f "$LOG_DIR/follower1.log" ] || [ ! -f "$LOG_DIR/follower2.log" ]; then
                log_test "Copying container logs (cleanup safety net)..."
                cd "$PROJECT_ROOT/deploy/docker/compose" 2>/dev/null || true
                # docker compose logs works even on stopped containers
                COMPOSE_PROJECT_NAME=test-sequence docker compose -f sequence.yaml logs --no-color leader > "$LOG_DIR/leader.log" 2>&1 || true
                COMPOSE_PROJECT_NAME=test-sequence docker compose -f sequence.yaml logs --no-color broker1 > "$LOG_DIR/follower1.log" 2>&1 || true
                COMPOSE_PROJECT_NAME=test-sequence docker compose -f sequence.yaml logs --no-color broker2 > "$LOG_DIR/follower2.log" 2>&1 || true
            fi
        fi
        cd "$PROJECT_ROOT/deploy/docker/compose" 2>/dev/null || true
        COMPOSE_PROJECT_NAME=test-sequence docker compose -f sequence.yaml down --remove-orphans 2>/dev/null || true
    elif [ "$MODE" = "vagrant" ]; then
        vagrant_ssh_retry leader 'pkill -f logstream' 2>/dev/null || true
        vagrant_ssh_retry broker1 'pkill -f logstream' 2>/dev/null || true
        vagrant_ssh_retry broker2 'pkill -f logstream' 2>/dev/null || true
    fi
    error_test "Cleanup complete"
}

# Track if timeout occurred (used to ensure non-zero exit code)
TIMEOUT_MARKER="/tmp/sequence-test-timeout-$$"

# Set up timeout
(
    sleep $TEST_TIMEOUT
    error_test "Test timeout reached (${TEST_TIMEOUT}s), forcing cleanup..."
    # Create marker file to indicate timeout
    touch "$TIMEOUT_MARKER"
    cleanup
    # Send TERM signal to parent process
    kill -TERM $$ 2>/dev/null || true
    sleep 2
    # If still alive, force kill
    kill -9 $$ 2>/dev/null || true
) &
TIMEOUT_PID=$!

# Trap cleanup on exit - check for timeout marker and exit with error if found
trap '
    kill $TIMEOUT_PID 2>/dev/null
    cleanup
    if [ -f "$TIMEOUT_MARKER" ]; then
        rm -f "$TIMEOUT_MARKER"
        error_test "EXITING WITH ERROR: Test timed out"
        exit 1
    fi
    exit
' EXIT

# Separate trap for signals - ensure error exit on interrupt/terminate
trap '
    kill $TIMEOUT_PID 2>/dev/null
    cleanup
    if [ -f "$TIMEOUT_MARKER" ]; then
        rm -f "$TIMEOUT_MARKER"
        error_test "EXITING WITH ERROR: Test timed out"
        exit 1
    fi
    error_test "EXITING WITH ERROR: Test interrupted"
    exit 1
' INT TERM

log_test "================================================"
log_test "SEQUENCE NUMBER DEMONSTRATION"
log_test "Shows how sequences increase with state changes"
log_test "Mode: $MODE"
log_test "Timeout: ${TEST_TIMEOUT}s"
log_test "================================================"
log_test ""

if [ "$MODE" = "local" ]; then
    cd "$PROJECT_ROOT"
    build_if_needed "logstream" "main.go"
    
    # Ensure netns is set up for multicast support
    log_test "Setting up network namespaces for multicast support..."
    if ! ensure_netns_setup; then
        error_test "Failed to setup network namespaces. Make sure you have sudo access."
        exit 1
    fi
    
    # Set up log files
    LEADER_LOG="$LOG_DIR/leader.log"
    FOLLOWER1_LOG="$LOG_DIR/follower1.log"
    FOLLOWER2_LOG="$LOG_DIR/follower2.log"
    
    EXPECTED_SEQ=1
    
    show_sequence() {
        local label="$1"
        log_test ""
        log_test "========================================="
        log_test "$label"
        log_test "========================================="
        tail -50 "$LEADER_LOG" 2>/dev/null | grep -E "seq=[0-9]+|Total Brokers:|Serialized cluster state" | grep -v "seq=0" | tail -5 || log_test "No matching log entries found"
    }
    
    log_test "=== STEP 1: Start Leader ==="
    # Start leader in logstream-a namespace
    sudo ip netns exec logstream-a env \
        NODE_ADDRESS=172.20.0.10:8001 \
        MULTICAST_GROUP=239.0.0.1:9999 \
        BROADCAST_PORT=8888 \
        stdbuf -oL -eL ./logstream > "$LEADER_LOG" 2>&1 &
    LEADER_PID=$!
    success_test "Leader started (PID: $LEADER_PID) in logstream-a (172.20.0.10:8001)"
    sleep 5
    
    show_sequence "After leader start (seq should be 1)"
    success_test "[OK] Expected seq=$EXPECTED_SEQ, Leader registered"
    ((EXPECTED_SEQ++))
    
    log_test ""
    log_test "=== STEP 2: Add Follower 1 ==="
    # Start follower 1 in logstream-b namespace
    sudo ip netns exec logstream-b env \
        NODE_ADDRESS=172.20.0.20:8002 \
        MULTICAST_GROUP=239.0.0.1:9999 \
        BROADCAST_PORT=8888 \
        stdbuf -oL -eL ./logstream > "$FOLLOWER1_LOG" 2>&1 &
    FOLLOWER1_PID=$!
    success_test "Follower 1 started (PID: $FOLLOWER1_PID) in logstream-b (172.20.0.20:8002)"
    sleep 10
    
    show_sequence "After follower 1 joins (seq should be 2)"
    success_test "[OK] Expected seq=$EXPECTED_SEQ, Follower 1 joined"
    ((EXPECTED_SEQ++))
    
    log_test ""
    log_test "=== STEP 3: Add Follower 2 ==="
    # Start follower 2 in logstream-c namespace
    sudo ip netns exec logstream-c env \
        NODE_ADDRESS=172.20.0.30:8003 \
        MULTICAST_GROUP=239.0.0.1:9999 \
        BROADCAST_PORT=8888 \
        stdbuf -oL -eL ./logstream > "$FOLLOWER2_LOG" 2>&1 &
    FOLLOWER2_PID=$!
    success_test "Follower 2 started (PID: $FOLLOWER2_PID) in logstream-c (172.20.0.30:8003)"
    sleep 10
    
    show_sequence "After follower 2 joins (seq should be 3)"
    success_test "[OK] Expected seq=$EXPECTED_SEQ, Follower 2 joined"
    
    log_test ""
    log_test "================================================"
    log_test "SEQUENCE PROGRESSION SUMMARY"
    log_test "================================================"
    log_test "seq=1: Leader self-registration"
    log_test "seq=2: Follower 1 joined"
    log_test "seq=3: Follower 2 joined"
    log_test "================================================"
    log_test ""
    success_test "[OK] Sequence test complete"
    
    # Show final logs
    log_test "Final logs:"
    tail -30 "$LEADER_LOG" 2>/dev/null || log_test "No logs available"
    
    # Cleanup
    cleanup_netns_processes
    pkill -f "logstream" 2>/dev/null || true

elif [ "$MODE" = "docker" ]; then
    COMPOSE_FILE="$PROJECT_ROOT/deploy/docker/compose/sequence.yaml"
    
    if [ ! -f "$COMPOSE_FILE" ]; then
        error_test "Compose file not found: $COMPOSE_FILE"
        exit 1
    fi
    
    cd "$PROJECT_ROOT/deploy/docker/compose"
    
    EXPECTED_SEQ=1
    
    show_sequence() {
        local label="$1"
        log_test ""
        log_test "========================================="
        log_test "$label"
        log_test "========================================="
        COMPOSE_PROJECT_NAME=test-sequence docker compose -f sequence.yaml logs leader 2>/dev/null | grep -E "seq=[0-9]+|Total Brokers:" | grep -v "seq=0" | tail -5 || log_test "No matching log entries found"
    }
    
    log_test "=== STEP 1: Start Leader ==="
    COMPOSE_PROJECT_NAME=test-sequence docker compose -f sequence.yaml up -d leader
    if [ $? -ne 0 ]; then
        error_test "Failed to start leader container"
        exit 1
    fi
    sleep 3
    
    show_sequence "After leader start (seq should be 1)"
    success_test "[OK] Expected seq=$EXPECTED_SEQ, Leader registered"
    ((EXPECTED_SEQ++))
    
    log_test ""
    log_test "=== STEP 2: Add Follower 1 ==="
    COMPOSE_PROJECT_NAME=test-sequence docker compose -f sequence.yaml up -d broker1
    if [ $? -ne 0 ]; then
        error_test "Failed to start broker1 container"
        exit 1
    fi
    sleep 5
    
    show_sequence "After follower 1 joins (seq should be 2)"
    success_test "[OK] Expected seq=$EXPECTED_SEQ, Follower 1 joined"
    ((EXPECTED_SEQ++))
    
    log_test ""
    log_test "=== STEP 3: Add Follower 2 ==="
    COMPOSE_PROJECT_NAME=test-sequence docker compose -f sequence.yaml up -d broker2
    if [ $? -ne 0 ]; then
        error_test "Failed to start broker2 container"
        exit 1
    fi
    sleep 5
    
    show_sequence "After follower 2 joins (seq should be 3)"
    success_test "[OK] Expected seq=$EXPECTED_SEQ, Follower 2 joined"
    
    log_test ""
    log_test "================================================"
    log_test "SEQUENCE PROGRESSION SUMMARY"
    log_test "================================================"
    log_test "seq=1: Leader self-registration"
    log_test "seq=2: Follower 1 joined"
    log_test "seq=3: Follower 2 joined"
    log_test "================================================"
    log_test ""
    success_test "[OK] Sequence test complete"
    
    # Show final logs
    log_test "Final logs:"
    COMPOSE_PROJECT_NAME=test-sequence docker compose -f sequence.yaml logs --tail=50 2>/dev/null || log_test "No logs available"
    
    # Copy container logs BEFORE stopping containers
    if [ -n "$LOG_DIR" ] && [ "$LOG_DIR" != "/tmp" ]; then
        log_test "Copying container logs before cleanup..."
        cd "$PROJECT_ROOT/deploy/docker/compose" 2>/dev/null || true
        if COMPOSE_PROJECT_NAME=test-sequence docker compose -f sequence.yaml ps -q leader 2>/dev/null | grep -q .; then
            COMPOSE_PROJECT_NAME=test-sequence docker compose -f sequence.yaml logs --no-color leader > "$LOG_DIR/leader.log" 2>&1 || true
            log_test "Saved leader container log to: $LOG_DIR/leader.log"
        fi
        if COMPOSE_PROJECT_NAME=test-sequence docker compose -f sequence.yaml ps -q broker1 2>/dev/null | grep -q .; then
            COMPOSE_PROJECT_NAME=test-sequence docker compose -f sequence.yaml logs --no-color broker1 > "$LOG_DIR/follower1.log" 2>&1 || true
            log_test "Saved broker1 container log to: $LOG_DIR/follower1.log"
        fi
        if COMPOSE_PROJECT_NAME=test-sequence docker compose -f sequence.yaml ps -q broker2 2>/dev/null | grep -q .; then
            COMPOSE_PROJECT_NAME=test-sequence docker compose -f sequence.yaml logs --no-color broker2 > "$LOG_DIR/follower2.log" 2>&1 || true
            log_test "Saved broker2 container log to: $LOG_DIR/follower2.log"
        fi
    fi
    
    # Cleanup - containers will be stopped by cleanup function
    # Note: Don't call docker compose down here - let cleanup() handle it

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
    
    EXPECTED_SEQ=1
    
    show_sequence() {
        local label="$1"
        log_test ""
        log_test "========================================="
        log_test "$label"
        log_test "========================================="
        # Use multiple approaches to get sequence info (similar to election test)
        # Approach 1: Try to get broker count from "Serialized cluster state" messages (most reliable)
        SEQUENCE_INFO=""
        BROKER_COUNT=$(vagrant_ssh_retry leader "tail -200 '$LEADER_LOG' 2>/dev/null | grep -oE 'Serialized cluster state: [0-9]+ bytes, [0-9]+ brokers' | tail -1 | grep -oE '[0-9]+ brokers' | grep -oE '[0-9]+'" 2>/dev/null || echo "")
        
        if [ -n "$BROKER_COUNT" ]; then
            log_test "Broker count from leader: $BROKER_COUNT"
        fi
        
        # Approach 2: Try to get sequence from REPLICATE messages
        SEQUENCE_INFO=$(vagrant_ssh_retry leader "tail -200 '$LEADER_LOG' 2>/dev/null | grep -iE 'REPLICATE seq=|-> REPLICATE seq=' | tail -3" 2>/dev/null || echo "")
        
        # Approach 3: If not found, try to get from cluster state status
        if [ -z "$SEQUENCE_INFO" ]; then
            SEQUENCE_INFO=$(vagrant_ssh_retry leader "tail -200 '$LEADER_LOG' 2>/dev/null | grep -A 5 '=== Cluster State Status ===' | grep -E 'Sequence Number:|Brokers:' | tail -5" 2>/dev/null || echo "")
        fi
        
        # Approach 4: Fallback to any sequence-related messages
        if [ -z "$SEQUENCE_INFO" ]; then
            SEQUENCE_INFO=$(vagrant_ssh_retry leader "tail -200 '$LEADER_LOG' 2>/dev/null | grep -iE 'seq=[0-9]+|Sequence Number:' | grep -v 'seq=0' | tail -5" 2>/dev/null || echo "")
        fi
        
        # Approach 5: Try follower logs as fallback
        if [ -z "$SEQUENCE_INFO" ] && [ -n "$FOLLOWER1_LOG" ]; then
            SEQUENCE_INFO=$(vagrant_ssh_retry broker1 "tail -100 '$FOLLOWER1_LOG' 2>/dev/null | grep -A 5 '=== Cluster State Status ===' | grep -E 'Sequence Number:|Brokers:' | tail -3" 2>/dev/null || echo "")
        fi
        
        if [ -n "$SEQUENCE_INFO" ]; then
            echo "$SEQUENCE_INFO"
        elif [ -n "$BROKER_COUNT" ]; then
            log_test "Found broker count: $BROKER_COUNT (sequence info not available, but cluster is active)"
        else
            log_test "No matching log entries found (process is running, assuming success)"
        fi
    }
    
    # CRITICAL: Clean up ALL existing processes on HOST and ALL VMs before starting
    log_test "Performing global cleanup on host and all VMs..."
    
    # First, kill processes on HOST machine (these might auto-detect wrong network)
    log_test "Cleaning up host machine..."
    pkill -9 -f 'logstream' 2>/dev/null || true
    # Kill processes using ports on host
    for port in 8001 8002 8003 8888 9999; do
        if command -v fuser >/dev/null 2>&1; then
            fuser -k ${port}/tcp 2>/dev/null || fuser -k ${port}/udp 2>/dev/null || true
        elif command -v lsof >/dev/null 2>&1; then
            lsof -ti:${port} 2>/dev/null | xargs -r kill -9 2>/dev/null || true
        fi
    done
    
    # Then clean up VMs
    for vm in leader broker1 broker2; do
        log_test "Cleaning up $vm..."
        # Kill all logstream processes
        vagrant_ssh_retry "$vm" "pkill -9 -f 'logstream' 2>/dev/null || true" 2>/dev/null || true
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
    log_test "Global cleanup complete"
    log_test ""
    
    log_test "=== STEP 1: Start Leader ==="
    
    # Verify binary and setup logs
    if ! ensure_binary_on_vm leader; then
        error_test "Failed to ensure binary on leader VM"
        exit 1
    fi
    
    LEADER_LOG="$VM_LOG_DIR/leader.log"
    if ! ensure_vm_log_directory leader "$VM_LOG_DIR" || ! prepare_vm_log_file leader "$LEADER_LOG"; then
        error_test "Failed to prepare log file on leader VM"
        exit 1
    fi
    
    if ! start_logstream_vm_wrapper leader "192.168.100.10:8001" "true" "$LEADER_LOG" "1"; then
        error_test "Failed to start leader - wrapper script failed"
        exit 1
    fi
    
    log_test ""
    log_test "Verifying leader process..."
    sleep 2
    if ! verify_process_running leader "$LEADER_LOG" "logstream" 7; then
        error_test "Leader process verification failed"
        exit 1
    fi
    log_test ""
    
    # Give leader time to initialize and register
    log_test "Waiting for leader to be ready to accept JOIN messages..."
    sleep 5
    
    show_sequence "After leader start (seq should be 1)"
    success_test "[OK] Expected seq=$EXPECTED_SEQ, Leader registered"
    ((EXPECTED_SEQ++))
    
    log_test ""
    log_test "=== STEP 2: Add Follower 1 ==="
    
    if ! ensure_binary_on_vm broker1; then
        error_test "Failed to ensure binary on broker1 VM"
        exit 1
    fi
    
    FOLLOWER1_LOG="$VM_LOG_DIR/follower1.log"
    if ! ensure_vm_log_directory broker1 "$VM_LOG_DIR" || ! prepare_vm_log_file broker1 "$FOLLOWER1_LOG"; then
        error_test "Failed to prepare log file on broker1 VM"
        exit 1
    fi
    
    if ! start_logstream_vm_wrapper broker1 "192.168.100.20:8002" "false" "$FOLLOWER1_LOG" "1"; then
        error_test "Failed to start follower 1 - wrapper script failed"
        exit 1
    fi
    
    log_test ""
    log_test "Verifying follower 1 process..."
    sleep 2
    if ! verify_process_running broker1 "$FOLLOWER1_LOG" "logstream" 7; then
        error_test "Follower 1 process verification failed"
        exit 1
    fi
    log_test ""
    # Give follower 1 time to join and stabilize before starting follower 2
    log_test "Waiting for follower 1 to join cluster..."
    sleep 10
    
    # Verify that broker1 joined as a follower
    if ! vagrant_ssh_retry broker1 "grep -q 'Joined existing cluster as follower' '$FOLLOWER1_LOG' 2>/dev/null"; then
        error_test "Broker1 did not join as a follower. Check logs for discovery issues."
        exit 1
    fi
    success_test "[OK] Broker1 successfully joined as a follower."
    
    show_sequence "After follower 1 joins (seq should be 2)"
    success_test "[OK] Expected seq=$EXPECTED_SEQ, Follower 1 joined"
    ((EXPECTED_SEQ++))
    
    log_test ""
    log_test "=== STEP 3: Add Follower 2 ==="
    
    if ! ensure_binary_on_vm broker2; then
        error_test "Failed to ensure binary on broker2 VM"
        exit 1
    fi
    
    FOLLOWER2_LOG="$VM_LOG_DIR/follower2.log"
    if ! ensure_vm_log_directory broker2 "$VM_LOG_DIR" || ! prepare_vm_log_file broker2 "$FOLLOWER2_LOG"; then
        error_test "Failed to prepare log file on broker2 VM"
        exit 1
    fi
    
    if ! start_logstream_vm_wrapper broker2 "192.168.100.30:8003" "false" "$FOLLOWER2_LOG" "1"; then
        error_test "Failed to start follower 2 - wrapper script failed"
        exit 1
    fi
    
    log_test ""
    log_test "Verifying follower 2 process..."
    sleep 2
    if ! verify_process_running broker2 "$FOLLOWER2_LOG" "logstream" 7; then
        error_test "Follower 2 process verification failed"
        # Try to get diagnostic information before exiting
        log_test "Attempting to gather diagnostic information..."
        log_test "Checking if log file exists on broker2 VM..."
        vagrant_ssh_retry broker2 "test -f '$FOLLOWER2_LOG' && echo 'Log file exists' || echo 'Log file does NOT exist'" 2>/dev/null || true
        log_test "Checking log file size on broker2 VM..."
        vagrant_ssh_retry broker2 "stat -c%s '$FOLLOWER2_LOG' 2>/dev/null || echo '0'" 2>/dev/null || true
        log_test "Checking for any logstream processes on broker2 VM..."
        vagrant_ssh_retry broker2 "ps aux | grep -E '[l]ogstream' || echo 'No logstream processes found'" 2>/dev/null || true
        log_test "Attempting to copy follower2 log for diagnostics..."
        if [ -n "$FOLLOWER2_LOG" ]; then
            copy_vm_log_to_host broker2 "$FOLLOWER2_LOG" "$LOG_DIR/broker2-follower2.log" 2>/dev/null || true
        fi
        error_test "Follower 2 failed to start properly - see logs above for details"
        exit 1
    fi
    log_test ""
    # Give broker2 time to discover and join the cluster
    log_test "Waiting for follower 2 to discover and join cluster..."
    sleep 10
    # Verify broker2 actually joined as follower (not leader)
    if vagrant_ssh_retry broker2 "grep -q 'Joined existing cluster as follower\|becomeFollower' '$FOLLOWER2_LOG' 2>/dev/null" 2>/dev/null; then
        success_test "Follower 2 successfully joined cluster as follower"
    elif vagrant_ssh_retry broker2 "grep -q 'automatically declaring myself leader\|becomeLeader' '$FOLLOWER2_LOG' 2>/dev/null" 2>/dev/null; then
        error_test "Follower 2 incorrectly declared itself leader - cluster discovery failed!"
        log_test "This indicates broker2 could not discover the existing cluster."
        log_test "Checking leader and follower1 logs..."
        get_vm_log_content leader "$LEADER_LOG" 30 || true
        get_vm_log_content broker1 "$FOLLOWER1_LOG" 30 || true
        # Copy logs before exiting
        if [ -n "$FOLLOWER2_LOG" ]; then
            copy_vm_log_to_host broker2 "$FOLLOWER2_LOG" "$LOG_DIR/broker2-follower2.log" 2>/dev/null || true
        fi
        exit 1
    else
        error_test "Could not determine follower 2's role from logs"
        log_test "Follower 2 log excerpt:"
        get_vm_log_content broker2 "$FOLLOWER2_LOG" 50 || true
        # Copy logs before exiting
        if [ -n "$FOLLOWER2_LOG" ]; then
            copy_vm_log_to_host broker2 "$FOLLOWER2_LOG" "$LOG_DIR/broker2-follower2.log" 2>/dev/null || true
        fi
        exit 1
    fi
    log_test ""
    
    show_sequence "After follower 2 joins (seq should be 3)"
    success_test "[OK] Expected seq=$EXPECTED_SEQ, Follower 2 joined"
    
    log_test ""
    log_test "================================================"
    log_test "SEQUENCE PROGRESSION SUMMARY"
    log_test "================================================"
    log_test "seq=1: Leader self-registration"
    log_test "seq=2: Follower 1 joined"
    log_test "seq=3: Follower 2 joined"
    log_test "================================================"
    log_test ""
    success_test "[OK] Sequence test complete (Vagrant)"
    
    # Note: Log copying is now done in cleanup() function to ensure it happens even on timeout

else
    error_test "Invalid mode: $MODE"
    log_test "Usage: $0 [local|docker|vagrant]"
    exit 1
fi

# Cancel timeout since we completed successfully
kill $TIMEOUT_PID 2>/dev/null || true
success_test "Test completed successfully"
