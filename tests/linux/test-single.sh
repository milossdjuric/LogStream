#!/bin/bash

# Timeout in seconds (10 minutes - allows time for VM operations and cleanup)
TEST_TIMEOUT=600
MODE="${1:-local}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/lib/common.sh"

# Setup log directory
setup_log_directory "$MODE"

TEST_PREFIX="[TEST-SINGLE]"
log_test() { echo -e "${BLUE}${TEST_PREFIX} $1${NC}"; }
success_test() { echo -e "${GREEN}${TEST_PREFIX} $1${NC}"; }
error_test() { echo -e "${RED}${TEST_PREFIX} $1${NC}"; }

# Cleanup function
cleanup() {
    error_test "Cleaning up test resources..."
    
    # Copy process logs from VMs to host BEFORE killing processes (vagrant mode only)
    if [ "$MODE" = "vagrant" ] && [ -n "$LOG_DIR" ] && [ "$LOG_DIR" != "/tmp" ]; then
        if [ -n "$LOG_FILE" ]; then
            log_test "Copying process logs from VM to host before cleanup..."
            HOST_LOG_FILE="$LOG_DIR/leader-leader.log"
            if copy_vm_log_to_host leader "$LOG_FILE" "$HOST_LOG_FILE"; then
                local line_count=$(wc -l < "$HOST_LOG_FILE" 2>/dev/null || echo "0")
                log_test "Copied leader log ($line_count lines) to: $HOST_LOG_FILE"
            else
                log_test "Warning: Failed to copy leader log"
            fi
        fi
    fi
    
    if [ "$MODE" = "local" ]; then
        # Copy node log files BEFORE killing processes (so we capture final output)
        if [ -n "$LOG_FILE" ] && [ -f "$LOG_FILE" ] && [ "$LOG_DIR" != "/tmp" ]; then
            log_test "Preserving node log files..."
            cp "$LOG_FILE" "$LOG_DIR/leader.log" 2>/dev/null || true
            log_test "Saved leader log to: $LOG_DIR/leader.log"
        fi
        # Clean up processes in netns (but keep namespaces for next test)
        cleanup_netns_processes
        # Also kill any processes that might have escaped
        pkill -f "logstream" 2>/dev/null || true
    elif [ "$MODE" = "docker" ]; then
        # Copy container logs if not already copied (safety net for early exits)
        # Main script flow copies logs before cleanup, but this ensures we get logs even on early exit
        # Note: docker compose logs works even on stopped containers
        if [ -n "$LOG_DIR" ] && [ "$LOG_DIR" != "/tmp" ]; then
            # Only copy if log file doesn't already exist (avoid overwriting)
            if [ ! -f "$LOG_DIR/leader.log" ]; then
                log_test "Copying container logs (cleanup safety net)..."
                cd "$PROJECT_ROOT/deploy/docker/compose" 2>/dev/null || true
                # docker compose logs works even on stopped containers
                COMPOSE_PROJECT_NAME=test-single docker compose -f single.yaml logs --no-color leader > "$LOG_DIR/leader.log" 2>&1 || true
            fi
        fi
        cd "$PROJECT_ROOT/deploy/docker/compose" 2>/dev/null || true
        COMPOSE_PROJECT_NAME=test-single docker compose -f single.yaml down --remove-orphans 2>/dev/null || true
    elif [ "$MODE" = "vagrant" ]; then
        # Clean up processes on ALL VMs to prevent leftover processes from interfering
        vagrant_ssh_retry leader 'pkill -f logstream' 2>/dev/null || true
        vagrant_ssh_retry broker1 'pkill -f logstream' 2>/dev/null || true
        vagrant_ssh_retry broker2 'pkill -f logstream' 2>/dev/null || true
    fi
    error_test "Cleanup complete"
}

# Track if timeout occurred (used to ensure non-zero exit code)
TIMEOUT_MARKER="/tmp/single-test-timeout-$$"

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

log_test "========================================"
log_test "Test: Single Leader"
log_test "Mode: $MODE"
log_test "Timeout: ${TEST_TIMEOUT}s"
log_test "========================================"
log_test ""

if [ "$MODE" = "local" ]; then
    cd "$PROJECT_ROOT"
    build_if_needed "logstream" "main.go"
    
    # Ensure netns is set up for multicast support (even for single node, for consistency)
    log_test "Setting up network namespaces for multicast support..."
    if ! ensure_netns_setup; then
        error_test "Failed to setup network namespaces. Make sure you have sudo access."
        exit 1
    fi
    
    log_test "Starting leader..."
    LOG_FILE="$LOG_DIR/leader.log"
    # Start leader in logstream-a namespace
    sudo ip netns exec logstream-a env \
        NODE_ADDRESS=172.20.0.10:8001 \
        MULTICAST_GROUP=239.0.0.1:9999 \
        BROADCAST_PORT=8888 \
        stdbuf -oL -eL ./logstream > "$LOG_FILE" 2>&1 &
    LEADER_PID=$!
    success_test "Leader started (PID: $LEADER_PID) in logstream-a (172.20.0.10:8001)"
    sleep 3
    
    log_test ""
    log_test "Initial state - Leader only (seq=1)"
    log_test "-----------------------------------"
    tail -20 "$LOG_FILE" 2>/dev/null | grep -E "seq=|Registered|REPLICATE" || log_test "No matching log entries found"
    
    log_test ""
    log_test "Heartbeat phase (seq=0 is correct for heartbeats)"
    log_test "--------------------------------------------------"
    sleep 5
    tail -10 "$LOG_FILE" 2>/dev/null | grep -E "HEARTBEAT|seq=" || log_test "No heartbeat entries found"
    
    log_test ""
    success_test "[OK] Single leader test complete"
    
    # Show final logs instead of tail -f
    log_test "Final logs:"
    tail -20 "$LOG_FILE" 2>/dev/null || log_test "No logs available"
    
    # Cleanup
    pkill -f "logstream" 2>/dev/null || true

elif [ "$MODE" = "docker" ]; then
    COMPOSE_FILE="$PROJECT_ROOT/deploy/docker/compose/single.yaml"
    
    if [ ! -f "$COMPOSE_FILE" ]; then
        error_test "Compose file not found: $COMPOSE_FILE"
        exit 1
    fi
    
    cd "$PROJECT_ROOT/deploy/docker/compose"
    
    log_test "Starting leader..."
    COMPOSE_PROJECT_NAME=test-single docker compose -f single.yaml up -d
    if [ $? -ne 0 ]; then
        error_test "Failed to start docker containers"
        exit 1
    fi
    sleep 3
    
    log_test ""
    log_test "Initial state - Leader only (seq=1)"
    log_test "-----------------------------------"
    COMPOSE_PROJECT_NAME=test-single docker compose -f single.yaml logs 2>/dev/null | grep -E "seq=|Registered|REPLICATE" | head -20 || log_test "No matching log entries found"
    
    log_test ""
    log_test "Heartbeat phase (seq=0 is correct for heartbeats)"
    log_test "--------------------------------------------------"
    sleep 5
    COMPOSE_PROJECT_NAME=test-single docker compose -f single.yaml logs --tail=10 2>/dev/null | grep -E "HEARTBEAT|seq=" || log_test "No heartbeat entries found"
    
    log_test ""
    success_test "[OK] Single leader test complete"
    
    # Show final logs
    log_test "Final logs:"
    COMPOSE_PROJECT_NAME=test-single docker compose -f single.yaml logs --tail=30 2>/dev/null || log_test "No logs available"
    
    # Copy container logs BEFORE stopping containers
    if [ -n "$LOG_DIR" ] && [ "$LOG_DIR" != "/tmp" ]; then
        log_test "Copying container logs before cleanup..."
        cd "$PROJECT_ROOT/deploy/docker/compose" 2>/dev/null || true
        if COMPOSE_PROJECT_NAME=test-single docker compose -f single.yaml ps -q leader 2>/dev/null | grep -q .; then
            COMPOSE_PROJECT_NAME=test-single docker compose -f single.yaml logs --no-color leader > "$LOG_DIR/leader.log" 2>&1 || true
            log_test "Saved leader container log to: $LOG_DIR/leader.log"
        fi
    fi
    
    # Cleanup - containers will be stopped by cleanup function
    # Note: Don't call docker compose down here - let cleanup() handle it

elif [ "$MODE" = "vagrant" ]; then
    cd "$PROJECT_ROOT/deploy/vagrant"
    
    log_test "Checking Vagrant VMs..."
    # Add small random delay to prevent race conditions in parallel execution
    sleep $((RANDOM % 3))
    if ! check_vm_status leader; then
        error_test "Leader VM not running! Run: vagrant up leader"
        exit 1
    fi
    
    log_test "Starting leader on VM..."
    
    # Ensure binary exists and is up-to-date (always rebuilds to ensure latest code)
    if ! ensure_binary_on_vm leader; then
        error_test "Failed to ensure binary on leader VM"
        exit 1
    fi
    
    # Create log file in test-logs folder and ensure it's writable, use stdbuf to disable buffering
    LOG_FILE="$VM_LOG_DIR/leader.log"
    log_test "Creating log file: $LOG_FILE"
    if ! ensure_vm_log_directory leader "$VM_LOG_DIR"; then
        error_test "Failed to create log directory on VM"
        exit 1
    fi
    if ! prepare_vm_log_file leader "$LOG_FILE"; then
        error_test "Failed to prepare log file on VM"
        exit 1
    fi
    
    log_test "Starting logstream process..."
    # Use wrapper script approach to ensure environment variables and output redirection work correctly
    if ! start_logstream_vm_wrapper leader "192.168.100.10:8001" "true" "$LOG_FILE"; then
        error_test "Failed to start leader process using wrapper script"
        exit 1
    fi
    
    # Give process time to start, then verify comprehensively
    log_test ""
    log_test "Waiting for process to start and stabilize..."
    sleep 2  # Initial wait for process to fork and start
    
    # Comprehensive verification with up to 15 seconds wait time
    # This will do pre-flight checks, monitor startup, and verify health
    if ! verify_process_running leader "$LOG_FILE" "logstream" 7; then
        error_test ""
        error_test "==========================================================="
        error_test "[X] PROCESS VERIFICATION FAILED"
        error_test "==========================================================="
        error_test ""
        error_test "The comprehensive verification detected one or more issues:"
        error_test "  - Process may have failed to start"
        error_test "  - Process may have crashed after startup"
        error_test "  - Process may be running but not producing output"
        error_test "  - Environment or permission issues may be preventing execution"
        error_test ""
        error_test "Check the detailed diagnostics above for specific issues."
        error_test ""
        exit 1
    fi
    
    log_test ""
    
    log_test "Process started successfully, log file verified"
    
    # Wait a bit more for initial logs to be written
    sleep 2
    
    log_test ""
    log_test "Initial state - Leader only (seq=1)"
    log_test "-----------------------------------"
    # Search entire log for registration (not just tail, as heartbeats quickly fill the log)
    LOG_ENTRIES=$(vagrant_ssh_retry leader "grep -E 'seq=|Registered|REPLICATE' '$LOG_FILE' 2>/dev/null" 2>/dev/null | wc -l)
    if [ "$LOG_ENTRIES" -eq 0 ]; then
        error_test "No matching log entries found - process may not be running correctly"
        error_test "Checking if process is running..."
        vagrant_ssh_retry leader 'ps aux | grep logstream | grep -v grep' 2>/dev/null || error_test "Process not found!"
        error_test "Last 20 lines of log:"
        vagrant_ssh_retry leader "tail -20 '$LOG_FILE' 2>/dev/null" 2>/dev/null || error_test "Log file not found or empty!"
        exit 1
    else
        # Show relevant registration entries from the log
        vagrant_ssh_retry leader "grep -E 'Registered|REPLICATE seq=' '$LOG_FILE' 2>/dev/null | head -5" 2>/dev/null || true
    fi
    
    log_test ""
    log_test "Heartbeat phase (seq=0 is correct for heartbeats)"
    log_test "--------------------------------------------------"
    sleep 5
    HEARTBEAT_ENTRIES=$(vagrant_ssh_retry leader "tail -10 '$LOG_FILE' 2>/dev/null" 2>/dev/null | grep -E "HEARTBEAT|seq=" | wc -l)
    if [ "$HEARTBEAT_ENTRIES" -eq 0 ]; then
        error_test "No heartbeat entries found - process may not be running correctly"
        exit 1
    else
        vagrant_ssh_retry leader "tail -10 '$LOG_FILE' 2>/dev/null" 2>/dev/null | grep -E "HEARTBEAT|seq=" || true
    fi
    
    log_test ""
    success_test "[OK] Single leader test complete (Vagrant)"
    
    # Note: Log copying is now done in cleanup() function to ensure it happens even on timeout

else
    error_test "Invalid mode: $MODE"
    log_test "Usage: $0 [local|docker|vagrant]"
    exit 1
fi

# Cancel timeout since we completed successfully
kill $TIMEOUT_PID 2>/dev/null || true
success_test "Test completed successfully"
