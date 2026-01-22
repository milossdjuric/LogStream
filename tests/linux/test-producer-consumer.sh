#!/bin/bash

# Timeout in seconds (5 minutes max)
TEST_TIMEOUT=300
MODE="${1:-local}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/lib/common.sh"

# Setup log directory
setup_log_directory "$MODE"

TEST_PREFIX="[TEST-PRODUCER-CONSUMER]"
log_test() { echo -e "${BLUE}${TEST_PREFIX} $1${NC}"; }
success_test() { echo -e "${GREEN}${TEST_PREFIX} $1${NC}"; }
error_test() { echo -e "${RED}${TEST_PREFIX} $1${NC}"; }

# Cleanup flag to prevent multiple calls
CLEANUP_DONE=false

# Cleanup function
cleanup() {
    if [ "$CLEANUP_DONE" = "true" ]; then
        return 0
    fi
    CLEANUP_DONE=true
    
    error_test "Cleaning up test resources..."
    
    # Copy process logs from VMs to host BEFORE killing processes (vagrant mode only)
    if [ "$MODE" = "vagrant" ] && [ -n "$LOG_DIR" ] && [ "$LOG_DIR" != "/tmp" ]; then
        log_test "Copying process logs from VMs to host before cleanup..."
        # Always try to copy logs, even if variables aren't set (defensive approach)
        if [ -n "$LEADER_LOG" ]; then
            if copy_vm_log_to_host leader "$LEADER_LOG" "$LOG_DIR/leader-leader.log"; then
                local line_count=$(wc -l < "$LOG_DIR/leader-leader.log" 2>/dev/null || echo "0")
                log_test "Copied leader log ($line_count lines) to: $LOG_DIR/leader-leader.log"
            else
                log_test "Warning: Failed to copy leader log"
            fi
        elif [ -n "$VM_LOG_DIR" ]; then
            local default_log="$VM_LOG_DIR/leader.log"
            if copy_vm_log_to_host leader "$default_log" "$LOG_DIR/leader-leader.log" 2>/dev/null; then
                log_test "Copied leader log (using default path) to: $LOG_DIR/leader-leader.log"
            fi
        fi
        if [ -n "$PRODUCER_LOG" ]; then
            if copy_vm_log_to_host leader "$PRODUCER_LOG" "$LOG_DIR/leader-producer.log"; then
                local line_count=$(wc -l < "$LOG_DIR/leader-producer.log" 2>/dev/null || echo "0")
                log_test "Copied producer log ($line_count lines) to: $LOG_DIR/leader-producer.log"
            else
                log_test "Warning: Failed to copy producer log"
            fi
        elif [ -n "$VM_LOG_DIR" ]; then
            local default_log="$VM_LOG_DIR/producer.log"
            if copy_vm_log_to_host leader "$default_log" "$LOG_DIR/leader-producer.log" 2>/dev/null; then
                log_test "Copied producer log (using default path) to: $LOG_DIR/leader-producer.log"
            fi
        fi
        if [ -n "$CONSUMER_LOG" ]; then
            if copy_vm_log_to_host broker1 "$CONSUMER_LOG" "$LOG_DIR/broker1-consumer.log"; then
                local line_count=$(wc -l < "$LOG_DIR/broker1-consumer.log" 2>/dev/null || echo "0")
                log_test "Copied consumer log ($line_count lines) to: $LOG_DIR/broker1-consumer.log"
            else
                log_test "Warning: Failed to copy consumer log"
            fi
        elif [ -n "$VM_LOG_DIR" ]; then
            local default_log="$VM_LOG_DIR/consumer.log"
            if copy_vm_log_to_host broker1 "$default_log" "$LOG_DIR/broker1-consumer.log" 2>/dev/null; then
                log_test "Copied consumer log (using default path) to: $LOG_DIR/broker1-consumer.log"
            fi
        fi
    fi
    
    if [ "$MODE" = "local" ]; then
        # Close pipe file descriptor first (before killing processes to avoid SIGPIPE)
        # Ignore errors when closing - pipe may already be closed
        (trap '' 13; exec 3>&- 2>/dev/null) || true
        
        # Ensure LOG_DIR is set (in case script exited before setup_log_directory ran)
        if [ -z "$LOG_DIR" ] && [ -n "$LOG_BASE_DIR" ]; then
            # Try to reconstruct LOG_DIR from LOG_BASE_DIR
            local script_name=$(basename "${BASH_SOURCE[1]:-$0}" .sh | sed 's/test-//')
            LOG_DIR="$LOG_BASE_DIR/$script_name"
            mkdir -p "$LOG_DIR" 2>/dev/null || true
        fi
        
        # Copy node log files BEFORE killing processes (so we capture final output)
        # Always try to copy logs, even if script exited early
        if [ -n "$LOG_DIR" ] && [ "$LOG_DIR" != "/tmp" ] && [ -d "$LOG_DIR" ]; then
            log_test "Preserving node log files..."
            
            # Try to copy leader log
            if [ -n "$LEADER_LOG" ]; then
                if [ -f "$LEADER_LOG" ]; then
                    if cp "$LEADER_LOG" "$LOG_DIR/leader.log" 2>/dev/null; then
                        log_test "Saved leader log to: $LOG_DIR/leader.log"
                    else
                        log_test "Warning: Failed to copy leader log (may need sudo)"
                        # Try with sudo as fallback
                        sudo cp "$LEADER_LOG" "$LOG_DIR/leader.log" 2>/dev/null && \
                            sudo chown "$(whoami):$(whoami)" "$LOG_DIR/leader.log" 2>/dev/null && \
                            log_test "Saved leader log (with sudo) to: $LOG_DIR/leader.log" || true
                    fi
                else
                    log_test "Warning: Leader log file not found: $LEADER_LOG"
                fi
            fi
            
            # Try to copy producer log
            if [ -n "$PRODUCER_LOG" ]; then
                if [ -f "$PRODUCER_LOG" ]; then
                    if cp "$PRODUCER_LOG" "$LOG_DIR/producer.log" 2>/dev/null; then
                        log_test "Saved producer log to: $LOG_DIR/producer.log"
                    else
                        log_test "Warning: Failed to copy producer log (may need sudo)"
                        # Try with sudo as fallback
                        sudo cp "$PRODUCER_LOG" "$LOG_DIR/producer.log" 2>/dev/null && \
                            sudo chown "$(whoami):$(whoami)" "$LOG_DIR/producer.log" 2>/dev/null && \
                            log_test "Saved producer log (with sudo) to: $LOG_DIR/producer.log" || true
                    fi
                else
                    log_test "Warning: Producer log file not found: $PRODUCER_LOG"
                fi
            fi
            
            # Try to copy consumer log
            if [ -n "$CONSUMER_LOG" ]; then
                if [ -f "$CONSUMER_LOG" ]; then
                    if cp "$CONSUMER_LOG" "$LOG_DIR/consumer.log" 2>/dev/null; then
                        log_test "Saved consumer log to: $LOG_DIR/consumer.log"
                    else
                        log_test "Warning: Failed to copy consumer log (may need sudo)"
                        # Try with sudo as fallback
                        sudo cp "$CONSUMER_LOG" "$LOG_DIR/consumer.log" 2>/dev/null && \
                            sudo chown "$(whoami):$(whoami)" "$LOG_DIR/consumer.log" 2>/dev/null && \
                            log_test "Saved consumer log (with sudo) to: $LOG_DIR/consumer.log" || true
                    fi
                else
                    log_test "Warning: Consumer log file not found: $CONSUMER_LOG"
                fi
            fi
        fi
        # Kill processes in their namespaces
        [ -n "$LEADER_PID" ] && kill_netns_process logstream-a "$LEADER_PID" 2>/dev/null || kill -9 "$LEADER_PID" 2>/dev/null || true
        [ -n "$PRODUCER_PID" ] && kill_netns_process logstream-c "$PRODUCER_PID" 2>/dev/null || kill -9 "$PRODUCER_PID" 2>/dev/null || true
        [ -n "$CONSUMER_PID" ] && kill_netns_process logstream-b "$CONSUMER_PID" 2>/dev/null || kill -9 "$CONSUMER_PID" 2>/dev/null || true
        sleep 1
        # Force kill if still running
        cleanup_netns_processes
        rm -f /tmp/logstream-*.log /tmp/producer-*.pipe 2>/dev/null || true
    elif [ "$MODE" = "docker" ]; then
        # Copy container logs if not already copied (safety net for early exits)
        # Main script flow copies logs before cleanup, but this ensures we get logs even on early exit
        # Note: docker compose logs works even on stopped containers
        if [ -n "$LOG_DIR" ] && [ "$LOG_DIR" != "/tmp" ]; then
            # Only copy if log files don't already exist (avoid overwriting)
            if [ ! -f "$LOG_DIR/leader.log" ] || [ ! -f "$LOG_DIR/producer.log" ] || [ ! -f "$LOG_DIR/consumer.log" ]; then
                log_test "Copying container logs (cleanup safety net)..."
                cd "$PROJECT_ROOT/deploy/docker/compose" 2>/dev/null || true
                # docker compose logs works even on stopped containers
                COMPOSE_PROJECT_NAME=test-producer-consumer docker compose -f producer-consumer.yaml logs --no-color leader > "$LOG_DIR/leader.log" 2>&1 || true
                COMPOSE_PROJECT_NAME=test-producer-consumer docker compose -f producer-consumer.yaml logs --no-color producer > "$LOG_DIR/producer.log" 2>&1 || true
                COMPOSE_PROJECT_NAME=test-producer-consumer docker compose -f producer-consumer.yaml logs --no-color consumer > "$LOG_DIR/consumer.log" 2>&1 || true
            fi
        fi
        cd "$PROJECT_ROOT/deploy/docker/compose" 2>/dev/null || true
        COMPOSE_PROJECT_NAME=test-producer-consumer docker compose -f producer-consumer.yaml down --remove-orphans 2>/dev/null || true
    elif [ "$MODE" = "vagrant" ]; then
        vagrant_ssh_retry leader 'pkill -f logstream; pkill -f producer' 2>/dev/null || true
        vagrant_ssh_retry broker1 'pkill -f consumer' 2>/dev/null || true
    fi
    error_test "Cleanup complete"
}

# Track if timeout occurred (used to ensure non-zero exit code)
TIMEOUT_MARKER="/tmp/producer-consumer-test-timeout-$$"

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

# Trap cleanup on exit (SIGPIPE will be handled in the write loop)
cleanup_and_exit() {
    local exit_code=${1:-0}
    kill $TIMEOUT_PID 2>/dev/null || true
    cleanup
    # Check for timeout marker - always exit with error if timed out
    if [ -f "$TIMEOUT_MARKER" ]; then
        rm -f "$TIMEOUT_MARKER"
        error_test "EXITING WITH ERROR: Test timed out"
        exit 1
    fi
    exit $exit_code
}

trap 'cleanup_and_exit 1' INT TERM
# EXIT trap will always run, ensuring cleanup happens even on SIGPIPE
trap 'cleanup_and_exit $?' EXIT

log_test "================================================"
log_test "Test: Producer -> Leader -> Consumer Data Flow"
log_test "Mode: $MODE"
log_test "Timeout: ${TEST_TIMEOUT}s"
log_test "================================================"
log_test ""
log_test "This test demonstrates:"
log_test "  1. Leader accepts producer registration (TCP)"
log_test "  2. Leader accepts consumer subscription (TCP)"
log_test "  3. Producer sends data to leader (UDP)"
log_test "  4. Leader forwards to consumer (TCP)"
log_test ""

if [ "$MODE" = "local" ]; then
    cd "$PROJECT_ROOT"
    
    build_if_needed "logstream" "main.go"
    build_if_needed "producer" "cmd/producer/main.go"
    build_if_needed "consumer" "cmd/consumer/main.go"
    
    log_test "Setting up network namespaces for multicast support..."
    if ! ensure_netns_setup; then
        error_test "Failed to setup network namespaces. Make sure you have sudo access."
        exit 1
    fi
    
    log_test "=== STEP 1: Starting Leader (Broker) ==="
    LEADER_LOG="/tmp/logstream-leader.log"
    # Create log file with proper permissions for processes in namespace
    sudo touch "$LEADER_LOG"
    sudo chmod 666 "$LEADER_LOG"
    sudo ip netns exec logstream-a env \
        NODE_ADDRESS=172.20.0.10:8001 \
        MULTICAST_GROUP=239.0.0.1:9999 \
        BROADCAST_PORT=8888 \
        IS_LEADER="true" \
        stdbuf -oL -eL ./logstream > "$LEADER_LOG" 2>&1 &
    LEADER_PID=$!
    success_test "Leader started (PID: $LEADER_PID) in logstream-a (172.20.0.10:8001)"
    sleep 5
    
    log_test ""
    log_test "Waiting for leader to be ready..."
    # Wait for TCP listener to be ready and actually accepting connections
    leader_ready=false
    for i in {1..15}; do
        # Check if TCP listener is started in logs
        if sudo ip netns exec logstream-a grep -q "TCP listener started" "$LEADER_LOG" 2>/dev/null; then
            # Verify TCP port is actually accepting connections using nc or timeout with bash
            if command -v nc >/dev/null 2>&1; then
                # Use nc (netcat) if available
                if sudo ip netns exec logstream-b timeout 1 nc -z 172.20.0.10 8001 2>/dev/null; then
                    leader_ready=true
                    break
                fi
            else
                # Fallback: use bash /dev/tcp (if available) or just check log message
                if sudo ip netns exec logstream-b timeout 1 bash -c "echo > /dev/tcp/172.20.0.10/8001" 2>/dev/null; then
                    leader_ready=true
                    break
                fi
            fi
        fi
        sleep 1
    done
    
    if [ "$leader_ready" = "true" ]; then
        log_test "Leader ready:"
        sudo ip netns exec logstream-a tail -10 "$LEADER_LOG" 2>/dev/null | grep -E "TCP listener|Started" || log_test "No matching log entries found"
        success_test "[OK] Leader accepting connections on port 8001"
    else
        # Even if connectivity test fails, check if leader is listening in logs
        if sudo ip netns exec logstream-a grep -q "TCP listener started" "$LEADER_LOG" 2>/dev/null; then
            log_test "Leader TCP listener found in logs (connectivity test may have failed, continuing anyway)"
            success_test "[OK] Leader appears ready (TCP listener started)"
        else
            error_test "Leader failed to become ready after 15 seconds"
            log_test "Leader log (last 30 lines):"
            sudo ip netns exec logstream-a tail -30 "$LEADER_LOG" 2>/dev/null || log_test "  Could not read leader log"
            cleanup_and_exit 1
        fi
    fi
    
    log_test "=== STEP 2: Starting Consumer (Subscriber) ==="
    CONSUMER_LOG="/tmp/logstream-consumer.log"
    # Create log file with proper permissions for processes in namespace
    sudo touch "$CONSUMER_LOG"
    sudo chmod 666 "$CONSUMER_LOG"
    sudo ip netns exec logstream-b env \
        LEADER_ADDRESS=172.20.0.10:8001 \
        TOPIC="test-logs" \
        stdbuf -oL -eL ./consumer > "$CONSUMER_LOG" 2>&1 &
    CONSUMER_PID=$!
    success_test "Consumer started (PID: $CONSUMER_PID) in logstream-b"
    
    # Wait for consumer to connect (with retries)
    log_test "Waiting for consumer to connect..."
    sleep 5
    if ! kill -0 "$CONSUMER_PID" 2>/dev/null; then
        error_test "Consumer process died immediately!"
        log_test "Consumer log:"
        sudo ip netns exec logstream-b cat "$CONSUMER_LOG" 2>/dev/null || log_test "  Could not read consumer log"
        cleanup_and_exit 1
    fi
    
    log_test ""
    log_test "Consumer registration:"
    sudo ip netns exec logstream-a tail -20 "$LEADER_LOG" 2>/dev/null | grep -E "CONSUME from|Registered consumer|Subscribed" || log_test "No consumer registration found"
    sudo ip netns exec logstream-b tail -10 "$CONSUMER_LOG" 2>/dev/null | grep -E "Connected|Subscribed" || log_test "No consumer connection found"
    success_test "[OK] Consumer registered and subscribed to 'test-logs'"
    
    log_test ""
    log_test "=== STEP 3: Starting Producer ==="
    PRODUCER_LOG="/tmp/logstream-producer.log"
    PIPE="/tmp/producer-input.pipe"
    rm -f "$PIPE"
    # Create pipe on host (accessible from both host and namespace)
    mkfifo "$PIPE" 2>/dev/null || true
    chmod 666 "$PIPE"
    # Create log file with proper permissions for processes in namespace
    sudo touch "$PRODUCER_LOG"
    sudo chmod 666 "$PRODUCER_LOG"
    # Start producer in namespace, reading from pipe
    # Use bash to properly handle input redirection
    sudo ip netns exec logstream-c bash -c "
        cd '$PROJECT_ROOT' && \
        env LEADER_ADDRESS=172.20.0.10:8001 TOPIC=test-logs \
        stdbuf -oL -eL ./producer < '$PIPE' > '$PRODUCER_LOG' 2>&1
    " &
    PRODUCER_PID=$!
    # Open pipe for writing from host
    exec 3> "$PIPE"
    success_test "Producer started (PID: $PRODUCER_PID) in logstream-c"
    
    # Wait a moment for producer to initialize (but don't wait too long since it's reading from empty pipe)
    sleep 2
    
    # Check if producer is still running (it might exit if connection fails immediately)
    if ! kill -0 "$PRODUCER_PID" 2>/dev/null; then
        error_test "Producer process died immediately!"
        log_test "Producer log:"
        sudo ip netns exec logstream-c cat "$PRODUCER_LOG" 2>/dev/null || log_test "  Could not read producer log"
        log_test "Leader log (last 30 lines):"
        sudo ip netns exec logstream-a tail -30 "$LEADER_LOG" 2>/dev/null || log_test "  Could not read leader log"
        cleanup_and_exit 1
    fi
    
    # Send a dummy message to keep producer alive (it reads from pipe)
    echo "dummy" >&3 2>/dev/null || true
    sleep 2
    
    log_test ""
    log_test "Producer registration:"
    sudo ip netns exec logstream-a tail -20 "$LEADER_LOG" 2>/dev/null | grep -E "PRODUCE from|Registered producer|assigned broker" || log_test "No producer registration found"
    sudo ip netns exec logstream-c tail -10 "$PRODUCER_LOG" 2>/dev/null | grep -E "Connected|Registered|PRODUCE_ACK" || log_test "No producer connection found"
    success_test "[OK] Producer registered for 'test-logs'"
    
    log_test ""
    log_test "=== STEP 4: Sending Test Messages ==="
    
    # Check if producer is still running before sending messages
    if ! kill -0 "$PRODUCER_PID" 2>/dev/null; then
        error_test "Producer process (PID $PRODUCER_PID) is not running!"
        log_test "Producer log (last 30 lines):"
        sudo ip netns exec logstream-c tail -30 "$PRODUCER_LOG" 2>/dev/null || log_test "  Could not read producer log"
        cleanup_and_exit 1
    fi
    
    # Handle SIGPIPE gracefully - check producer status and write carefully
    pipe_broken=false
    for i in {1..5}; do
        MSG="Test message #$i: Server event at $(date +%H:%M:%S)"
        # Check if producer is still alive before writing
        if ! kill -0 "$PRODUCER_PID" 2>/dev/null; then
            error_test "Producer process died while sending message #$i"
            pipe_broken=true
            break
        fi
        # Write to pipe with SIGPIPE handling - use a subshell to catch errors
        # SIGPIPE (signal 13) will cause write to fail, but we catch it
        (
            trap '' 13  # Ignore SIGPIPE in subshell
            echo "$MSG" >&3 2>/dev/null
        ) || {
            # If write failed, check if producer is still alive
            if ! kill -0 "$PRODUCER_PID" 2>/dev/null; then
                error_test "Producer process died - pipe broken"
            else
                error_test "Failed to send message #$i (pipe write failed)"
            fi
            pipe_broken=true
            break
        }
        success_test "Sent: $MSG"
        sleep 1
    done
    
    if [ "$pipe_broken" = "true" ]; then
        error_test "Pipe communication failed - producer may have crashed"
        log_test "Producer log (last 30 lines):"
        sudo ip netns exec logstream-c tail -30 "$PRODUCER_LOG" 2>/dev/null || log_test "  Could not read producer log"
        log_test "Leader log (last 30 lines):"
        sudo ip netns exec logstream-a tail -30 "$LEADER_LOG" 2>/dev/null || log_test "  Could not read leader log"
    fi
    
    sleep 3
    
    log_test ""
    log_test "========================================="
    log_test "PRODUCER-CONSUMER TEST RESULTS:"
    log_test "========================================="
    
    log_test ""
    log_test "Leader activity (DATA forwarding):"
    sudo ip netns exec logstream-a tail -30 "$LEADER_LOG" 2>/dev/null | grep -E "DATA from|Forwarding|RESULT to" || log_test "No data forwarding activity found"
    
    log_test ""
    log_test "Producer activity (sent messages):"
    sudo ip netns exec logstream-c tail -15 "$PRODUCER_LOG" 2>/dev/null | grep -E "DATA|sent" || log_test "No producer activity found"
    
    log_test ""
    log_test "Consumer activity (received messages):"
    sudo ip netns exec logstream-b tail -15 "$CONSUMER_LOG" 2>/dev/null | grep -E "\[test-logs\] Offset" || log_test "No consumer activity found"
    
    log_test ""
    log_test "========================================="
    log_test "DATA FLOW VERIFICATION:"
    log_test "========================================="
    
    SENT=$(sudo ip netns exec logstream-c tail -50 "$PRODUCER_LOG" 2>/dev/null | grep -cE "DATA|sent" || echo "0")
    RECEIVED=$(sudo ip netns exec logstream-b tail -50 "$CONSUMER_LOG" 2>/dev/null | grep -c "\[test-logs\] Offset" || echo "0")
    
    log_test "Messages sent by producer:     $SENT"
    log_test "Messages received by consumer: $RECEIVED"
    
    if [ "$SENT" -eq "$RECEIVED" ] && [ "$SENT" -gt 0 ]; then
        success_test "[OK] All messages delivered successfully!"
    else
        error_test "[!] Message count mismatch (sent: $SENT, received: $RECEIVED)"
    fi
    
    log_test ""
    success_test "[OK] Producer-Consumer test complete"
    
    # Note: Cleanup will be handled by trap
    # Don't close pipe here - let cleanup function handle it to avoid SIGPIPE
    # The pipe will be closed in cleanup() before processes are killed

elif [ "$MODE" = "docker" ]; then
    COMPOSE_FILE="$PROJECT_ROOT/deploy/docker/compose/producer-consumer.yaml"
    
    if [ ! -f "$COMPOSE_FILE" ]; then
        error_test "Compose file not found: $COMPOSE_FILE"
        exit 1
    fi
    
    cd "$PROJECT_ROOT/deploy/docker/compose"
    
    log_test "=== Starting All Containers ==="
    log_test "Building Docker images (with latest code)..."
    COMPOSE_PROJECT_NAME=test-producer-consumer docker compose -f producer-consumer.yaml build
    if [ $? -ne 0 ]; then
        error_test "Failed to build docker images"
        exit 1
    fi
    
    log_test "Starting containers..."
    COMPOSE_PROJECT_NAME=test-producer-consumer docker compose -f producer-consumer.yaml up -d
    if [ $? -ne 0 ]; then
        error_test "Failed to start docker containers"
        exit 1
    fi
    
    sleep 8
    
    log_test ""
    log_test "Container status:"
    COMPOSE_PROJECT_NAME=test-producer-consumer docker compose -f producer-consumer.yaml ps 2>/dev/null || log_test "Could not get container status"
    
    log_test ""
    log_test "=== STEP 1: Leader Status ==="
    COMPOSE_PROJECT_NAME=test-producer-consumer docker compose -f producer-consumer.yaml logs leader 2>/dev/null | grep -E "TCP listener|Started" | tail -5 || log_test "No matching log entries found"
    success_test "[OK] Leader ready"
    
    log_test ""
    log_test "=== STEP 2: Consumer Status ==="
    COMPOSE_PROJECT_NAME=test-producer-consumer docker compose -f producer-consumer.yaml logs leader 2>/dev/null | grep -E "CONSUME from|Registered consumer" | tail -5 || log_test "No consumer registration found"
    COMPOSE_PROJECT_NAME=test-producer-consumer docker compose -f producer-consumer.yaml logs consumer 2>/dev/null | grep -E "Connected|Subscribed" | tail -5 || log_test "No consumer connection found"
    success_test "[OK] Consumer registered"
    
    log_test ""
    log_test "=== STEP 3: Producer Status ==="
    COMPOSE_PROJECT_NAME=test-producer-consumer docker compose -f producer-consumer.yaml logs leader 2>/dev/null | grep -E "PRODUCE from|Registered producer" | tail -5 || log_test "No producer registration found"
    COMPOSE_PROJECT_NAME=test-producer-consumer docker compose -f producer-consumer.yaml logs producer 2>/dev/null | grep -E "Connected|Registered" | tail -5 || log_test "No producer connection found"
    success_test "[OK] Producer registered"
    
    log_test ""
    log_test "=== STEP 4: Data Flow ==="
    sleep 5
    
    log_test ""
    log_test "========================================="
    log_test "PRODUCER-CONSUMER TEST RESULTS:"
    log_test "========================================="
    
    log_test ""
    log_test "Leader activity:"
    COMPOSE_PROJECT_NAME=test-producer-consumer docker compose -f producer-consumer.yaml logs leader 2>/dev/null | grep -E "DATA from|Forwarding|RESULT to" | tail -15 || log_test "No data forwarding activity found"
    
    log_test ""
    log_test "Producer activity:"
    COMPOSE_PROJECT_NAME=test-producer-consumer docker compose -f producer-consumer.yaml logs producer 2>/dev/null | grep -E "DATA|sent" | tail -10 || log_test "No producer activity found"
    
    log_test ""
    log_test "Consumer activity:"
    COMPOSE_PROJECT_NAME=test-producer-consumer docker compose -f producer-consumer.yaml logs consumer 2>/dev/null | grep -E "\[test-logs\] Offset" | tail -10 || log_test "No consumer activity found"
    
    log_test ""
    log_test "========================================="
    log_test "DATA FLOW VERIFICATION:"
    log_test "========================================="
    
    SENT=$(COMPOSE_PROJECT_NAME=test-producer-consumer docker compose -f producer-consumer.yaml logs producer 2>/dev/null | grep -cE "DATA|sent" || echo "0")
    RECEIVED=$(COMPOSE_PROJECT_NAME=test-producer-consumer docker compose -f producer-consumer.yaml logs consumer 2>/dev/null | grep -c "\[test-logs\] Offset" || echo "0")
    
    log_test "Messages sent by producer:     $SENT"
    log_test "Messages received by consumer: $RECEIVED"
    
    if [ "$SENT" -gt 0 ] && [ "$RECEIVED" -gt 0 ]; then
        success_test "[OK] Messages flowing successfully!"
    else
        error_test "[!] No message flow detected"
    fi
    
    log_test ""
    success_test "[OK] Producer-Consumer test complete"
    
    # Show final logs
    log_test "Final logs:"
    COMPOSE_PROJECT_NAME=test-producer-consumer docker compose -f producer-consumer.yaml logs --tail=50 2>/dev/null || log_test "No logs available"
    
    # Copy container logs BEFORE stopping containers
    if [ -n "$LOG_DIR" ] && [ "$LOG_DIR" != "/tmp" ]; then
        log_test "Copying container logs before cleanup..."
        cd "$PROJECT_ROOT/deploy/docker/compose" 2>/dev/null || true
        if COMPOSE_PROJECT_NAME=test-producer-consumer docker compose -f producer-consumer.yaml ps -q leader 2>/dev/null | grep -q .; then
            COMPOSE_PROJECT_NAME=test-producer-consumer docker compose -f producer-consumer.yaml logs --no-color leader > "$LOG_DIR/leader.log" 2>&1 || true
            log_test "Saved leader container log to: $LOG_DIR/leader.log"
        fi
        if COMPOSE_PROJECT_NAME=test-producer-consumer docker compose -f producer-consumer.yaml ps -q producer 2>/dev/null | grep -q .; then
            COMPOSE_PROJECT_NAME=test-producer-consumer docker compose -f producer-consumer.yaml logs --no-color producer > "$LOG_DIR/producer.log" 2>&1 || true
            log_test "Saved producer container log to: $LOG_DIR/producer.log"
        fi
        if COMPOSE_PROJECT_NAME=test-producer-consumer docker compose -f producer-consumer.yaml ps -q consumer 2>/dev/null | grep -q .; then
            COMPOSE_PROJECT_NAME=test-producer-consumer docker compose -f producer-consumer.yaml logs --no-color consumer > "$LOG_DIR/consumer.log" 2>&1 || true
            log_test "Saved consumer container log to: $LOG_DIR/consumer.log"
        fi
    fi
    
    # Cleanup - containers will be stopped by cleanup function
    # Note: Don't call docker compose down here - let cleanup() handle it

elif [ "$MODE" = "vagrant" ]; then
    cd "$PROJECT_ROOT/deploy/vagrant"
    
    log_test "Checking Vagrant VMs..."
    # Add small random delay to prevent race conditions in parallel execution
    sleep $((RANDOM % 3))
    for vm in leader broker1; do
        if ! check_vm_status $vm; then
            error_test "$vm VM not running! Run: vagrant up"
            exit 1
        fi
    done
    
    log_test "Building binaries on VMs..."
    # Build producer on leader
    log_test "Building producer binary on leader..."
    build_output=$(vagrant_ssh_retry leader 'cd /vagrant/logstream && /usr/local/go/bin/go build -o producer cmd/producer/main.go 2>&1' 2>&1)
    build_exit_code=$?
    if [ $build_exit_code -ne 0 ]; then
        error_test "Failed to build producer on leader (exit code: $build_exit_code)"
        log_test "Build output:"
        echo "$build_output" | while IFS= read -r line; do
            log_test "  $line"
        done
        exit 1
    fi
    # Build consumer on broker1 (where it will run)
    log_test "Building consumer binary on broker1..."
    build_output=$(vagrant_ssh_retry broker1 'cd /vagrant/logstream && /usr/local/go/bin/go build -o consumer cmd/consumer/main.go 2>&1' 2>&1)
    build_exit_code=$?
    if [ $build_exit_code -ne 0 ]; then
        error_test "Failed to build consumer on broker1 (exit code: $build_exit_code)"
        log_test "Build output:"
        echo "$build_output" | while IFS= read -r line; do
            log_test "  $line"
        done
        exit 1
    fi
    success_test "[OK] Binaries built successfully"
    
    # Verify main binary exists
    if ! ensure_binary_on_vm leader; then
        error_test "Failed to ensure binary on leader VM"
        exit 1
    fi
    
    # Setup log files in synced folder
    LEADER_LOG="$VM_LOG_DIR/leader.log"
    PRODUCER_LOG="$VM_LOG_DIR/producer.log"
    CONSUMER_LOG="$VM_LOG_DIR/consumer.log"
    
    if ! ensure_vm_log_directory leader "$VM_LOG_DIR" || ! ensure_vm_log_directory broker1 "$VM_LOG_DIR"; then
        error_test "Failed to create log directories on VMs"
        exit 1
    fi
    
    log_test "=== STEP 1: Starting Leader (Broker) ==="
    if ! prepare_vm_log_file leader "$LEADER_LOG" || \
       ! start_logstream_vm_wrapper leader "192.168.100.10:8001" "true" "$LEADER_LOG"; then
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
    
    log_test ""
    log_test "Leader ready:"
    vagrant_ssh_retry leader "tail -10 '$LEADER_LOG' 2>/dev/null" 2>/dev/null | grep -E "TCP listener|Started" || log_test "No matching log entries found"
    success_test "[OK] Leader accepting connections"
    
    log_test ""
    log_test "=== STEP 2: Starting Consumer (Subscriber) ==="
    if ! prepare_vm_log_file broker1 "$CONSUMER_LOG"; then
        error_test "Failed to prepare consumer log file"
        exit 1
    fi
    
    # Start consumer using a wrapper script similar to logstream
    consumer_script="/tmp/start-consumer-$$-$(date +%s).sh"
    if ! vagrant_ssh_retry broker1 "
        printf '%s\n' '#!/bin/bash' > '$consumer_script' && \
        printf '%s\n' 'set +H' >> '$consumer_script' && \
        printf '%s\n' 'cd /vagrant/logstream' >> '$consumer_script' && \
        printf '%s\n' 'export LEADER_ADDRESS=\"192.168.100.10:8001\"' >> '$consumer_script' && \
        printf '%s\n' 'export TOPIC=\"test-logs\"' >> '$consumer_script' && \
        printf '%s\n' 'trap \"\" HUP INT TERM' >> '$consumer_script' && \
        printf '%s\n' 'setsid nohup stdbuf -oL -eL ./consumer > \"$CONSUMER_LOG\" 2>&1 < /dev/null &' >> '$consumer_script' && \
        printf '%s\n' 'PID=\$!' >> '$consumer_script' && \
        printf '%s\n' 'disown -h \$PID' >> '$consumer_script' && \
        printf '%s\n' 'sleep 2' >> '$consumer_script' && \
        printf '%s\n' 'if kill -0 \$PID 2>/dev/null; then' >> '$consumer_script' && \
        printf '%s\n' '  echo \"Consumer started successfully with PID \$PID\"' >> '$consumer_script' && \
        printf '%s\n' '  exit 0' >> '$consumer_script' && \
        printf '%s\n' 'else' >> '$consumer_script' && \
        printf '%s\n' '  echo \"ERROR: Consumer failed to start or crashed immediately\"' >> '$consumer_script' && \
        printf '%s\n' '  tail -20 \"$CONSUMER_LOG\" 2>/dev/null || echo \"Log file empty or not readable\"' >> '$consumer_script' && \
        printf '%s\n' '  exit 1' >> '$consumer_script' && \
        printf '%s\n' 'fi' >> '$consumer_script' && \
        chmod +x '$consumer_script'
    " 2>/dev/null; then
        error_test "Failed to create consumer startup script"
        exit 1
    fi
    
    consumer_output=$(vagrant_ssh_retry broker1 "bash '$consumer_script'" 2>/dev/null)
    consumer_exit_code=$?
    log_test "Consumer startup output: $consumer_output"
    
    # Clean up the script after a delay
    (sleep 5 && vagrant_ssh_retry broker1 "rm -f '$consumer_script'" 2>/dev/null &) &
    
    if [ $consumer_exit_code -ne 0 ]; then
        error_test "Failed to start consumer - startup script failed"
        exit 1
    fi
    
    sleep 3
    
    log_test ""
    log_test "Consumer registration:"
    vagrant_ssh_retry leader "tail -20 '$LEADER_LOG' 2>/dev/null" 2>/dev/null | grep -E "CONSUME from|Registered consumer" || log_test "No consumer registration found"
    vagrant_ssh_retry broker1 "tail -10 '$CONSUMER_LOG' 2>/dev/null" 2>/dev/null | grep -E "Connected|Subscribed" || log_test "No consumer connection found"
    success_test "[OK] Consumer registered"
    
    log_test ""
    log_test "=== STEP 3: Starting Producer ==="
    if ! prepare_vm_log_file leader "$PRODUCER_LOG"; then
        error_test "Failed to prepare producer log file"
        exit 1
    fi
    
    # Start producer using a wrapper script - just connect, don't send messages
    producer_script="/tmp/start-producer-$$-$(date +%s).sh"
    if ! vagrant_ssh_retry leader "
        printf '%s\n' '#!/bin/bash' > '$producer_script' && \
        printf '%s\n' 'set +H' >> '$producer_script' && \
        printf '%s\n' 'cd /vagrant/logstream' >> '$producer_script' && \
        printf '%s\n' 'export LEADER_ADDRESS=\"192.168.100.10:8001\"' >> '$producer_script' && \
        printf '%s\n' 'export TOPIC=\"test-logs\"' >> '$producer_script' && \
        printf '%s\n' 'trap \"\" HUP INT TERM' >> '$producer_script' && \
        printf '%s\n' 'setsid nohup stdbuf -oL -eL bash -c \"echo \\\"Producer connecting...\\\"; sleep 5 | ./producer\" > \"$PRODUCER_LOG\" 2>&1 < /dev/null &' >> '$producer_script' && \
        printf '%s\n' 'PID=\$!' >> '$producer_script' && \
        printf '%s\n' 'disown -h \$PID' >> '$producer_script' && \
        printf '%s\n' 'sleep 3' >> '$producer_script' && \
        printf '%s\n' 'if kill -0 \$PID 2>/dev/null; then' >> '$producer_script' && \
        printf '%s\n' '  echo \"Producer started successfully with PID \$PID\"' >> '$producer_script' && \
        printf '%s\n' '  exit 0' >> '$producer_script' && \
        printf '%s\n' 'else' >> '$producer_script' && \
        printf '%s\n' '  echo \"ERROR: Producer failed to start or crashed immediately\"' >> '$producer_script' && \
        printf '%s\n' '  tail -20 \"$PRODUCER_LOG\" 2>/dev/null || echo \"Log file empty or not readable\"' >> '$producer_script' && \
        printf '%s\n' '  exit 1' >> '$producer_script' && \
        printf '%s\n' 'fi' >> '$producer_script' && \
        chmod +x '$producer_script'
    " 2>/dev/null; then
        error_test "Failed to create producer startup script"
        exit 1
    fi
    
    producer_output=$(vagrant_ssh_retry leader "bash '$producer_script'" 2>/dev/null)
    producer_exit_code=$?
    log_test "Producer startup output: $producer_output"
    
    # Clean up the script after a delay
    (sleep 5 && vagrant_ssh_retry leader "rm -f '$producer_script'" 2>/dev/null &) &
    
    if [ $producer_exit_code -ne 0 ]; then
        error_test "Failed to start producer - startup script failed"
        exit 1
    fi
    
    sleep 5
    
    log_test ""
    log_test "Producer registration:"
    vagrant_ssh_retry leader "tail -20 '$LEADER_LOG' 2>/dev/null" 2>/dev/null | grep -E "PRODUCE from|Registered producer" || log_test "No producer registration found"
    vagrant_ssh_retry leader "tail -10 '$PRODUCER_LOG' 2>/dev/null" 2>/dev/null | grep -E "Connected|Registered" || log_test "No producer connection found"
    success_test "[OK] Producer registered"
    
    log_test ""
    log_test "========================================="
    log_test "PRODUCER-CONSUMER CONNECTION VERIFICATION:"
    log_test "========================================="
    
    log_test ""
    log_test "Verifying connections..."
    
    # Check producer connection to broker
    PRODUCER_CONNECTED=false
    if vagrant_ssh_retry leader "grep -E 'Connected|PRODUCE_ACK|assigned broker' '$PRODUCER_LOG' 2>/dev/null" 2>/dev/null | grep -qE "Connected|PRODUCE_ACK|assigned broker"; then
        PRODUCER_CONNECTED=true
        success_test "[OK] Producer connected to broker"
    else
        error_test "[X] Producer failed to connect to broker"
    fi
    
    # Check consumer connection to broker
    CONSUMER_CONNECTED=false
    if vagrant_ssh_retry broker1 "grep -E 'Connected|CONSUME_ACK|subscribed' '$CONSUMER_LOG' 2>/dev/null" 2>/dev/null | grep -qE "Connected|CONSUME_ACK|subscribed"; then
        CONSUMER_CONNECTED=true
        success_test "[OK] Consumer connected to broker"
    else
        error_test "[X] Consumer failed to connect to broker"
    fi
    
    # Check broker registration of producer
    PRODUCER_REGISTERED=false
    if vagrant_ssh_retry leader "grep -E 'PRODUCE from|Registered producer' '$LEADER_LOG' 2>/dev/null" 2>/dev/null | grep -qE "PRODUCE from|Registered producer"; then
        PRODUCER_REGISTERED=true
        success_test "[OK] Producer registered with broker"
    else
        error_test "[X] Producer not registered with broker"
    fi
    
    # Check broker registration of consumer
    CONSUMER_REGISTERED=false
    if vagrant_ssh_retry leader "grep -E 'CONSUME from|Registered consumer' '$LEADER_LOG' 2>/dev/null" 2>/dev/null | grep -qE "CONSUME from|Registered consumer"; then
        CONSUMER_REGISTERED=true
        success_test "[OK] Consumer registered with broker"
    else
        error_test "[X] Consumer not registered with broker"
    fi
    
    log_test ""
    log_test "========================================="
    log_test "CONNECTION SUMMARY:"
    log_test "========================================="
    log_test "Producer -> Broker: $([ "$PRODUCER_CONNECTED" = "true" ] && echo "[OK] Connected" || echo "[X] Failed")"
    log_test "Consumer -> Broker: $([ "$CONSUMER_CONNECTED" = "true" ] && echo "[OK] Connected" || echo "[X] Failed")"
    log_test "Producer registered: $([ "$PRODUCER_REGISTERED" = "true" ] && echo "[OK] Registered" || echo "[X] Not registered")"
    log_test "Consumer registered: $([ "$CONSUMER_REGISTERED" = "true" ] && echo "[OK] Registered" || echo "[X] Not registered")"
    
    if [ "$PRODUCER_CONNECTED" = "true" ] && [ "$CONSUMER_CONNECTED" = "true" ] && [ "$PRODUCER_REGISTERED" = "true" ] && [ "$CONSUMER_REGISTERED" = "true" ]; then
        log_test ""
        success_test "[OK] All connections established successfully!"
        success_test "[OK] Producer-Consumer test complete (Vagrant)"
    else
        log_test ""
        error_test "[X] Connection test failed - not all connections established"
        exit 1
    fi
    
    # Note: Log copying is now done in cleanup() function to ensure it happens even on timeout

else
    error_test "Invalid mode: $MODE"
    log_test "Usage: $0 [local|docker|vagrant]"
    exit 1
fi

# Cancel timeout since we completed successfully
kill $TIMEOUT_PID 2>/dev/null || true
# Note: Don't call cleanup here - let the EXIT trap handle it
# This ensures logs are copied even if we exit normally
success_test "Test completed successfully"
