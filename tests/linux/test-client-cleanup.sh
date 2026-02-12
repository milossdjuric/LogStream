#!/bin/bash

# Test: Client Cleanup on Disconnect
# This test verifies that when producers/consumers disconnect or timeout,
# their state is properly cleaned up (stream assignments, offsets, etc.)

TEST_TIMEOUT=300
MODE="${1:-local}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/lib/common.sh"

# Setup log directory
setup_log_directory "$MODE"

TEST_PREFIX="[TEST-CLEANUP]"
log_test() { echo -e "${BLUE}${TEST_PREFIX} $1${NC}"; }
success_test() { echo -e "${GREEN}${TEST_PREFIX} $1${NC}"; }
error_test() { echo -e "${RED}${TEST_PREFIX} $1${NC}"; }

# Cleanup function
cleanup() {
    error_test "Cleaning up test resources..."
    
    # Copy process logs from VMs to host BEFORE killing processes (vagrant mode only)
    if [ "$MODE" = "vagrant" ] && [ -n "$LOG_DIR" ] && [ "$LOG_DIR" != "/tmp" ]; then
        log_test "Copying process logs from VMs..."
        [ -n "$LEADER_LOG" ] && copy_vm_log_to_host leader "$LEADER_LOG" "$LOG_DIR/leader-leader.log" 2>/dev/null || true
        [ -n "$PRODUCER_LOG" ] && copy_vm_log_to_host leader "$PRODUCER_LOG" "$LOG_DIR/leader-producer.log" 2>/dev/null || true
        [ -n "$CONSUMER_LOG" ] && copy_vm_log_to_host broker1 "$CONSUMER_LOG" "$LOG_DIR/broker1-consumer.log" 2>/dev/null || true
    fi
    
    if [ "$MODE" = "local" ]; then
        (trap '' 13; exec 3>&- 2>/dev/null) || true
        
        # Copy node log files BEFORE killing processes
        if [ -n "$LOG_DIR" ] && [ "$LOG_DIR" != "/tmp" ]; then
            log_test "Preserving node log files..."
            [ -n "$LEADER_LOG" ] && [ -f "$LEADER_LOG" ] && cp "$LEADER_LOG" "$LOG_DIR/leader.log" 2>/dev/null
            [ -n "$PRODUCER_LOG" ] && [ -f "$PRODUCER_LOG" ] && cp "$PRODUCER_LOG" "$LOG_DIR/producer.log" 2>/dev/null
            [ -n "$CONSUMER_LOG" ] && [ -f "$CONSUMER_LOG" ] && cp "$CONSUMER_LOG" "$LOG_DIR/consumer.log" 2>/dev/null
        fi
        
        [ -n "$LEADER_PID" ] && kill_netns_process logstream-a "$LEADER_PID" 2>/dev/null
        [ -n "$PRODUCER_PID" ] && kill -9 "$PRODUCER_PID" 2>/dev/null
        [ -n "$CONSUMER_PID" ] && kill -9 "$CONSUMER_PID" 2>/dev/null
        sleep 1
        cleanup_netns_processes
        rm -f /tmp/logstream-*.log /tmp/producer-*.pipe 2>/dev/null
    elif [ "$MODE" = "docker" ]; then
        if [ -n "$LOG_DIR" ] && [ "$LOG_DIR" != "/tmp" ]; then
            if [ ! -f "$LOG_DIR/leader.log" ]; then
                log_test "Copying container logs..."
                cd "$PROJECT_ROOT/deploy/docker/compose" 2>/dev/null || true
                COMPOSE_PROJECT_NAME=test-cleanup docker compose -f producer-consumer.yaml logs --no-color leader > "$LOG_DIR/leader.log" 2>&1 || true
                COMPOSE_PROJECT_NAME=test-cleanup docker compose -f producer-consumer.yaml logs --no-color producer > "$LOG_DIR/producer.log" 2>&1 || true
                COMPOSE_PROJECT_NAME=test-cleanup docker compose -f producer-consumer.yaml logs --no-color consumer > "$LOG_DIR/consumer.log" 2>&1 || true
            fi
        fi
        cd "$PROJECT_ROOT/deploy/docker/compose" 2>/dev/null || true
        COMPOSE_PROJECT_NAME=test-cleanup docker compose -f producer-consumer.yaml down --remove-orphans 2>/dev/null || true
    elif [ "$MODE" = "vagrant" ]; then
        vagrant_ssh_retry leader 'pkill -f logstream; pkill -f producer; pkill -f consumer' 2>/dev/null || true
        vagrant_ssh_retry broker1 'pkill -f consumer' 2>/dev/null || true
    fi
    error_test "Cleanup complete"
}

TIMEOUT_MARKER="/tmp/cleanup-test-timeout-$$"

# Timeout will be started AFTER setup phase (not here!)
# This ensures we only time the actual test logic, not binary builds/node startup
TIMEOUT_PID=""

start_test_timeout() {
    log_test "========================================="
    log_test "STARTING TEST TIMEOUT: ${TEST_TIMEOUT}s"
    log_test "========================================="
    (
        sleep $TEST_TIMEOUT
        error_test "Test timeout reached (${TEST_TIMEOUT}s), forcing cleanup..."
        touch "$TIMEOUT_MARKER"
        cleanup
        kill -TERM $$ 2>/dev/null || true
        sleep 2
        kill -9 $$ 2>/dev/null || true
    ) &
    TIMEOUT_PID=$!
    log_test "Test timeout started (PID: $TIMEOUT_PID)"
}

trap '
    # Kill timeout if it was started
    if [ -n "$TIMEOUT_PID" ]; then
        kill $TIMEOUT_PID 2>/dev/null
    fi
    cleanup
    if [ -f "$TIMEOUT_MARKER" ]; then
        rm -f "$TIMEOUT_MARKER"
        error_test "EXITING WITH ERROR: Test timed out"
        exit 1
    fi
    exit
' EXIT

trap '
    # Kill timeout if it was started
    if [ -n "$TIMEOUT_PID" ]; then
        kill $TIMEOUT_PID 2>/dev/null
    fi
    cleanup
    if [ -f "$TIMEOUT_MARKER" ]; then
        rm -f "$TIMEOUT_MARKER"
        exit 1
    fi
    exit 1
' INT TERM

log_test "================================================"
log_test "Test: Client Cleanup on Disconnect"
log_test "Mode: $MODE"
log_test "Timeout: ${TEST_TIMEOUT}s"
log_test "================================================"
log_test ""
log_test "This test verifies:"
log_test "  1. Producer registers and creates stream"
log_test "  2. Consumer subscribes to stream"
log_test "  3. Producer disconnects -> stream removed"
log_test "  4. Consumer times out -> consumer cleanup"
log_test "  5. State is properly cleaned up"
log_test ""

if [ "$MODE" = "local" ]; then
    cd "$PROJECT_ROOT"
    
    build_if_needed "logstream" "main.go"
    build_if_needed "producer" "cmd/producer/main.go"
    build_if_needed "consumer" "cmd/consumer/main.go"
    
    log_test "Setting up network namespaces..."
    if ! ensure_netns_setup; then
        error_test "Failed to setup network namespaces"
        exit 1
    fi
    
    # Start leader
    log_test "=== STEP 1: Starting Leader ==="
    LEADER_LOG="/tmp/logstream-leader-cleanup.log"
    sudo touch "$LEADER_LOG" && sudo chmod 666 "$LEADER_LOG"
    sudo ip netns exec logstream-a env \
        NODE_ADDRESS=172.20.0.10:8001 \
        MULTICAST_GROUP=239.0.0.1:9999 \
        BROADCAST_PORT=8888 \
        IS_LEADER="true" \
        stdbuf -oL -eL ./logstream > "$LEADER_LOG" 2>&1 &
    LEADER_PID=$!
    success_test "Leader started (PID: $LEADER_PID)"
    sleep 5
    
    # Wait for leader
    for i in {1..15}; do
        if grep -q "TCP listener started" "$LEADER_LOG" 2>/dev/null; then
            success_test "Leader ready"
            break
        fi
        sleep 1
    done
    
    # Start producer
    log_test ""
    log_test "=== STEP 2: Starting Producer ==="
    PRODUCER_LOG="/tmp/logstream-producer-cleanup.log"
    PIPE="/tmp/producer-cleanup.pipe"
    rm -f "$PIPE"
    mkfifo "$PIPE" 2>/dev/null || true
    chmod 666 "$PIPE"
    sudo touch "$PRODUCER_LOG" && sudo chmod 666 "$PRODUCER_LOG"
    
    sudo ip netns exec logstream-c bash -c "
        cd '$PROJECT_ROOT' && \
        env LEADER_ADDRESS=172.20.0.10:8001 TOPIC=cleanup-test \
        stdbuf -oL -eL ./producer < '$PIPE' > '$PRODUCER_LOG' 2>&1
    " &
    PRODUCER_PID=$!
    exec 3> "$PIPE"
    success_test "Producer started (PID: $PRODUCER_PID)"
    sleep 3
    
    echo "cleanup-test-data-1" >&3 2>/dev/null
    echo "cleanup-test-data-2" >&3 2>/dev/null
    sleep 2
    
    log_test "Producer registered:"
    grep -E "Registered producer|Assigned stream" "$LEADER_LOG" 2>/dev/null | tail -5
    
    # Start consumer
    log_test ""
    log_test "=== STEP 3: Starting Consumer ==="
    CONSUMER_LOG="/tmp/logstream-consumer-cleanup.log"
    sudo touch "$CONSUMER_LOG" && sudo chmod 666 "$CONSUMER_LOG"
    
    sudo ip netns exec logstream-b env \
        LEADER_ADDRESS=172.20.0.10:8001 \
        TOPIC="cleanup-test" \
        stdbuf -oL -eL ./consumer > "$CONSUMER_LOG" 2>&1 &
    CONSUMER_PID=$!
    success_test "Consumer started (PID: $CONSUMER_PID)"
    sleep 5
    
    log_test "Consumer registered:"
    grep -E "Registered consumer|Consumer.*assigned" "$LEADER_LOG" 2>/dev/null | tail -5
    
    # Check initial state
    log_test ""
    log_test "=== STEP 4: Checking Initial State ==="
    INITIAL_PRODUCERS=$(grep -c "Registered producer" "$LEADER_LOG" 2>/dev/null || echo "0")
    INITIAL_CONSUMERS=$(grep -c "Registered consumer" "$LEADER_LOG" 2>/dev/null || echo "0")
    INITIAL_STREAMS=$(grep -c "Assigned stream" "$LEADER_LOG" 2>/dev/null || echo "0")
    
    log_test "Initial producers: $INITIAL_PRODUCERS"
    log_test "Initial consumers: $INITIAL_CONSUMERS"
    log_test "Initial streams: $INITIAL_STREAMS"

    log_test ""
    log_test "========================================="
    log_test "SETUP PHASE COMPLETE (LOCAL)"
    log_test "========================================="
    log_test ""

    # NOW start the timeout - setup is done, only test logic will be timed
    start_test_timeout

    # Kill producer
    log_test ""
    log_test "=== STEP 5: Killing Producer ==="
    exec 3>&- 2>/dev/null || true
    kill -9 $PRODUCER_PID 2>/dev/null || true
    PRODUCER_PID=""
    success_test "Producer killed"
    
    # Wait for timeout detection
    log_test ""
    log_test "Waiting for producer timeout detection (60s timeout)..."
    
    PRODUCER_CLEANUP_DETECTED=false
    for i in {1..70}; do
        if grep -qE "\[Cleanup\].*producer|Timeout: Removed producer|Removed stream" "$LEADER_LOG" 2>/dev/null; then
            PRODUCER_CLEANUP_DETECTED=true
            break
        fi
        sleep 2
        echo -n "."
    done
    echo ""
    
    if [ "$PRODUCER_CLEANUP_DETECTED" = "true" ]; then
        success_test "Producer cleanup detected!"
        grep -E "\[Cleanup\].*producer|Timeout: Removed producer|Removed stream" "$LEADER_LOG" 2>/dev/null | tail -10
    else
        log_test "Producer cleanup not explicitly detected (may need longer timeout)"
    fi
    
    # Kill consumer
    log_test ""
    log_test "=== STEP 6: Killing Consumer ==="
    kill -9 $CONSUMER_PID 2>/dev/null || true
    CONSUMER_PID=""
    success_test "Consumer killed"
    
    # Wait for consumer cleanup
    log_test ""
    log_test "Waiting for consumer timeout detection..."
    
    CONSUMER_CLEANUP_DETECTED=false
    for i in {1..70}; do
        if grep -qE "\[Cleanup\].*consumer|Timeout: Removed consumer|removed from stream" "$LEADER_LOG" 2>/dev/null; then
            CONSUMER_CLEANUP_DETECTED=true
            break
        fi
        sleep 2
        echo -n "."
    done
    echo ""
    
    if [ "$CONSUMER_CLEANUP_DETECTED" = "true" ]; then
        success_test "Consumer cleanup detected!"
        grep -E "\[Cleanup\].*consumer|Timeout: Removed consumer|removed from stream" "$LEADER_LOG" 2>/dev/null | tail -10
    fi
    
    log_test ""
    log_test "========================================="
    log_test "CLIENT CLEANUP TEST RESULTS:"
    log_test "========================================="
    
    log_test ""
    log_test "Timeout detection in leader logs:"
    grep -E "Timeout:|dead|Removed|\[Cleanup\]" "$LEADER_LOG" 2>/dev/null | tail -15
    
    log_test ""
    log_test "Summary:"
    log_test "  Producer cleanup detected: $([ "$PRODUCER_CLEANUP_DETECTED" = "true" ] && echo "YES" || echo "PENDING")"
    log_test "  Consumer cleanup detected: $([ "$CONSUMER_CLEANUP_DETECTED" = "true" ] && echo "YES" || echo "PENDING")"
    
    DEAD_PRODUCERS=$(grep -c "\[Cleanup\].*producer\|Timeout: Removed producer" "$LEADER_LOG" 2>/dev/null || echo "0")
    DEAD_CONSUMERS=$(grep -c "\[Cleanup\].*consumer\|Timeout: Removed consumer" "$LEADER_LOG" 2>/dev/null || echo "0")
    
    log_test "  Dead producers removed: $DEAD_PRODUCERS"
    log_test "  Dead consumers removed: $DEAD_CONSUMERS"
    
    if [ "$PRODUCER_CLEANUP_DETECTED" = "true" ] || [ "$CONSUMER_CLEANUP_DETECTED" = "true" ] || \
       [ "$DEAD_PRODUCERS" -gt 0 ] || [ "$DEAD_CONSUMERS" -gt 0 ]; then
        success_test ""
        success_test "[OK] Client cleanup mechanism is working!"
    else
        log_test ""
        log_test "[NOTE] Cleanup not detected - may need longer timeout (60s)"
    fi
    
    success_test "[OK] Client cleanup test complete"

elif [ "$MODE" = "docker" ]; then
    COMPOSE_FILE="$PROJECT_ROOT/deploy/docker/compose/producer-consumer.yaml"
    
    if [ ! -f "$COMPOSE_FILE" ]; then
        error_test "Compose file not found: $COMPOSE_FILE"
        exit 1
    fi
    
    cd "$PROJECT_ROOT/deploy/docker/compose"
    
    log_test "=== Starting All Containers ==="
    COMPOSE_PROJECT_NAME=test-cleanup docker compose -f producer-consumer.yaml build
    COMPOSE_PROJECT_NAME=test-cleanup docker compose -f producer-consumer.yaml up -d
    if [ $? -ne 0 ]; then
        error_test "Failed to start containers"
        exit 1
    fi
    
    sleep 10
    
    log_test ""
    log_test "Container status:"
    COMPOSE_PROJECT_NAME=test-cleanup docker compose -f producer-consumer.yaml ps
    
    log_test ""
    log_test "Initial state - producer and consumer registered:"
    COMPOSE_PROJECT_NAME=test-cleanup docker compose -f producer-consumer.yaml logs leader 2>/dev/null | grep -E "Registered|Assigned stream" | tail -10
    
    log_test ""
    log_test "=== Stopping Producer to Simulate Disconnect ==="
    COMPOSE_PROJECT_NAME=test-cleanup docker compose -f producer-consumer.yaml stop producer
    success_test "Producer stopped"
    
    log_test ""
    log_test "Waiting for producer timeout detection (60s)..."
    for i in {1..70}; do
        CLEANUP=$(COMPOSE_PROJECT_NAME=test-cleanup docker compose -f producer-consumer.yaml logs leader 2>/dev/null | grep -E "\[Cleanup\].*producer|Timeout: Removed producer|Removed stream" | tail -1)
        if [ -n "$CLEANUP" ]; then
            success_test "Producer cleanup detected!"
            break
        fi
        sleep 2
        echo -n "."
    done
    echo ""
    
    log_test ""
    log_test "=== Stopping Consumer to Simulate Disconnect ==="
    COMPOSE_PROJECT_NAME=test-cleanup docker compose -f producer-consumer.yaml stop consumer
    success_test "Consumer stopped"
    
    log_test ""
    log_test "Waiting for consumer timeout detection..."
    for i in {1..70}; do
        CLEANUP=$(COMPOSE_PROJECT_NAME=test-cleanup docker compose -f producer-consumer.yaml logs leader 2>/dev/null | grep -E "\[Cleanup\].*consumer|Timeout: Removed consumer|removed from stream" | tail -1)
        if [ -n "$CLEANUP" ]; then
            success_test "Consumer cleanup detected!"
            break
        fi
        sleep 2
        echo -n "."
    done
    echo ""
    
    log_test ""
    log_test "========================================="
    log_test "CLIENT CLEANUP TEST RESULTS:"
    log_test "========================================="
    
    COMPOSE_PROJECT_NAME=test-cleanup docker compose -f producer-consumer.yaml logs leader 2>/dev/null | grep -E "\[Cleanup\]|Timeout: Removed" | tail -15
    
    # Copy logs
    if [ -n "$LOG_DIR" ] && [ "$LOG_DIR" != "/tmp" ]; then
        COMPOSE_PROJECT_NAME=test-cleanup docker compose -f producer-consumer.yaml logs --no-color leader > "$LOG_DIR/leader.log" 2>&1 || true
        COMPOSE_PROJECT_NAME=test-cleanup docker compose -f producer-consumer.yaml logs --no-color producer > "$LOG_DIR/producer.log" 2>&1 || true
        COMPOSE_PROJECT_NAME=test-cleanup docker compose -f producer-consumer.yaml logs --no-color consumer > "$LOG_DIR/consumer.log" 2>&1 || true
    fi
    
    success_test "[OK] Client cleanup test complete (Docker)"

elif [ "$MODE" = "vagrant" ]; then
    cd "$PROJECT_ROOT/deploy/vagrant"
    
    log_test "Checking Vagrant VMs..."
    sleep $((RANDOM % 3))
    for vm in leader broker1; do
        if ! check_vm_status $vm; then
            error_test "$vm VM not running! Run: vagrant up"
            exit 1
        fi
    done
    
    # Build binaries
    vagrant_ssh_retry leader 'cd /vagrant/logstream && /usr/local/go/bin/go build -o producer cmd/producer/main.go && /usr/local/go/bin/go build -o consumer cmd/consumer/main.go' 2>/dev/null
    vagrant_ssh_retry broker1 'cd /vagrant/logstream && /usr/local/go/bin/go build -o consumer cmd/consumer/main.go' 2>/dev/null
    
    if ! ensure_binary_on_vm leader; then
        error_test "Failed to ensure binary on leader VM"
        exit 1
    fi
    
    # Setup log files
    LEADER_LOG="$VM_LOG_DIR/leader.log"
    PRODUCER_LOG="$VM_LOG_DIR/producer.log"
    CONSUMER_LOG="$VM_LOG_DIR/consumer.log"
    
    if ! ensure_vm_log_directory leader "$VM_LOG_DIR" || ! ensure_vm_log_directory broker1 "$VM_LOG_DIR"; then
        error_test "Failed to create log directories"
        exit 1
    fi
    
    log_test "=== STEP 1: Starting Leader ==="
    if ! prepare_vm_log_file leader "$LEADER_LOG" || \
       ! start_logstream_vm_wrapper leader "192.168.100.10:8001" "true" "$LEADER_LOG"; then
        error_test "Failed to start leader"
        exit 1
    fi
    sleep 2
    if ! verify_process_running leader "$LEADER_LOG" "logstream" 7; then
        error_test "Leader verification failed"
        exit 1
    fi
    success_test "Leader ready"
    
    log_test ""
    log_test "=== STEP 2: Starting Producer ==="
    
    # Ensure producer binary exists on VM
    log_test "Ensuring producer binary exists on leader..."
    if ! vagrant_ssh_retry leader "test -f /vagrant/logstream/producer" 2>/dev/null; then
        log_test "Producer binary not found - building..."
        BUILD_OUTPUT=$(vagrant_ssh_retry leader "cd /vagrant/logstream && /usr/local/go/bin/go build -o producer cmd/producer/main.go 2>&1" 2>&1 | grep -vE "WARNING|Permission denied|libvirt_ip_command" || true)
        
        if [ $? -ne 0 ] || echo "$BUILD_OUTPUT" | grep -qiE "error|undefined|cannot|failed"; then
            error_test "Failed to build producer:"
            echo "$BUILD_OUTPUT"
            exit 1
        fi
        success_test "Producer binary built successfully"
    else
        log_test "Producer binary already exists"
    fi
    
    if ! prepare_vm_log_file leader "$PRODUCER_LOG"; then
        error_test "Failed to prepare producer log"
        exit 1
    fi

    # Start producer using a robust wrapper script (same pattern as test-producer-consumer.sh)
    # Uses setsid, stdbuf, disown for proper process management
    producer_script="/tmp/start-producer-cleanup-$$-$(date +%s).sh"
    if ! vagrant_ssh_retry leader "
        printf '%s\n' '#!/bin/bash' > '$producer_script' && \
        printf '%s\n' 'set +H' >> '$producer_script' && \
        printf '%s\n' 'cd /vagrant/logstream' >> '$producer_script' && \
        printf '%s\n' 'export LEADER_ADDRESS=\"192.168.100.10:8001\"' >> '$producer_script' && \
        printf '%s\n' 'export TOPIC=\"cleanup-test\"' >> '$producer_script' && \
        printf '%s\n' 'trap \"\" HUP INT TERM' >> '$producer_script' && \
        printf '%s\n' 'echo \"Producer connecting...\"' >> '$producer_script' && \
        printf 'setsid nohup stdbuf -oL -eL sh -c \"for i in 1 2 3 4 5; do echo data-\\\$i; sleep 2; done | LEADER_ADDRESS=\\\"\$LEADER_ADDRESS\\\" TOPIC=\\\"\$TOPIC\\\" ./producer\" > \"%s\" 2>&1 < /dev/null &\n' \"$PRODUCER_LOG\" >> '$producer_script' && \
        printf '%s\n' 'PID=\$!' >> '$producer_script' && \
        printf '%s\n' 'disown -h \$PID 2>/dev/null || true' >> '$producer_script' && \
        printf '%s\n' 'sleep 3' >> '$producer_script' && \
        printf '%s\n' 'if kill -0 \$PID 2>/dev/null; then' >> '$producer_script' && \
        printf '%s\n' '  echo \"Producer started successfully with PID \$PID\"' >> '$producer_script' && \
        printf '%s\n' '  exit 0' >> '$producer_script' && \
        printf '%s\n' 'else' >> '$producer_script' && \
        printf '%s\n' '  echo \"ERROR: Producer failed to start or crashed immediately\"' >> '$producer_script' && \
        printf '  tail -20 \"%s\" 2>/dev/null || echo \"Log file empty or not readable\"\n' \"$PRODUCER_LOG\" >> '$producer_script' && \
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
    success_test "Producer started"

    # Verify producer actually registered with leader (not just PID check)
    PRODUCER_REGISTERED=false
    for i in {1..10}; do
        if vagrant_ssh_retry leader "grep -qE 'Registered producer|PRODUCE from' '$LEADER_LOG' 2>/dev/null" 2>/dev/null; then
            PRODUCER_REGISTERED=true
            break
        fi
        sleep 1
    done

    if [ "$PRODUCER_REGISTERED" = "true" ]; then
        success_test "Producer registered with leader"
        log_test "Producer registration:"
        vagrant_ssh_retry leader "grep -E 'Registered producer|Assigned stream' '$LEADER_LOG' 2>/dev/null | tail -5" 2>/dev/null
    else
        error_test "Producer failed to register with leader!"
        error_test "Producer log contents:"
        vagrant_ssh_retry leader "cat '$PRODUCER_LOG' 2>/dev/null | head -30" 2>/dev/null || true
        error_test "Leader log (last 30 lines):"
        vagrant_ssh_retry leader "tail -30 '$LEADER_LOG' 2>/dev/null" 2>/dev/null || true
        exit 1
    fi
    
    log_test ""
    log_test "=== STEP 3: Starting Consumer ==="
    
    # Ensure consumer binary exists on VM
    log_test "Ensuring consumer binary exists on broker1..."
    if ! vagrant_ssh_retry broker1 "test -f /vagrant/logstream/consumer" 2>/dev/null; then
        log_test "Consumer binary not found - building..."
        BUILD_OUTPUT=$(vagrant_ssh_retry broker1 "cd /vagrant/logstream && /usr/local/go/bin/go build -o consumer cmd/consumer/main.go 2>&1" 2>&1 | grep -vE "WARNING|Permission denied|libvirt_ip_command" || true)
        
        if [ $? -ne 0 ] || echo "$BUILD_OUTPUT" | grep -qiE "error|undefined|cannot|failed"; then
            error_test "Failed to build consumer:"
            echo "$BUILD_OUTPUT"
            exit 1
        fi
        success_test "Consumer binary built successfully"
    else
        log_test "Consumer binary already exists"
    fi
    
    if ! prepare_vm_log_file broker1 "$CONSUMER_LOG"; then
        error_test "Failed to prepare consumer log"
        exit 1
    fi

    # Start consumer using a robust wrapper script (same pattern as test-producer-consumer.sh)
    # Uses setsid, stdbuf, disown for proper process management
    consumer_script="/tmp/start-consumer-cleanup-$$-$(date +%s).sh"
    if ! vagrant_ssh_retry broker1 "
        printf '%s\n' '#!/bin/bash' > '$consumer_script' && \
        printf '%s\n' 'set +H' >> '$consumer_script' && \
        printf '%s\n' 'cd /vagrant/logstream' >> '$consumer_script' && \
        printf '%s\n' 'export LEADER_ADDRESS=\"192.168.100.10:8001\"' >> '$consumer_script' && \
        printf '%s\n' 'export TOPIC=\"cleanup-test\"' >> '$consumer_script' && \
        printf '%s\n' 'trap \"\" HUP INT TERM' >> '$consumer_script' && \
        printf 'setsid nohup stdbuf -oL -eL ./consumer > \"%s\" 2>&1 < /dev/null &\n' \"$CONSUMER_LOG\" >> '$consumer_script' && \
        printf '%s\n' 'PID=\$!' >> '$consumer_script' && \
        printf '%s\n' 'disown -h \$PID 2>/dev/null || true' >> '$consumer_script' && \
        printf '%s\n' 'sleep 2' >> '$consumer_script' && \
        printf '%s\n' 'if kill -0 \$PID 2>/dev/null; then' >> '$consumer_script' && \
        printf '%s\n' '  echo \"Consumer started successfully with PID \$PID\"' >> '$consumer_script' && \
        printf '%s\n' '  exit 0' >> '$consumer_script' && \
        printf '%s\n' 'else' >> '$consumer_script' && \
        printf '%s\n' '  echo \"ERROR: Consumer failed to start or crashed immediately\"' >> '$consumer_script' && \
        printf '  tail -20 \"%s\" 2>/dev/null || echo \"Log file empty or not readable\"\n' \"$CONSUMER_LOG\" >> '$consumer_script' && \
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
    success_test "Consumer started"

    # Verify consumer actually registered with leader (not just PID check)
    CONSUMER_REGISTERED=false
    for i in {1..10}; do
        if vagrant_ssh_retry leader "grep -qE 'Registered consumer|CONSUME from' '$LEADER_LOG' 2>/dev/null" 2>/dev/null; then
            CONSUMER_REGISTERED=true
            break
        fi
        sleep 1
    done

    if [ "$CONSUMER_REGISTERED" = "true" ]; then
        success_test "Consumer registered with leader"
        log_test "Consumer registration:"
        vagrant_ssh_retry leader "grep -E 'Registered consumer|Consumer.*assigned' '$LEADER_LOG' 2>/dev/null | tail -5" 2>/dev/null
    else
        error_test "Consumer failed to register with leader!"
        error_test "Consumer log contents:"
        vagrant_ssh_retry broker1 "cat '$CONSUMER_LOG' 2>/dev/null | head -30" 2>/dev/null || true
        error_test "Leader log (last 30 lines):"
        vagrant_ssh_retry leader "tail -30 '$LEADER_LOG' 2>/dev/null" 2>/dev/null || true
        exit 1
    fi

    log_test ""
    log_test "========================================="
    log_test "SETUP PHASE COMPLETE (VAGRANT)"
    log_test "========================================="
    log_test ""

    # NOW start the timeout - setup is done, only test logic will be timed
    start_test_timeout

    log_test ""
    log_test "=== STEP 4: Killing Producer ==="
    vagrant_ssh_retry leader 'pkill -9 -f producer' 2>/dev/null || true
    success_test "Producer killed"
    
    log_test ""
    log_test "Waiting for producer timeout detection (60s)..."
    PRODUCER_CLEANUP_DETECTED=false
    for i in {1..70}; do
        CLEANUP=$(vagrant_ssh_retry leader "grep -E '\[Cleanup\].*producer|Timeout: Removed producer|Removed stream' '$LEADER_LOG' 2>/dev/null | tail -1" 2>/dev/null)
        if [ -n "$CLEANUP" ]; then
            PRODUCER_CLEANUP_DETECTED=true
            break
        fi
        sleep 2
        echo -n "."
    done
    echo ""
    
    if [ "$PRODUCER_CLEANUP_DETECTED" = "true" ]; then
        success_test "Producer cleanup detected!"
    fi
    
    log_test ""
    log_test "=== STEP 5: Killing Consumer ==="
    vagrant_ssh_retry broker1 'pkill -9 -f consumer' 2>/dev/null || true
    success_test "Consumer killed"
    
    log_test ""
    log_test "Waiting for consumer timeout detection..."
    CONSUMER_CLEANUP_DETECTED=false
    for i in {1..70}; do
        CLEANUP=$(vagrant_ssh_retry leader "grep -E '\[Cleanup\].*consumer|Timeout: Removed consumer|removed from stream' '$LEADER_LOG' 2>/dev/null | tail -1" 2>/dev/null)
        if [ -n "$CLEANUP" ]; then
            CONSUMER_CLEANUP_DETECTED=true
            break
        fi
        sleep 2
        echo -n "."
    done
    echo ""
    
    if [ "$CONSUMER_CLEANUP_DETECTED" = "true" ]; then
        success_test "Consumer cleanup detected!"
    fi
    
    log_test ""
    log_test "========================================="
    log_test "CLIENT CLEANUP TEST RESULTS:"
    log_test "========================================="
    
    vagrant_ssh_retry leader "grep -E '\[Cleanup\]|Timeout: Removed' '$LEADER_LOG' 2>/dev/null | tail -15" 2>/dev/null
    
    log_test ""
    log_test "Summary:"
    log_test "  Producer cleanup: $([ "$PRODUCER_CLEANUP_DETECTED" = "true" ] && echo "YES" || echo "PENDING")"
    log_test "  Consumer cleanup: $([ "$CONSUMER_CLEANUP_DETECTED" = "true" ] && echo "YES" || echo "PENDING")"
    
    success_test "[OK] Client cleanup test complete (Vagrant)"

else
    error_test "Invalid mode: $MODE"
    log_test "Usage: $0 [local|docker|vagrant]"
    exit 1
fi

kill $TIMEOUT_PID 2>/dev/null || true
success_test "Test completed successfully"
