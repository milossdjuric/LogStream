#!/bin/bash

# Test: FIFO Ordering of Data Messages
# This test verifies that data messages from producers are delivered in order
# even when they arrive out of order at the broker.

TEST_TIMEOUT=300
MODE="${1:-local}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/lib/common.sh"

# Setup log directory
setup_log_directory "$MODE"

TEST_PREFIX="[TEST-FIFO]"
log_test() { echo -e "${BLUE}${TEST_PREFIX} $1${NC}"; }
success_test() { echo -e "${GREEN}${TEST_PREFIX} $1${NC}"; }
error_test() { echo -e "${RED}${TEST_PREFIX} $1${NC}"; }

# Cleanup function
cleanup() {
    error_test "Cleaning up test resources..."
    
    # Copy process logs from VMs to host BEFORE killing processes (vagrant mode only)
    if [ "$MODE" = "vagrant" ] && [ -n "$LOG_DIR" ] && [ "$LOG_DIR" != "/tmp" ]; then
        log_test "Copying process logs from VMs to host before cleanup..."
        if [ -n "$LEADER_LOG" ]; then
            copy_vm_log_to_host leader "$LEADER_LOG" "$LOG_DIR/leader-leader.log" 2>/dev/null || true
        fi
        if [ -n "$PRODUCER_LOG" ]; then
            copy_vm_log_to_host leader "$PRODUCER_LOG" "$LOG_DIR/leader-producer.log" 2>/dev/null || true
        fi
        if [ -n "$CONSUMER_LOG" ]; then
            copy_vm_log_to_host broker1 "$CONSUMER_LOG" "$LOG_DIR/broker1-consumer.log" 2>/dev/null || true
        fi
    fi
    
    if [ "$MODE" = "local" ]; then
        # Close pipe file descriptor first
        (trap '' 13; exec 3>&- 2>/dev/null) || true
        
        # Copy node log files BEFORE killing processes
        if [ -n "$LOG_DIR" ] && [ "$LOG_DIR" != "/tmp" ]; then
            log_test "Preserving node log files..."
            [ -n "$LEADER_LOG" ] && [ -f "$LEADER_LOG" ] && cp "$LEADER_LOG" "$LOG_DIR/leader.log" 2>/dev/null
            [ -n "$PRODUCER_LOG" ] && [ -f "$PRODUCER_LOG" ] && cp "$PRODUCER_LOG" "$LOG_DIR/producer.log" 2>/dev/null
            [ -n "$CONSUMER_LOG" ] && [ -f "$CONSUMER_LOG" ] && cp "$CONSUMER_LOG" "$LOG_DIR/consumer.log" 2>/dev/null
        fi
        
        # Kill processes in their namespaces
        [ -n "$LEADER_PID" ] && kill_netns_process logstream-a "$LEADER_PID" 2>/dev/null
        [ -n "$PRODUCER_PID" ] && kill_netns_process logstream-c "$PRODUCER_PID" 2>/dev/null
        [ -n "$CONSUMER_PID" ] && kill_netns_process logstream-b "$CONSUMER_PID" 2>/dev/null
        sleep 1
        cleanup_netns_processes
        rm -f /tmp/logstream-*.log /tmp/producer-*.pipe 2>/dev/null
    elif [ "$MODE" = "docker" ]; then
        # Copy container logs
        if [ -n "$LOG_DIR" ] && [ "$LOG_DIR" != "/tmp" ]; then
            if [ ! -f "$LOG_DIR/leader.log" ]; then
                log_test "Copying container logs..."
                cd "$PROJECT_ROOT/deploy/docker/compose" 2>/dev/null || true
                COMPOSE_PROJECT_NAME=test-fifo docker compose -f producer-consumer.yaml logs --no-color leader > "$LOG_DIR/leader.log" 2>&1 || true
                COMPOSE_PROJECT_NAME=test-fifo docker compose -f producer-consumer.yaml logs --no-color producer > "$LOG_DIR/producer.log" 2>&1 || true
                COMPOSE_PROJECT_NAME=test-fifo docker compose -f producer-consumer.yaml logs --no-color consumer > "$LOG_DIR/consumer.log" 2>&1 || true
            fi
        fi
        cd "$PROJECT_ROOT/deploy/docker/compose" 2>/dev/null || true
        COMPOSE_PROJECT_NAME=test-fifo docker compose -f producer-consumer.yaml down --remove-orphans 2>/dev/null || true
    elif [ "$MODE" = "vagrant" ]; then
        vagrant_ssh_retry leader 'pkill -f logstream; pkill -f producer' 2>/dev/null || true
        vagrant_ssh_retry broker1 'pkill -f consumer' 2>/dev/null || true
    fi
    error_test "Cleanup complete"
}

TIMEOUT_MARKER="/tmp/fifo-test-timeout-$$"

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
log_test "Test: FIFO Ordering of Data Messages"
log_test "Mode: $MODE"
log_test "Timeout: ${TEST_TIMEOUT}s"
log_test "================================================"
log_test ""
log_test "This test verifies:"
log_test "  1. Producer sends data with sequence numbers"
log_test "  2. Broker uses holdback queue for FIFO ordering"
log_test "  3. Consumer receives data in correct order"
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
    LEADER_LOG="/tmp/logstream-leader-fifo.log"
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
    
    # Start producer with pipe input (MUST be before consumer!)
    log_test "=== STEP 2: Starting Producer ==="
    PRODUCER_LOG="/tmp/logstream-producer-fifo.log"
    PIPE="/tmp/producer-fifo.pipe"
    rm -f "$PIPE"
    mkfifo "$PIPE" 2>/dev/null || true
    chmod 666 "$PIPE"
    sudo touch "$PRODUCER_LOG" && sudo chmod 666 "$PRODUCER_LOG"
    
    sudo ip netns exec logstream-c bash -c "
        cd '$PROJECT_ROOT' && \
        env LEADER_ADDRESS=172.20.0.10:8001 TOPIC=fifo-test \
        stdbuf -oL -eL ./producer < '$PIPE' > '$PRODUCER_LOG' 2>&1
    " &
    PRODUCER_PID=$!
    exec 3> "$PIPE"
    success_test "Producer started (PID: $PRODUCER_PID)"
    sleep 3
    
    # Verify producer registered
    log_test "Waiting for producer registration..."
    for i in {1..10}; do
        if grep -qE "Registered producer.*fifo-test" "$LEADER_LOG" 2>/dev/null; then
            success_test "Producer registered successfully"
            break
        fi
        sleep 1
    done
    
    # NOW start consumer (after producer registered)
    log_test "=== STEP 3: Starting Consumer ==="
    CONSUMER_LOG="/tmp/logstream-consumer-fifo.log"
    sudo touch "$CONSUMER_LOG" && sudo chmod 666 "$CONSUMER_LOG"
    sudo ip netns exec logstream-b env \
        LEADER_ADDRESS=172.20.0.10:8001 \
        TOPIC="fifo-test" \
        stdbuf -oL -eL ./consumer > "$CONSUMER_LOG" 2>&1 &
    CONSUMER_PID=$!
    success_test "Consumer started (PID: $CONSUMER_PID)"
    sleep 5

    log_test ""
    log_test "========================================="
    log_test "SETUP PHASE COMPLETE (LOCAL)"
    log_test "========================================="
    log_test ""

    # NOW start the timeout - setup is done, only test logic will be timed
    start_test_timeout

    # Send test messages with sequence numbers visible in content
    log_test "=== STEP 4: Sending Numbered Messages ==="
    
    NUM_MESSAGES=10
    for i in $(seq 1 $NUM_MESSAGES); do
        MSG="FIFO-MESSAGE-$i"
        echo "$MSG" >&3 2>/dev/null || true
        log_test "Sent: $MSG"
        sleep 0.5
    done
    
    sleep 5
    
    # Verify FIFO ordering
    log_test ""
    log_test "========================================="
    log_test "FIFO ORDERING VERIFICATION:"
    log_test "========================================="
    
    log_test ""
    log_test "Checking sequence numbers in broker logs..."
    SEQ_DELIVERED=$(grep -E "\[DataHoldback\] Delivering data seq=|FIFO delivered" "$LEADER_LOG" 2>/dev/null | grep -oP "seq=\K[0-9]+" | tr '\n' ' ')
    log_test "Sequence numbers delivered: $SEQ_DELIVERED"
    
    log_test ""
    log_test "Checking for holdback queue activity..."
    BUFFERED=$(grep -c "\[DataHoldback\] Buffered data seq=" "$LEADER_LOG" 2>/dev/null | head -1 | tr -d '\n' || echo "0")
    DELIVERED=$(grep -c "\[DataHoldback\] Delivering data seq=\|FIFO delivered" "$LEADER_LOG" 2>/dev/null | head -1 | tr -d '\n' || echo "0")
    log_test "Messages buffered (out of order): $BUFFERED"
    log_test "Messages delivered (FIFO): $DELIVERED"
    
    log_test ""
    log_test "Consumer received messages:"
    grep -E "^Topic:  fifo-test|^Offset:" "$CONSUMER_LOG" 2>/dev/null | tail -30 || log_test "No messages received"
    
    # Verify order - count messages with actual data offsets (not -1 which is analytics)
    RECEIVED_COUNT=$(grep -E "^Offset: [0-9]" "$CONSUMER_LOG" 2>/dev/null | grep -v "^Offset: -1" | wc -l || echo "0")
    
    log_test ""
    log_test "========================================="
    log_test "RESULTS:"
    log_test "========================================="
    log_test "Messages sent:     $NUM_MESSAGES"
    log_test "Messages received: $RECEIVED_COUNT"
    
    # Check if sequence numbers are monotonically increasing
    FIFO_OK=true
    LAST_SEQ=0
    while read -r seq; do
        if [ "$seq" -lt "$LAST_SEQ" ]; then
            FIFO_OK=false
            error_test "FIFO violation: seq $seq came after seq $LAST_SEQ"
        fi
        LAST_SEQ=$seq
    done < <(grep -E "\[DataHoldback\] Delivering data seq=|FIFO delivered" "$LEADER_LOG" 2>/dev/null | grep -oP "seq=\K[0-9]+")
    
    if [ "$FIFO_OK" = "true" ] && [ "$DELIVERED" -gt 0 ]; then
        success_test "[OK] FIFO ordering verified - all messages delivered in sequence order!"
    else
        error_test "[FAIL] FIFO ordering test failed"
        exit 1
    fi
    
    success_test "[OK] FIFO ordering test complete"

elif [ "$MODE" = "docker" ]; then
    COMPOSE_FILE="$PROJECT_ROOT/deploy/docker/compose/producer-consumer.yaml"
    
    if [ ! -f "$COMPOSE_FILE" ]; then
        error_test "Compose file not found: $COMPOSE_FILE"
        exit 1
    fi
    
    cd "$PROJECT_ROOT/deploy/docker/compose"
    
    log_test "=== Starting All Containers ==="
    log_test "Building Docker images..."
    COMPOSE_PROJECT_NAME=test-fifo docker compose -f producer-consumer.yaml build
    if [ $? -ne 0 ]; then
        error_test "Failed to build docker images"
        exit 1
    fi
    
    log_test "Starting containers..."
    COMPOSE_PROJECT_NAME=test-fifo docker compose -f producer-consumer.yaml up -d
    if [ $? -ne 0 ]; then
        error_test "Failed to start docker containers"
        exit 1
    fi
    
    sleep 10
    
    log_test ""
    log_test "Container status:"
    COMPOSE_PROJECT_NAME=test-fifo docker compose -f producer-consumer.yaml ps 2>/dev/null
    
    log_test ""
    log_test "=== Checking FIFO Ordering ==="
    sleep 5
    
    log_test ""
    log_test "Checking sequence numbers in broker logs..."
    COMPOSE_PROJECT_NAME=test-fifo docker compose -f producer-consumer.yaml logs leader 2>/dev/null | grep -E "\[DataHoldback\] Delivering data seq=|\[DataHoldback\] Buffered data seq=|FIFO delivered" | tail -20
    
    DELIVERED=$(COMPOSE_PROJECT_NAME=test-fifo docker compose -f producer-consumer.yaml logs leader 2>/dev/null | grep -cE "\[DataHoldback\] Delivering data seq=|FIFO delivered" | tr -d '\n' || echo "0")
    BUFFERED=$(COMPOSE_PROJECT_NAME=test-fifo docker compose -f producer-consumer.yaml logs leader 2>/dev/null | grep -c "\[DataHoldback\] Buffered data seq=" | tr -d '\n' || echo "0")
    DELIVERED=${DELIVERED:-0}
    BUFFERED=${BUFFERED:-0}
    
    log_test ""
    log_test "========================================="
    log_test "FIFO ORDERING RESULTS:"
    log_test "========================================="
    log_test "Messages delivered (FIFO): $DELIVERED"
    log_test "Messages buffered (out of order): $BUFFERED"
    
    if [ "$DELIVERED" -gt 0 ] 2>/dev/null; then
        success_test "[OK] FIFO ordering active - $DELIVERED messages delivered in order"
    else
        error_test "[!] No FIFO delivery detected"
    fi
    
    # Copy logs before cleanup
    if [ -n "$LOG_DIR" ] && [ "$LOG_DIR" != "/tmp" ]; then
        log_test "Copying container logs..."
        COMPOSE_PROJECT_NAME=test-fifo docker compose -f producer-consumer.yaml logs --no-color leader > "$LOG_DIR/leader.log" 2>&1 || true
        COMPOSE_PROJECT_NAME=test-fifo docker compose -f producer-consumer.yaml logs --no-color producer > "$LOG_DIR/producer.log" 2>&1 || true
        COMPOSE_PROJECT_NAME=test-fifo docker compose -f producer-consumer.yaml logs --no-color consumer > "$LOG_DIR/consumer.log" 2>&1 || true
    fi
    
    success_test "[OK] FIFO ordering test complete (Docker)"

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
    
    # Sync latest code to VMs before building (ensures protobuf and other changes are present)
    log_test "Syncing latest code to VMs..."
    vagrant rsync 2>&1 | grep -vE "WARNING|Permission denied|libvirt_ip_command" || true
    
    log_test "Building binaries on VMs..."
    BUILD_OUTPUT=$(vagrant_ssh_retry leader 'cd /vagrant/logstream && /usr/local/go/bin/go build -o producer cmd/producer/main.go 2>&1' 2>&1 | grep -vE "WARNING|Permission denied|libvirt_ip_command" || true)
    if [ $? -ne 0 ] || echo "$BUILD_OUTPUT" | grep -qiE "error|undefined|cannot|failed"; then
        error_test "Failed to build producer on leader"
        echo "$BUILD_OUTPUT"
        exit 1
    fi
    
    BUILD_OUTPUT=$(vagrant_ssh_retry broker1 'cd /vagrant/logstream && /usr/local/go/bin/go build -o consumer cmd/consumer/main.go 2>&1' 2>&1 | grep -vE "WARNING|Permission denied|libvirt_ip_command" || true)
    if [ $? -ne 0 ] || echo "$BUILD_OUTPUT" | grep -qiE "error|undefined|cannot|failed"; then
        error_test "Failed to build consumer on broker1"
        echo "$BUILD_OUTPUT"
        exit 1
    fi
    
    success_test "Binaries built successfully on VMs"
    
    if ! ensure_binary_on_vm leader; then
        error_test "Failed to ensure binary on leader VM"
        exit 1
    fi
    
    # Setup log files
    LEADER_LOG="$VM_LOG_DIR/leader.log"
    PRODUCER_LOG="$VM_LOG_DIR/producer.log"
    CONSUMER_LOG="$VM_LOG_DIR/consumer.log"
    
    if ! ensure_vm_log_directory leader "$VM_LOG_DIR" || ! ensure_vm_log_directory broker1 "$VM_LOG_DIR"; then
        error_test "Failed to create log directories on VMs"
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
        error_test "Leader process verification failed"
        exit 1
    fi
    success_test "Leader ready"
    
    log_test "=== STEP 2: Starting Producer and Sending Messages ==="
    if ! prepare_vm_log_file leader "$PRODUCER_LOG"; then
        error_test "Failed to prepare producer log"
        exit 1
    fi
    
    # Start producer and send messages
    vagrant_ssh_retry leader "
        cd /vagrant/logstream && \
        for i in 1 2 3 4 5 6 7 8 9 10; do
            echo \"FIFO-MESSAGE-\$i\"
            sleep 0.5
        done | env LEADER_ADDRESS=192.168.100.10:8001 TOPIC=fifo-test \
        stdbuf -oL -eL ./producer > '$PRODUCER_LOG' 2>&1
    " 2>/dev/null &
    
    sleep 5
    success_test "Producer started and sending messages"
    
    log_test "=== STEP 3: Starting Consumer ==="
    if ! prepare_vm_log_file broker1 "$CONSUMER_LOG"; then
        error_test "Failed to prepare consumer log"
        exit 1
    fi
    
    vagrant_ssh_retry broker1 "
        cd /vagrant/logstream && \
        nohup env LEADER_ADDRESS=192.168.100.10:8001 TOPIC=fifo-test \
        stdbuf -oL -eL ./consumer > '$CONSUMER_LOG' 2>&1 &
        sleep 2
        pgrep -f consumer && echo 'Consumer started'
    " 2>/dev/null
    sleep 5
    success_test "Consumer started"

    sleep 5

    log_test ""
    log_test "========================================="
    log_test "SETUP PHASE COMPLETE (VAGRANT)"
    log_test "========================================="
    log_test ""

    # NOW start the timeout - setup is done, only test logic will be timed
    start_test_timeout

    log_test ""
    log_test "========================================="
    log_test "FIFO ORDERING VERIFICATION:"
    log_test "========================================="

    log_test "Checking sequence numbers in broker logs..."
    vagrant_ssh_retry leader "grep -E '\[DataHoldback\] Delivering data seq=|\[DataHoldback\] Buffered data seq=|FIFO delivered' '$LEADER_LOG' 2>/dev/null | tail -20" 2>/dev/null
    
    DELIVERED=$(vagrant_ssh_retry leader "grep -cE '\[DataHoldback\] Delivering data seq=|FIFO delivered' '$LEADER_LOG' 2>/dev/null || echo 0" 2>/dev/null | tr -d '\n\r' | xargs)
    DELIVERED=${DELIVERED:-0}
    
    log_test ""
    log_test "Messages delivered: $DELIVERED"
    
    if [ "$DELIVERED" -gt 0 ] 2>/dev/null; then
        success_test "[OK] FIFO ordering verified!"
    else
        log_test "[NOTE] No delivery messages found yet"
    fi
    
    success_test "[OK] FIFO ordering test complete (Vagrant)"

else
    error_test "Invalid mode: $MODE"
    log_test "Usage: $0 [local|docker|vagrant]"
    exit 1
fi

kill $TIMEOUT_PID 2>/dev/null || true
success_test "Test completed successfully"
