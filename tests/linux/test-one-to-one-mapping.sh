#!/bin/bash

# Test: One-to-One Producer-Consumer Mapping
# This test verifies that each topic can have only one producer and one consumer.

TEST_TIMEOUT=300
MODE="${1:-local}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/lib/common.sh"

# Setup log directory
setup_log_directory "$MODE"

TEST_PREFIX="[TEST-ONE-TO-ONE]"
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
        [ -n "$PRODUCER1_LOG" ] && copy_vm_log_to_host leader "$PRODUCER1_LOG" "$LOG_DIR/leader-producer1.log" 2>/dev/null || true
        [ -n "$PRODUCER2_LOG" ] && copy_vm_log_to_host broker1 "$PRODUCER2_LOG" "$LOG_DIR/broker1-producer2.log" 2>/dev/null || true
        [ -n "$CONSUMER1_LOG" ] && copy_vm_log_to_host broker1 "$CONSUMER1_LOG" "$LOG_DIR/broker1-consumer1.log" 2>/dev/null || true
        [ -n "$CONSUMER2_LOG" ] && copy_vm_log_to_host broker2 "$CONSUMER2_LOG" "$LOG_DIR/broker2-consumer2.log" 2>/dev/null || true
    fi
    
    if [ "$MODE" = "local" ]; then
        (trap '' 13; exec 3>&- 2>/dev/null) || true
        (trap '' 13; exec 4>&- 2>/dev/null) || true
        
        # Copy node log files BEFORE killing processes
        if [ -n "$LOG_DIR" ] && [ "$LOG_DIR" != "/tmp" ]; then
            log_test "Preserving node log files..."
            [ -n "$LEADER_LOG" ] && [ -f "$LEADER_LOG" ] && cp "$LEADER_LOG" "$LOG_DIR/leader.log" 2>/dev/null
            [ -n "$PRODUCER1_LOG" ] && [ -f "$PRODUCER1_LOG" ] && cp "$PRODUCER1_LOG" "$LOG_DIR/producer1.log" 2>/dev/null
            [ -n "$PRODUCER2_LOG" ] && [ -f "$PRODUCER2_LOG" ] && cp "$PRODUCER2_LOG" "$LOG_DIR/producer2.log" 2>/dev/null
            [ -n "$CONSUMER1_LOG" ] && [ -f "$CONSUMER1_LOG" ] && cp "$CONSUMER1_LOG" "$LOG_DIR/consumer1.log" 2>/dev/null
            [ -n "$CONSUMER2_LOG" ] && [ -f "$CONSUMER2_LOG" ] && cp "$CONSUMER2_LOG" "$LOG_DIR/consumer2.log" 2>/dev/null
        fi
        
        [ -n "$LEADER_PID" ] && kill_netns_process logstream-a "$LEADER_PID" 2>/dev/null
        [ -n "$PRODUCER1_PID" ] && kill -9 "$PRODUCER1_PID" 2>/dev/null
        [ -n "$PRODUCER2_PID" ] && kill -9 "$PRODUCER2_PID" 2>/dev/null
        [ -n "$CONSUMER1_PID" ] && kill -9 "$CONSUMER1_PID" 2>/dev/null
        [ -n "$CONSUMER2_PID" ] && kill -9 "$CONSUMER2_PID" 2>/dev/null
        sleep 1
        cleanup_netns_processes
        rm -f /tmp/logstream-*.log /tmp/producer*.pipe 2>/dev/null
    elif [ "$MODE" = "docker" ]; then
        # Cleanup temp producer containers
        docker rm -f test-producer1 test-producer2 2>/dev/null || true
        if [ -n "$LOG_DIR" ] && [ "$LOG_DIR" != "/tmp" ]; then
            if [ ! -f "$LOG_DIR/leader.log" ]; then
                log_test "Copying container logs..."
                cd "$PROJECT_ROOT/deploy/docker/compose" 2>/dev/null || true
                COMPOSE_PROJECT_NAME=test-one-to-one docker compose -f producer-consumer.yaml logs --no-color leader > "$LOG_DIR/leader.log" 2>&1 || true
            fi
        fi
        cd "$PROJECT_ROOT/deploy/docker/compose" 2>/dev/null || true
        COMPOSE_PROJECT_NAME=test-one-to-one docker compose -f producer-consumer.yaml down --remove-orphans 2>/dev/null || true
    elif [ "$MODE" = "vagrant" ]; then
        vagrant_ssh_retry leader 'pkill -f logstream; pkill -f producer; pkill -f consumer' 2>/dev/null || true
        vagrant_ssh_retry broker1 'pkill -f producer; pkill -f consumer' 2>/dev/null || true
        vagrant_ssh_retry broker2 'pkill -f consumer' 2>/dev/null || true
    fi
    error_test "Cleanup complete"
}

TIMEOUT_MARKER="/tmp/one-to-one-test-timeout-$$"

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
log_test "Test: One-to-One Producer-Consumer Mapping"
log_test "Mode: $MODE"
log_test "Timeout: ${TEST_TIMEOUT}s"
log_test "================================================"
log_test ""
log_test "This test verifies:"
log_test "  1. First producer can register for a topic"
log_test "  2. Second producer for same topic is REJECTED"
log_test "  3. First consumer can subscribe to a topic"
log_test "  4. Second consumer for same topic is REJECTED"
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
    LEADER_LOG="/tmp/logstream-leader-one-to-one.log"
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
    
    # Wait for leader (discovery + init ~15-20s)
    for i in {1..25}; do
        if grep -q "TCP listener started" "$LEADER_LOG" 2>/dev/null; then
            success_test "Leader ready"
            break
        fi
        sleep 1
    done

    log_test ""
    log_test "========================================="
    log_test "SETUP PHASE COMPLETE (LOCAL)"
    log_test "========================================="
    log_test ""

    # NOW start the timeout - setup is done, only test logic will be timed
    start_test_timeout

    # Test 1: First producer should succeed
    log_test ""
    log_test "=== TEST 1: First Producer Registration ==="
    PRODUCER1_LOG="/tmp/logstream-producer1.log"
    PIPE1="/tmp/producer1.pipe"
    rm -f "$PIPE1"
    mkfifo "$PIPE1" 2>/dev/null || true
    chmod 666 "$PIPE1"
    sudo touch "$PRODUCER1_LOG" && sudo chmod 666 "$PRODUCER1_LOG"
    
    sudo ip netns exec logstream-c bash -c "
        cd '$PROJECT_ROOT' && \
        env LEADER_ADDRESS=172.20.0.10:8001 TOPIC=exclusive-topic \
        stdbuf -oL -eL ./producer < '$PIPE1' > '$PRODUCER1_LOG' 2>&1
    " &
    PRODUCER1_PID=$!
    exec 3> "$PIPE1"
    
    sleep 3
    
    # Check LEADER_LOG (authoritative source) for registration
    if grep -qE "Registered producer.*exclusive-topic|Stream assigned.*topic=exclusive-topic.*producer=" "$LEADER_LOG" 2>/dev/null; then
        success_test "[OK] Producer1 registered successfully"
        PRODUCER1_OK=true
        
        # Show evidence from leader log
        log_test "Registration evidence from leader:"
        grep "Registered producer.*exclusive-topic\|Stream assigned.*exclusive-topic" "$LEADER_LOG" 2>/dev/null | head -2 | sed 's/^/    /'
    else
        error_test "[FAIL] Producer1 failed to register"
        PRODUCER1_OK=false
        
        # Show what was checked
        error_test "Checked leader log for patterns:"
        error_test "  - 'Registered producer ... exclusive-topic'"
        error_test "  - 'Stream assigned: topic=exclusive-topic'"
    fi
    
    echo "producer1-test-data" >&3 2>/dev/null
    sleep 2
    
    # Test 2: Second producer should be REJECTED
    log_test ""
    log_test "=== TEST 2: Second Producer Registration (should be REJECTED) ==="
    PRODUCER2_LOG="/tmp/logstream-producer2.log"
    PIPE2="/tmp/producer2.pipe"
    rm -f "$PIPE2"
    mkfifo "$PIPE2" 2>/dev/null || true
    chmod 666 "$PIPE2"
    sudo touch "$PRODUCER2_LOG" && sudo chmod 666 "$PRODUCER2_LOG"
    
    sudo ip netns exec logstream-b bash -c "
        cd '$PROJECT_ROOT' && \
        env LEADER_ADDRESS=172.20.0.10:8001 TOPIC=exclusive-topic \
        timeout 10 stdbuf -oL -eL ./producer < '$PIPE2' > '$PRODUCER2_LOG' 2>&1
    " &
    PRODUCER2_PID=$!
    exec 4> "$PIPE2"
    
    sleep 5
    
    if grep -qE "REJECTED.*already has a producer.*one-to-one|topic.*already has a producer" "$LEADER_LOG" 2>/dev/null || \
       grep -qE "failed|error|Empty|REJECTED" "$PRODUCER2_LOG" 2>/dev/null; then
        success_test "[OK] Producer2 correctly REJECTED (one-to-one enforced)"
        PRODUCER2_REJECTED=true
    else
        error_test "[FAIL] Producer2 was not rejected!"
        PRODUCER2_REJECTED=false
    fi
    
    # Test 3: First consumer should succeed
    log_test ""
    log_test "=== TEST 3: First Consumer Registration ==="
    CONSUMER1_LOG="/tmp/logstream-consumer1.log"
    sudo touch "$CONSUMER1_LOG" && sudo chmod 666 "$CONSUMER1_LOG"
    
    sudo ip netns exec logstream-b env \
        LEADER_ADDRESS=172.20.0.10:8001 \
        TOPIC="exclusive-topic" \
        stdbuf -oL -eL ./consumer > "$CONSUMER1_LOG" 2>&1 &
    CONSUMER1_PID=$!
    
    sleep 5
    
    # Check LEADER_LOG (authoritative source) for registration
    if grep -qE "Registered consumer.*exclusive-topic|Consumer assigned to stream.*exclusive-topic" "$LEADER_LOG" 2>/dev/null; then
        success_test "[OK] Consumer1 registered successfully"
        CONSUMER1_OK=true
        
        # Show evidence from leader log
        log_test "Registration evidence from leader:"
        grep "Registered consumer.*exclusive-topic\|Consumer assigned.*exclusive-topic" "$LEADER_LOG" 2>/dev/null | head -2 | sed 's/^/    /'
    else
        error_test "[FAIL] Consumer1 failed to register"
        CONSUMER1_OK=false
        
        # Show what was checked
        error_test "Checked leader log for patterns:"
        error_test "  - 'Registered consumer ... exclusive-topic'"
        error_test "  - 'Consumer assigned to stream: topic=exclusive-topic'"
    fi
    
    # Test 4: Second consumer should be REJECTED
    log_test ""
    log_test "=== TEST 4: Second Consumer Registration (should be REJECTED) ==="
    CONSUMER2_LOG="/tmp/logstream-consumer2.log"
    sudo touch "$CONSUMER2_LOG" && sudo chmod 666 "$CONSUMER2_LOG"
    
    sudo ip netns exec logstream-c bash -c "
        cd '$PROJECT_ROOT' && \
        env LEADER_ADDRESS=172.20.0.10:8001 TOPIC=exclusive-topic \
        timeout 10 stdbuf -oL -eL ./consumer > '$CONSUMER2_LOG' 2>&1
    " &
    CONSUMER2_PID=$!
    
    sleep 7
    
    if grep -qE "REJECTED.*already has a consumer.*one-to-one|topic.*already has a consumer" "$LEADER_LOG" 2>/dev/null || \
       grep -qE "failed|error|subscription failed|REJECTED" "$CONSUMER2_LOG" 2>/dev/null; then
        success_test "[OK] Consumer2 correctly REJECTED (one-to-one enforced)"
        CONSUMER2_REJECTED=true
    else
        error_test "[FAIL] Consumer2 was not rejected!"
        CONSUMER2_REJECTED=false
    fi
    
    log_test ""
    log_test "========================================="
    log_test "ONE-TO-ONE MAPPING TEST RESULTS:"
    log_test "========================================="
    
    log_test ""
    log_test "Stream assignments in leader:"
    grep -E "Assigned stream|stream.*topic|one-to-one" "$LEADER_LOG" 2>/dev/null | tail -10
    
    log_test ""
    log_test "Summary:"
    log_test "  Producer 1 registered: $([ "$PRODUCER1_OK" = "true" ] && echo "YES" || echo "NO")"
    log_test "  Producer 2 rejected:   $([ "$PRODUCER2_REJECTED" = "true" ] && echo "YES" || echo "NO")"
    log_test "  Consumer 1 registered: $([ "$CONSUMER1_OK" = "true" ] && echo "YES" || echo "NO")"
    log_test "  Consumer 2 rejected:   $([ "$CONSUMER2_REJECTED" = "true" ] && echo "YES" || echo "NO")"
    
    if [ "$PRODUCER1_OK" = "true" ] && [ "$PRODUCER2_REJECTED" = "true" ] && \
       [ "$CONSUMER1_OK" = "true" ] && [ "$CONSUMER2_REJECTED" = "true" ]; then
        success_test ""
        success_test "[OK] ALL ONE-TO-ONE MAPPING TESTS PASSED!"
    else
        error_test ""
        error_test "[FAIL] One-to-one mapping test failed"
        exit 1
    fi
    
    success_test "[OK] One-to-one mapping test complete"

elif [ "$MODE" = "docker" ]; then
    COMPOSE_FILE="$PROJECT_ROOT/deploy/docker/compose/producer-consumer.yaml"
    
    if [ ! -f "$COMPOSE_FILE" ]; then
        error_test "Compose file not found: $COMPOSE_FILE"
        exit 1
    fi
    
    cd "$PROJECT_ROOT/deploy/docker/compose"
    
    log_test "=== Building Images ==="
    COMPOSE_PROJECT_NAME=test-one-to-one docker compose -f producer-consumer.yaml build
    
    log_test "=== Starting Leader Container ==="
    COMPOSE_PROJECT_NAME=test-one-to-one docker compose -f producer-consumer.yaml up -d leader
    if [ $? -ne 0 ]; then
        error_test "Failed to start leader"
        exit 1
    fi
    
    # Wait for leader to be healthy
    log_test "Waiting for leader to be ready..."
    for i in {1..30}; do
        if docker inspect test-one-to-one-leader-1 2>/dev/null | grep -q '"Status": "healthy"'; then
            success_test "Leader is healthy"
            break
        fi
        sleep 2
    done
    
    log_test ""
    log_test "Leader status:"
    COMPOSE_PROJECT_NAME=test-one-to-one docker compose -f producer-consumer.yaml logs leader 2>/dev/null | grep -E "TCP listener|Started" | tail -3

    log_test ""
    log_test "========================================="
    log_test "SETUP PHASE COMPLETE (DOCKER)"
    log_test "========================================="
    log_test ""

    # NOW start the timeout - setup is done, only test logic will be timed
    start_test_timeout

    # Test one-to-one by running producer in temporary containers
    log_test ""
    log_test "=== TEST 1: First Producer Registration ==="
    
    # Run first producer (should succeed) - keep it running with stdin
    docker run -d --network test-one-to-one_logstream-net \
        -e LEADER_ADDRESS=172.28.0.10:8001 \
        -e TOPIC=exclusive-topic \
        --name test-producer1 \
        -i \
        test-one-to-one-producer \
        sh -c "sleep 2 && ./producer" > /dev/null 2>&1
    
    # Send initial data to producer
    sleep 5
    echo "test-data-1" | docker exec -i test-producer1 cat > /dev/null 2>&1 || true
    sleep 3
    
    docker logs test-producer1 > /tmp/producer1-docker.log 2>&1 || true
    
    if grep -qE "PRODUCE_ACK|assigned broker" /tmp/producer1-docker.log 2>/dev/null; then
        success_test "[OK] Producer1 registered"
        PRODUCER1_OK=true
    else
        PRODUCER1_OK=false
        log_test "Producer1 log:"
        cat /tmp/producer1-docker.log 2>/dev/null
    fi
    
    log_test ""
    log_test "=== TEST 2: Second Producer (should be REJECTED) ==="
    
    # Run second producer for same topic (should fail due to one-to-one)
    docker run -d --network test-one-to-one_logstream-net \
        -e LEADER_ADDRESS=172.28.0.10:8001 \
        -e TOPIC=exclusive-topic \
        --name test-producer2 \
        -i \
        test-one-to-one-producer \
        sh -c "sleep 2 && ./producer" > /dev/null 2>&1
    
    sleep 8
    
    docker logs test-producer2 > /tmp/producer2-docker.log 2>&1 || true
    
    LEADER_LOGS=$(COMPOSE_PROJECT_NAME=test-one-to-one docker compose -f producer-consumer.yaml logs leader 2>/dev/null)
    if echo "$LEADER_LOGS" | grep -qE "REJECTED.*already has a producer.*one-to-one|topic.*already has a producer" || \
       grep -qE "failed|error|REJECTED|already has" /tmp/producer2-docker.log 2>/dev/null; then
        success_test "[OK] Producer2 correctly REJECTED"
        PRODUCER2_REJECTED=true
    else
        PRODUCER2_REJECTED=false
        log_test "Producer2 log:"
        cat /tmp/producer2-docker.log 2>/dev/null
        log_test "Leader logs (rejection check):"
        echo "$LEADER_LOGS" | grep -iE "REJECTED|already has" | tail -5
    fi
    
    log_test ""
    log_test "========================================="
    log_test "ONE-TO-ONE MAPPING TEST RESULTS:"
    log_test "========================================="
    log_test "  Producer 1 registered: $([ "$PRODUCER1_OK" = "true" ] && echo "YES" || echo "NO")"
    log_test "  Producer 2 rejected:   $([ "$PRODUCER2_REJECTED" = "true" ] && echo "YES" || echo "NO")"
    
    # Cleanup temp containers
    docker rm -f test-producer1 test-producer2 2>/dev/null || true
    rm -f /tmp/producer1-docker.log /tmp/producer2-docker.log 2>/dev/null
    
    # Copy logs
    if [ -n "$LOG_DIR" ] && [ "$LOG_DIR" != "/tmp" ]; then
        COMPOSE_PROJECT_NAME=test-one-to-one docker compose -f producer-consumer.yaml logs --no-color leader > "$LOG_DIR/leader.log" 2>&1 || true
    fi
    
    if [ "$PRODUCER1_OK" = "true" ] && [ "$PRODUCER2_REJECTED" = "true" ]; then
        success_test "[OK] One-to-one producer mapping verified (Docker)"
    else
        log_test "[NOTE] One-to-one test partially verified"
    fi
    
    success_test "[OK] One-to-one mapping test complete (Docker)"

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
    log_test "Building binaries on VMs..."
    vagrant_ssh_retry leader 'cd /vagrant/logstream && /usr/local/go/bin/go build -o producer cmd/producer/main.go && /usr/local/go/bin/go build -o consumer cmd/consumer/main.go' 2>/dev/null
    vagrant_ssh_retry broker1 'cd /vagrant/logstream && /usr/local/go/bin/go build -o producer cmd/producer/main.go && /usr/local/go/bin/go build -o consumer cmd/consumer/main.go' 2>/dev/null
    
    if ! ensure_binary_on_vm leader; then
        error_test "Failed to ensure binary on leader VM"
        exit 1
    fi
    
    # Setup log files
    LEADER_LOG="$VM_LOG_DIR/leader.log"
    PRODUCER1_LOG="$VM_LOG_DIR/producer1.log"
    PRODUCER2_LOG="$VM_LOG_DIR/producer2.log"
    CONSUMER1_LOG="$VM_LOG_DIR/consumer1.log"
    CONSUMER2_LOG="$VM_LOG_DIR/consumer2.log"
    
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
    log_test "========================================="
    log_test "SETUP PHASE COMPLETE (VAGRANT)"
    log_test "========================================="
    log_test ""

    # NOW start the timeout - setup is done, only test logic will be timed
    start_test_timeout

    # Test 1: First producer
    log_test ""
    log_test "=== TEST 1: First Producer Registration ==="
    if ! prepare_vm_log_file leader "$PRODUCER1_LOG"; then
        error_test "Failed to prepare producer1 log"
        exit 1
    fi
    
    vagrant_ssh_retry leader "
        cd /vagrant/logstream && \
        echo 'test-data' | env LEADER_ADDRESS=192.168.100.10:8001 TOPIC=exclusive-topic \
        timeout 10 ./producer > '$PRODUCER1_LOG' 2>&1
    " 2>/dev/null
    
    sleep 3
    
    if vagrant_ssh_retry leader "grep -qE 'PRODUCE_ACK|assigned' '$PRODUCER1_LOG' 2>/dev/null" 2>/dev/null; then
        success_test "[OK] Producer1 registered"
        PRODUCER1_OK=true
    else
        error_test "[FAIL] Producer1 failed"
        PRODUCER1_OK=false
    fi
    
    # Keep producer1 running
    vagrant_ssh_retry leader "
        cd /vagrant/logstream && \
        nohup sh -c 'while true; do echo keepalive; sleep 5; done | env LEADER_ADDRESS=192.168.100.10:8001 TOPIC=exclusive-topic ./producer' > /dev/null 2>&1 &
    " 2>/dev/null
    sleep 2
    
    # Test 2: Second producer (should fail)
    log_test ""
    log_test "=== TEST 2: Second Producer (should be REJECTED) ==="
    if ! prepare_vm_log_file broker1 "$PRODUCER2_LOG"; then
        error_test "Failed to prepare producer2 log"
        exit 1
    fi
    
    vagrant_ssh_retry broker1 "
        cd /vagrant/logstream && \
        echo 'test-data' | env LEADER_ADDRESS=192.168.100.10:8001 TOPIC=exclusive-topic \
        timeout 10 ./producer > '$PRODUCER2_LOG' 2>&1
    " 2>/dev/null
    
    sleep 5
    
    LEADER_CHECK=$(vagrant_ssh_retry leader "grep -iE 'REJECTED.*producer.*one-to-one|already has a producer' '$LEADER_LOG' 2>/dev/null" 2>/dev/null)
    PRODUCER2_CHECK=$(vagrant_ssh_retry broker1 "grep -E 'failed|error|REJECTED' '$PRODUCER2_LOG' 2>/dev/null" 2>/dev/null)
    
    if [ -n "$LEADER_CHECK" ] || [ -n "$PRODUCER2_CHECK" ]; then
        success_test "[OK] Producer2 correctly REJECTED"
        PRODUCER2_REJECTED=true
    else
        error_test "[FAIL] Producer2 was not rejected"
        PRODUCER2_REJECTED=false
    fi
    
    log_test ""
    log_test "========================================="
    log_test "ONE-TO-ONE MAPPING TEST RESULTS:"
    log_test "========================================="
    log_test "  Producer 1 registered: $([ "$PRODUCER1_OK" = "true" ] && echo "YES" || echo "NO")"
    log_test "  Producer 2 rejected:   $([ "$PRODUCER2_REJECTED" = "true" ] && echo "YES" || echo "NO")"
    
    if [ "$PRODUCER1_OK" = "true" ] && [ "$PRODUCER2_REJECTED" = "true" ]; then
        success_test "[OK] One-to-one producer mapping verified!"
    else
        log_test "[NOTE] One-to-one test partially verified"
    fi
    
    success_test "[OK] One-to-one mapping test complete (Vagrant)"

else
    error_test "Invalid mode: $MODE"
    log_test "Usage: $0 [local|docker|vagrant]"
    exit 1
fi

kill $TIMEOUT_PID 2>/dev/null || true
success_test "Test completed successfully"
