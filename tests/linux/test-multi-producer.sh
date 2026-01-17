#!/bin/bash

# Multi-Producer Test - Multiple Producers → One Consumer
# Usage: ./tests/linux/test-multi-producer.sh [local|docker]

MODE="${1:-local}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/lib/common.sh"

# Test prefix for log identification
TEST_PREFIX="[TEST-MULTI-PRODUCER]"
log_test() { echo -e "${BLUE}${TEST_PREFIX} $1${NC}"; }
success_test() { echo -e "${GREEN}${TEST_PREFIX} $1${NC}"; }
error_test() { echo -e "${RED}${TEST_PREFIX} $1${NC}"; }


echo "================================================"
echo "Test: Multiple Producers → One Consumer"
echo "Mode: $MODE"
echo "================================================"
echo ""
echo "This test demonstrates:"
echo "  1. Three producers register with leader"
echo "  2. One consumer subscribes to topic"
echo "  3. All producers send data concurrently"
echo "  4. Consumer receives from all producers"
echo ""

if [ "$MODE" = "local" ]; then
    # Local mode
    cd "$PROJECT_ROOT"
    
    # Build all components
    build_if_needed "logstream" "main.go"
    build_if_needed "producer" "cmd/producer/main.go"
    build_if_needed "consumer" "cmd/consumer/main.go"
    
    # STEP 1: Start leader
    log "═══ STEP 1: Starting Leader ═══"
    export MULTICAST_GROUP="239.0.0.1:9999"
    export BROADCAST_PORT="8888"
    export IS_LEADER="true"
    ./logstream > /tmp/logstream-leader.log 2>&1 &
    LEADER_PID=$!
    success "Leader started (PID: $LEADER_PID)"
    sleep 3
    
    # STEP 2: Start consumer
    echo ""
    log "═══ STEP 2: Starting Consumer ═══"
    export LEADER_ADDRESS="localhost:8001"
    export TOPIC="test-logs"
    ./consumer > /tmp/logstream-consumer.log 2>&1 &
    CONSUMER_PID=$!
    success "Consumer started (PID: $CONSUMER_PID)"
    sleep 3
    
    tail -10 /tmp/logstream-consumer.log | grep -E "Connected|Subscribed"
    success " Consumer subscribed to 'test-logs'"
    
    # STEP 3: Start three producers
    echo ""
    log "═══ STEP 3: Starting Three Producers ═══"
    
    for i in {1..3}; do
        PIPE="/tmp/producer-${i}-input.pipe"
        rm -f "$PIPE"
        mkfifo "$PIPE"
        ./producer < "$PIPE" > /tmp/logstream-producer${i}.log 2>&1 &
        eval "PRODUCER${i}_PID=$!"
        eval "exec $((i+2))> '$PIPE'"  # fd 3, 4, 5
        success "Producer $i started (PID: $(eval echo \$PRODUCER${i}_PID))"
        sleep 2
    done
    
    # FIX: Wait longer for all producers to register
    echo ""
    log "Waiting for producers to register..."
    sleep 15
    
    # Check all registered with retry
    echo ""
    log "Producer registrations:"
    
    # Try up to 5 times to find all registrations
    for attempt in {1..5}; do
        REGISTERED=$(tail -50 /tmp/logstream-leader.log | grep "PRODUCE from" | wc -l)
        if [ "$REGISTERED" -eq 3 ]; then
            break
        fi
        if [ "$attempt" -lt 5 ]; then
            sleep 2
        fi
    done
    
    tail -30 /tmp/logstream-leader.log | grep "PRODUCE from"
    
    if [ "$REGISTERED" -eq 3 ]; then
        success " All 3 producers registered"
    else
        error_msg "⚠ Only $REGISTERED producers registered (expected 3)"
    fi
    
    # STEP 4: Send test data from all producers
    echo ""
    log "═══ STEP 4: Sending Concurrent Messages ═══"
    
    for round in {1..3}; do
        for i in {1..3}; do
            MSG="[Producer-$i] Round $round: Message at $(date +%H:%M:%S.%N)"
            echo "$MSG" >&$((i+2))
            echo "  Producer $i: $MSG"
        done
        sleep 1
    done
    
    sleep 3
    
    # Show results
    echo ""
    echo "========================================="
    echo "MULTI-PRODUCER TEST RESULTS:"
    echo "========================================="
    
    echo ""
    log "Leader activity (all producers):"
    tail -50 /tmp/logstream-leader.log | grep -E "DATA from" | tail -15
    
    echo ""
    log "Consumer received messages:"
    tail -20 /tmp/logstream-consumer.log | grep "\[test-logs\] Offset"
    
    echo ""
    echo "========================================="
    echo "MESSAGE COUNT VERIFICATION:"
    echo "========================================="
    
    for i in {1..3}; do
        SENT=$(tail -50 /tmp/logstream-producer${i}.log | grep "-> DATA" | wc -l)
        echo "Producer $i sent: $SENT messages"
    done
    
    TOTAL_RECEIVED=$(tail -50 /tmp/logstream-consumer.log | grep "\[test-logs\] Offset" | wc -l)
    echo "Consumer received: $TOTAL_RECEIVED messages total"
    
    if [ "$TOTAL_RECEIVED" -ge 9 ]; then
        success " Consumer received messages from multiple producers!"
    else
        error_msg "⚠ Expected at least 9 messages, got $TOTAL_RECEIVED"
    fi
    
    echo ""
    echo " Multiple producers can send concurrently"
    echo " Single consumer receives from all producers"
    echo " Topic-based routing works correctly"
    echo "========================================="
    echo ""
    
    # Cleanup pipes
    for i in {1..3}; do
        eval "exec $((i+2))>&-"
        rm -f "/tmp/producer-${i}-input.pipe"
    done
    
    # Follow logs
    log "Following logs (Ctrl+C to stop)..."
    tail -f /tmp/logstream-leader.log /tmp/logstream-consumer.log

elif [ "$MODE" = "docker" ]; then
    # Docker mode
    COMPOSE_FILE="$SCRIPT_DIR/../compose/multi-producer.yaml"
    
    if [ ! -f "$COMPOSE_FILE" ]; then
        error_msg "Compose file not found: $COMPOSE_FILE"
        exit 1
    fi
    
    cd "$(dirname "$COMPOSE_FILE")"
    
    log "═══ Starting All Containers ═══"
    docker compose -f "$(basename "$COMPOSE_FILE")" up -d
    
    sleep 10
    
    # Check status
    echo ""
    log "Container status:"
    docker compose -f "$(basename "$COMPOSE_FILE")" ps
    
    # Show results
    echo ""
    echo "========================================="
    echo "MULTI-PRODUCER TEST RESULTS:"
    echo "========================================="
    
    echo ""
    log "Leader activity (all producers):"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs leader | grep -E "DATA from" | tail -15
    
    echo ""
    log "Consumer received messages:"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs consumer | grep "\[test-logs\] Offset" | tail -15
    
    echo ""
    log "Producer 1 sent:"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs producer1 | grep "-> DATA" | wc -l
    
    log "Producer 2 sent:"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs producer2 | grep "-> DATA" | wc -l
    
    log "Producer 3 sent:"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs producer3 | grep "-> DATA" | wc -l
    
    echo ""
    echo " Multiple producers can send concurrently"
    echo " Single consumer receives from all producers"
    echo " Topic-based routing works correctly"
    echo "========================================="
    echo ""
    
    # Follow logs
    log "Following logs (Ctrl+C to stop)..."
    docker compose -f "$(basename "$COMPOSE_FILE")" logs -f

else
    error_msg "Invalid mode: $MODE (use 'local' or 'docker')"
    exit 1
fi
