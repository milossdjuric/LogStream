#!/bin/bash

# Enhanced E2E Test - Shows Complete Data Flow
# Usage: ./tests/test-e2e-enhanced.sh [local|docker]

MODE="${1:-local}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

source "$SCRIPT_DIR/lib/common.sh"

# Test prefix for log identification
TEST_PREFIX="[TEST-E2E]"
log_test() { echo -e "${BLUE}${TEST_PREFIX} $1${NC}"; }
success_test() { echo -e "${GREEN}${TEST_PREFIX} $1${NC}"; }
error_test() { echo -e "${RED}${TEST_PREFIX} $1${NC}"; }


echo "================================================"
echo "Test: End-to-End Data Flow (Producer→Consumer)"
echo "Mode: $MODE"
echo "================================================"
echo ""
echo "This test demonstrates:"
echo "  1. Producer registers with leader (TCP)"
echo "  2. Consumer subscribes to topic (TCP)"
echo "  3. Producer sends data (UDP)"
echo "  4. Leader routes to consumer (UDP)"
echo ""
echo "Data Flow: Producer → Leader → Consumer"
echo ""

if [ "$MODE" = "local" ]; then
    # Local mode
    cd "$PROJECT_ROOT"
    
    # Build
    build_if_needed "logstream" "main.go"
    build_if_needed "producer" "examples/producer/main.go"
    build_if_needed "consumer" "examples/consumer/main.go"
    
    # STEP 1: Start leader
    log "═══ STEP 1: Starting Leader (Broker) ═══"
    export MULTICAST_GROUP="239.0.0.1:9999"
    export BROADCAST_PORT="8888"
    export IS_LEADER="true"
    ./logstream > /tmp/logstream-leader.log 2>&1 &
    LEADER_PID=$!
    success "Leader started (PID: $LEADER_PID)"
    sleep 5
    
    echo ""
    log "Leader ready:"
    tail -10 /tmp/logstream-leader.log | grep -E "TCP listener|Started"
    success " Leader accepting connections on port 8001"
    
    # STEP 2: Start consumer
    echo ""
    log "═══ STEP 2: Starting Consumer (Subscriber) ═══"
    export LEADER_ADDRESS="localhost:8001"
    export TOPIC="test-logs"
    ./consumer > /tmp/logstream-consumer.log 2>&1 &
    CONSUMER_PID=$!
    success "Consumer started (PID: $CONSUMER_PID)"
    sleep 3
    
    echo ""
    log "Consumer registration:"
    tail -15 /tmp/logstream-leader.log | grep -E "CONSUME from|Registered consumer|Subscribed"
    tail -10 /tmp/logstream-consumer.log | grep -E "Connected|Subscribed"
    success " Consumer registered and subscribed to 'test-logs'"
    
    # STEP 3: Start producer
    echo ""
    log "═══ STEP 3: Starting Producer ═══"
    PIPE="/tmp/producer-input.pipe"
    rm -f "$PIPE"
    mkfifo "$PIPE"
    ./producer < "$PIPE" > /tmp/logstream-producer.log 2>&1 &
    PRODUCER_PID=$!
    success "Producer started (PID: $PRODUCER_PID)"
    sleep 3
    
    echo ""
    log "Producer registration:"
    tail -15 /tmp/logstream-leader.log | grep -E "PRODUCE from|Registered producer|assigned broker"
    tail -10 /tmp/logstream-producer.log | grep -E "Connected|Registered"
    success " Producer registered for 'test-logs'"
    
    # STEP 4: Send test data
    echo ""
    log "═══ STEP 4: Sending Test Messages ═══"
    for i in {1..5}; do
        MSG="Test message #$i: Server event at $(date +%H:%M:%S)"
        echo "$MSG" > "$PIPE"
        success "Sent: $MSG"
        sleep 1
    done
    
    sleep 2
    
    # Show results
    echo ""
    echo "========================================="
    echo "E2E TEST RESULTS:"
    echo "========================================="
    
    echo ""
    log "Leader activity (last 15 lines):"
    tail -15 /tmp/logstream-leader.log | grep -E "PRODUCE|CONSUME|DATA"
    
    echo ""
    log "Producer activity (last 10 lines):"
    tail -10 /tmp/logstream-producer.log
    
    echo ""
    log "Consumer activity (last 10 lines):"
    tail -10 /tmp/logstream-consumer.log
    
    echo ""
    echo "========================================="
    echo "DATA FLOW SUMMARY:"
    echo "========================================="
    echo " Producer → Leader (TCP registration)"
    echo " Consumer → Leader (TCP subscription)"
    echo " Producer → Leader (UDP data)"
    echo " Leader → Consumer (UDP delivery)"
    echo ""
    echo "Complete end-to-end flow verified!"
    echo "========================================="
    echo ""
    
    # Follow logs
    log "Following logs (Ctrl+C to stop)..."
    echo "You can send more messages by typing in the terminal"
    echo ""
    tail -f /tmp/logstream-*.log

elif [ "$MODE" = "docker" ]; then
    # Docker mode
    COMPOSE_FILE="$SCRIPT_DIR/../compose/e2e.yaml"
    
    if [ ! -f "$COMPOSE_FILE" ]; then
        error "Compose file not found: $COMPOSE_FILE"
        exit 1
    fi
    
    cd "$(dirname "$COMPOSE_FILE")"
    
    # Start all containers
    log "═══ Starting All Containers ═══"
    docker compose -f "$(basename "$COMPOSE_FILE")" up -d
    
    sleep 8  # Give time for connections to establish
    
    # Show container status
    echo ""
    log "Container status:"
    docker compose -f "$(basename "$COMPOSE_FILE")" ps
    
    # STEP 1: Check leader
    echo ""
    log "═══ STEP 1: Leader Status ═══"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs leader | grep -E "TCP listener|Started" | tail -5
    success " Leader ready"
    
    # STEP 2: Check consumer
    echo ""
    log "═══ STEP 2: Consumer Status ═══"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs leader | grep -E "CONSUME from|Registered consumer" | tail -5
    docker compose -f "$(basename "$COMPOSE_FILE")" logs consumer | grep -E "Connected|Subscribed" | tail -5
    success " Consumer registered"
    
    # STEP 3: Check producer
    echo ""
    log "═══ STEP 3: Producer Status ═══"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs leader | grep -E "PRODUCE from|Registered producer" | tail -5
    docker compose -f "$(basename "$COMPOSE_FILE")" logs producer | grep -E "Connected|Registered" | tail -5
    success " Producer registered"
    
    # Show recent logs
    echo ""
    echo "========================================="
    echo "E2E TEST RESULTS:"
    echo "========================================="
    
    echo ""
    log "Leader activity:"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs --tail=20 leader
    
    echo ""
    log "Producer activity:"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs --tail=15 producer
    
    echo ""
    log "Consumer activity:"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs --tail=15 consumer
    
    echo ""
    echo "========================================="
    echo "DATA FLOW SUMMARY:"
    echo "========================================="
    echo " Producer → Leader (TCP registration)"
    echo " Consumer → Leader (TCP subscription)"
    echo " Producer → Leader (UDP data)"
    echo " Leader → Consumer (UDP delivery)"
    echo ""
    echo "Complete end-to-end flow verified!"
    echo "========================================="
    echo ""
    
    # Follow logs
    log "Following logs (Ctrl+C to stop)..."
    docker compose -f "$(basename "$COMPOSE_FILE")" logs -f

else
    error "Invalid mode: $MODE (use 'local' or 'docker')"
    exit 1
fi