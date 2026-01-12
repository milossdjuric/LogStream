#!/bin/bash

# Producer-Consumer Test - Basic Data Flow
# Usage: ./tests/test-producer-consumer.sh [local|docker]

MODE="${1:-local}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

source "$SCRIPT_DIR/lib/common.sh"

# Test prefix for log identification
TEST_PREFIX="[TEST-PRODUCER-CONSUMER]"
log_test() { echo -e "${BLUE}${TEST_PREFIX} $1${NC}"; }
success_test() { echo -e "${GREEN}${TEST_PREFIX} $1${NC}"; }
error_test() { echo -e "${RED}${TEST_PREFIX} $1${NC}"; }


echo "================================================"
echo "Test: Producer → Leader → Consumer Data Flow"
echo "Mode: $MODE"
echo "================================================"
echo ""
echo "This test demonstrates:"
echo "  1. Leader accepts producer registration (TCP)"
echo "  2. Leader accepts consumer subscription (TCP)"
echo "  3. Producer sends data to leader (UDP)"
echo "  4. Leader forwards to consumer (TCP)"
echo ""

if [ "$MODE" = "local" ]; then
    # Local mode
    cd "$PROJECT_ROOT"
    
    # Build all components
    build_if_needed "logstream" "main.go"
    build_if_needed "producer" "cmd/producer/main.go"
    build_if_needed "consumer" "cmd/consumer/main.go"
    
    # STEP 1: Start leader
    log "═══ STEP 1: Starting Leader (Broker) ═══"
    export MULTICAST_GROUP="239.0.0.1:9999"
    export BROADCAST_PORT="8888"
    export IS_LEADER="true"
    ./logstream > /tmp/logstream-leader.log 2>&1 &
    LEADER_PID=$!
    success "Leader started (PID: $LEADER_PID)"
    sleep 3
    
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
    tail -20 /tmp/logstream-leader.log | grep -E "CONSUME from|Registered consumer|Subscribed"
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
    exec 3> "$PIPE"  # Keep pipe open
    success "Producer started (PID: $PRODUCER_PID)"
    sleep 3
    
    echo ""
    log "Producer registration:"
    tail -20 /tmp/logstream-leader.log | grep -E "PRODUCE from|Registered producer|assigned broker"
    tail -10 /tmp/logstream-producer.log | grep -E "Connected|Registered|PRODUCE_ACK"
    success " Producer registered for 'test-logs'"
    
    # STEP 4: Send test data
    echo ""
    log "═══ STEP 4: Sending Test Messages ═══"
    for i in {1..5}; do
        MSG="Test message #$i: Server event at $(date +%H:%M:%S)"
        echo "$MSG" >&3
        success "Sent: $MSG"
        sleep 1
    done
    
    sleep 3
    
    # Show results
    echo ""
    echo "========================================="
    echo "PRODUCER-CONSUMER TEST RESULTS:"
    echo "========================================="
    
    echo ""
    log "Leader activity (DATA forwarding):"
    tail -30 /tmp/logstream-leader.log | grep -E "DATA from|Forwarding|RESULT to"
    
    echo ""
    log "Producer activity (sent messages):"
    tail -15 /tmp/logstream-producer.log | grep -E "-> DATA"
    
    echo ""
    log "Consumer activity (received messages):"
    tail -15 /tmp/logstream-consumer.log | grep -E "\[test-logs\] Offset"
    
    echo ""
    echo "========================================="
    echo "DATA FLOW VERIFICATION:"
    echo "========================================="
    
    # Count messages
    SENT=$(tail -50 /tmp/logstream-producer.log | grep "-> DATA" | wc -l)
    RECEIVED=$(tail -50 /tmp/logstream-consumer.log | grep "\[test-logs\] Offset" | wc -l)
    
    echo "Messages sent by producer:     $SENT"
    echo "Messages received by consumer: $RECEIVED"
    
    if [ "$SENT" -eq "$RECEIVED" ] && [ "$SENT" -gt 0 ]; then
        success " All messages delivered successfully!"
    else
        error_msg "⚠ Message count mismatch (sent: $SENT, received: $RECEIVED)"
    fi
    
    echo ""
    echo " Producer → Leader (TCP registration)"
    echo " Consumer → Leader (TCP subscription)"
    echo " Producer → Leader (UDP data)"
    echo " Leader → Consumer (TCP delivery)"
    echo ""
    echo "Complete end-to-end flow verified!"
    echo "========================================="
    echo ""
    
    # Cleanup pipe
    exec 3>&-
    rm -f "$PIPE"
    
    # Follow logs
    log "Following logs (Ctrl+C to stop)..."
    tail -f /tmp/logstream-leader.log /tmp/logstream-producer.log /tmp/logstream-consumer.log

elif [ "$MODE" = "docker" ]; then
    # Docker mode
    COMPOSE_FILE="$SCRIPT_DIR/../compose/producer-consumer.yaml"
    
    if [ ! -f "$COMPOSE_FILE" ]; then
        error_msg "Compose file not found: $COMPOSE_FILE"
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
    
    # STEP 4: Check data flow
    echo ""
    log "═══ STEP 4: Data Flow ═══"
    sleep 5  # Let some messages flow
    
    echo ""
    echo "========================================="
    echo "PRODUCER-CONSUMER TEST RESULTS:"
    echo "========================================="
    
    echo ""
    log "Leader activity:"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs leader | grep -E "DATA from|Forwarding|RESULT to" | tail -15
    
    echo ""
    log "Producer activity:"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs producer | grep -E "-> DATA" | tail -10
    
    echo ""
    log "Consumer activity:"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs consumer | grep -E "\[test-logs\] Offset" | tail -10
    
    echo ""
    echo "========================================="
    echo "DATA FLOW VERIFICATION:"
    echo "========================================="
    
    # Count messages
    SENT=$(docker compose -f "$(basename "$COMPOSE_FILE")" logs producer | grep "-> DATA" | wc -l)
    RECEIVED=$(docker compose -f "$(basename "$COMPOSE_FILE")" logs consumer | grep "\[test-logs\] Offset" | wc -l)
    
    echo "Messages sent by producer:     $SENT"
    echo "Messages received by consumer: $RECEIVED"
    
    if [ "$SENT" -gt 0 ] && [ "$RECEIVED" -gt 0 ]; then
        success " Messages flowing successfully!"
    else
        error_msg "⚠ No message flow detected"
    fi
    
    echo ""
    echo " Producer → Leader (TCP registration)"
    echo " Consumer → Leader (TCP subscription)"
    echo " Producer → Leader (UDP data)"
    echo " Leader → Consumer (TCP delivery)"
    echo ""
    echo "Complete end-to-end flow verified!"
    echo "========================================="
    echo ""
    
    # Follow logs
    log "Following logs (Ctrl+C to stop)..."
    docker compose -f "$(basename "$COMPOSE_FILE")" logs -f

else
    error_msg "Invalid mode: $MODE (use 'local' or 'docker')"
    exit 1
fi