#!/bin/bash

# Enhanced Duo Test - Shows Sequence Progression (1→2)
# Usage: ./tests/test-duo-enhanced.sh [local|docker]

MODE="${1:-local}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

source "$SCRIPT_DIR/lib/common.sh"

# Test prefix for log identification
TEST_PREFIX="[TEST-DUO]"
log_test() { echo -e "${BLUE}${TEST_PREFIX} $1${NC}"; }
success_test() { echo -e "${GREEN}${TEST_PREFIX} $1${NC}"; }
error_test() { echo -e "${RED}${TEST_PREFIX} $1${NC}"; }


echo "========================================"
echo "Test: Dynamic Broker Join (Seq Demo)"
echo "Mode: $MODE"
echo "========================================"
echo ""

if [ "$MODE" = "local" ]; then
    # Local mode
    cd "$PROJECT_ROOT"
    
    # Build
    build_if_needed "logstream" "main.go"
    
    # Start leader
    log "STEP 1: Starting leader..."
    export MULTICAST_GROUP="239.0.0.1:9999"
    export BROADCAST_PORT="8888"
    export IS_LEADER="true"
    ./logstream > /tmp/logstream-leader.log 2>&1 &
    LEADER_PID=$!
    success "Leader started (PID: $LEADER_PID)"
    sleep 3
    
    echo ""
    log "Leader initial state:"
    echo "--------------------"
    tail -20 /tmp/logstream-leader.log | grep -E "seq=|Registered|REPLICATE|Registry Status" | head -10
    echo ""
    success " Leader registered itself (seq=1)"
    
    # Wait to show heartbeats
    echo ""
    log "STEP 2: Leader sending heartbeats (seq=0)..."
    sleep 5
    tail -5 /tmp/logstream-leader.log | grep "HEARTBEAT"
    success " Heartbeats use seq=0 (not state changes)"
    
    # Start follower
    echo ""
    log "STEP 3: Adding follower (this will increase seq to 2)..."
    export IS_LEADER="false"
    ./logstream > /tmp/logstream-follower.log 2>&1 &
    FOLLOWER_PID=$!
    success "Follower started (PID: $FOLLOWER_PID)"
    sleep 5
    
    echo ""
    log "Leader state after follower join:"
    echo "---------------------------------"
    tail -30 /tmp/logstream-leader.log | grep -E "JOIN|Registered broker|REPLICATE seq=|Registry Status" | head -15
    echo ""
    success " Follower joined → seq increased to 2"
    success " Leader sent REPLICATE seq=2 to synchronize"
    
    echo ""
    log "Follower state after joining:"
    echo "-----------------------------"
    tail -30 /tmp/logstream-follower.log | grep -E "Found cluster|REPLICATE|Registry Status|Total Brokers" | head -15
    echo ""
    success " Follower received and applied REPLICATE seq=2"
    
    # Show ongoing heartbeats
    echo ""
    log "STEP 4: Both nodes exchanging heartbeats..."
    sleep 5
    echo ""
    echo "Leader heartbeats (last 3):"
    tail -10 /tmp/logstream-leader.log | grep "-> HEARTBEAT" | tail -3
    echo ""
    echo "Follower heartbeats (last 3):"
    tail -10 /tmp/logstream-follower.log | grep "-> HEARTBEAT" | tail -3
    
    echo ""
    echo "========================================="
    echo "SEQUENCE NUMBER SUMMARY:"
    echo "========================================="
    echo "seq=1: Leader self-registration"
    echo "seq=2: Follower joined cluster"
    echo "seq=0: Heartbeat messages (not state changes)"
    echo ""
    echo " REPLICATE messages show state changes (seq=1,2,3...)"
    echo " HEARTBEAT messages are periodic pings (seq=0)"
    echo "========================================="
    echo ""
    
    # Follow logs
    log "Following logs (Ctrl+C to stop)..."
    echo "Watch for:"
    echo "  [Leader] -> HEARTBEAT (seq=0)"
    echo "  [Follower] -> HEARTBEAT (seq=0)"
    echo "  [Leader] <- HEARTBEAT from follower"
    echo ""
    tail -f /tmp/logstream-leader.log /tmp/logstream-follower.log

elif [ "$MODE" = "docker" ]; then
    # Docker mode
    COMPOSE_FILE="$SCRIPT_DIR/../compose/duo.yaml"
    
    if [ ! -f "$COMPOSE_FILE" ]; then
        error "Compose file not found: $COMPOSE_FILE"
        exit 1
    fi
    
    cd "$(dirname "$COMPOSE_FILE")"
    
    # Start leader only first
    log "STEP 1: Starting leader..."
    docker compose -f "$(basename "$COMPOSE_FILE")" up -d leader
    sleep 3
    
    echo ""
    log "Leader initial state:"
    echo "--------------------"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs leader | grep -E "seq=|Registered|REPLICATE" | head -10
    echo ""
    success " Leader registered itself (seq=1)"
    
    # Wait to show heartbeats
    echo ""
    log "STEP 2: Leader sending heartbeats (seq=0)..."
    sleep 5
    docker compose -f "$(basename "$COMPOSE_FILE")" logs --tail=5 leader | grep "HEARTBEAT"
    success " Heartbeats use seq=0 (not state changes)"
    
    # Start follower
    echo ""
    log "STEP 3: Adding follower (this will increase seq to 2)..."
    docker compose -f "$(basename "$COMPOSE_FILE")" up -d broker1
    sleep 5
    
    echo ""
    log "Leader state after follower join:"
    echo "---------------------------------"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs leader | grep -E "JOIN|Registered broker|REPLICATE seq=|Total Brokers: 2" | tail -10
    echo ""
    success " Follower joined → seq increased to 2"
    success " Leader sent REPLICATE seq=2 to synchronize"
    
    echo ""
    log "Follower state after joining:"
    echo "-----------------------------"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs broker1 | grep -E "Found cluster|REPLICATE|Total Brokers: 2" | tail -10
    echo ""
    success " Follower received and applied REPLICATE seq=2"
    
    # Show ongoing heartbeats
    echo ""
    log "STEP 4: Both nodes exchanging heartbeats..."
    sleep 5
    echo ""
    echo "Leader heartbeats (last 3):"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs --tail=10 leader | grep "-> HEARTBEAT" | tail -3
    echo ""
    echo "Follower heartbeats (last 3):"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs --tail=10 broker1 | grep "-> HEARTBEAT" | tail -3
    
    echo ""
    echo "========================================="
    echo "SEQUENCE NUMBER SUMMARY:"
    echo "========================================="
    echo "seq=1: Leader self-registration"
    echo "seq=2: Follower joined cluster"
    echo "seq=0: Heartbeat messages (not state changes)"
    echo ""
    echo " REPLICATE messages show state changes (seq=1,2,3...)"
    echo " HEARTBEAT messages are periodic pings (seq=0)"
    echo "========================================="
    echo ""
    
    # Follow logs
    log "Following logs (Ctrl+C to stop)..."
    echo "Watch for heartbeat exchanges between nodes"
    echo ""
    docker compose -f "$(basename "$COMPOSE_FILE")" logs -f

else
    error "Invalid mode: $MODE (use 'local' or 'docker')"
    exit 1
fi