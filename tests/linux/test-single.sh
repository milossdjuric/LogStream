#!/bin/bash

# Enhanced Single Leader Test - Shows Initial Sequence
# Usage: ./tests/test-single-enhanced.sh [local|docker]

MODE="${1:-local}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

source "$SCRIPT_DIR/lib/common.sh"

# Test prefix for log identification
TEST_PREFIX="[TEST-SINGLE]"
log_test() { echo -e "${BLUE}${TEST_PREFIX} $1${NC}"; }
success_test() { echo -e "${GREEN}${TEST_PREFIX} $1${NC}"; }
error_test() { echo -e "${RED}${TEST_PREFIX} $1${NC}"; }


echo "========================================"
echo "Test: Single Leader with Sequence Demo"
echo "Mode: $MODE"
echo "========================================"
echo ""

if [ "$MODE" = "local" ]; then
    # Local mode
    cd "$PROJECT_ROOT"
    
    # Build
    build_if_needed "logstream" "main.go"
    
    # Start leader
    log "Starting leader..."
    export MULTICAST_GROUP="239.0.0.1:9999"
    export BROADCAST_PORT="8888"
    export IS_LEADER="true"
    ./logstream > /tmp/logstream-leader.log 2>&1 &
    LEADER_PID=$!
    success "Leader started (PID: $LEADER_PID)"
    sleep 3
    
    # Show initial state
    echo ""
    log "Initial state - Leader only (seq=1)"
    echo "-----------------------------------"
    tail -20 /tmp/logstream-leader.log | grep -E "seq=|Registered|REPLICATE"
    
    # Wait and show heartbeats (seq=0)
    echo ""
    log "Heartbeat phase (seq=0 is correct for heartbeats)"
    echo "--------------------------------------------------"
    sleep 5
    tail -10 /tmp/logstream-leader.log | grep -E "HEARTBEAT|seq="
    
    echo ""
    log " Notice: HEARTBEAT messages use seq=0 (they're not state changes)"
    log " REPLICATE messages use incrementing seq (they ARE state changes)"
    
    # Follow logs
    echo ""
    log "Following logs (Ctrl+C to stop)..."
    echo "Look for:"
    echo "  - HEARTBEAT messages (seq=0) ← Periodic pings"
    echo "  - REPLICATE messages (seq=1+) ← State synchronization"
    echo ""
    tail -f /tmp/logstream-leader.log

elif [ "$MODE" = "docker" ]; then
    # Docker mode
    COMPOSE_FILE="$SCRIPT_DIR/../compose/single.yaml"
    
    if [ ! -f "$COMPOSE_FILE" ]; then
        error "Compose file not found: $COMPOSE_FILE"
        exit 1
    fi
    
    cd "$(dirname "$COMPOSE_FILE")"
    
    # Start leader
    log "Starting leader..."
    docker compose -f "$(basename "$COMPOSE_FILE")" up -d
    sleep 3
    
    # Show initial state
    echo ""
    log "Initial state - Leader only (seq=1)"
    echo "-----------------------------------"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs | grep -E "seq=|Registered|REPLICATE" | head -20
    
    # Wait and show heartbeats
    echo ""
    log "Heartbeat phase (seq=0 is correct for heartbeats)"
    echo "--------------------------------------------------"
    sleep 5
    docker compose -f "$(basename "$COMPOSE_FILE")" logs --tail=10 | grep -E "HEARTBEAT|seq="
    
    echo ""
    log " Notice: HEARTBEAT messages use seq=0 (they're not state changes)"
    log " REPLICATE messages use incrementing seq (they ARE state changes)"
    
    # Follow logs
    echo ""
    log "Following logs (Ctrl+C to stop)..."
    echo "Look for:"
    echo "  - HEARTBEAT messages (seq=0) ← Periodic pings"
    echo "  - REPLICATE messages (seq=1+) ← State synchronization"
    echo ""
    docker compose -f "$(basename "$COMPOSE_FILE")" logs -f

else
    error "Invalid mode: $MODE (use 'local' or 'docker')"
    exit 1
fi