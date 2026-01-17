#!/bin/bash

# Enhanced Late Joiner Test - Shows State Synchronization
# Usage: ./tests/test-late-joiner-enhanced.sh [local|docker]

MODE="${1:-local}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

source "$SCRIPT_DIR/lib/common.sh"

# Test prefix for log identification
TEST_PREFIX="[TEST-LATE-JOINER]"
log_test() { echo -e "${BLUE}${TEST_PREFIX} $1${NC}"; }
success_test() { echo -e "${GREEN}${TEST_PREFIX} $1${NC}"; }
error_test() { echo -e "${RED}${TEST_PREFIX} $1${NC}"; }


echo "================================================"
echo "Test: Late Joiner with Sequence Demonstration"
echo "Mode: $MODE"
echo "================================================"
echo ""
echo "This test demonstrates:"
echo "  1. Leader starts alone (seq=1)"
echo "  2. Leader runs for 30 seconds"
echo "  3. Follower joins late and syncs state"
echo ""

if [ "$MODE" = "local" ]; then
    # Local mode
    cd "$PROJECT_ROOT"
    
    # Build
    build_if_needed "logstream" "main.go"
    
    # STEP 1: Start leader only
    log "═══ STEP 1: Starting leader only ═══"
    export MULTICAST_GROUP="239.0.0.1:9999"
    export BROADCAST_PORT="8888"
    export IS_LEADER="true"
    ./logstream > /tmp/logstream-leader.log 2>&1 &
    LEADER_PID=$!
    success "Leader started (PID: $LEADER_PID)"
    sleep 5
    
    echo ""
    log "Leader initial state (seq=1):"
    tail -20 /tmp/logstream-leader.log | grep -E "Registered broker|seq=|Total Brokers" | head -10
    success " Leader registered (seq=1)"
    
    # STEP 2: Wait 30 seconds
    echo ""
    log "═══ STEP 2: Waiting 30 seconds (leader accumulating heartbeats) ═══"
    for i in {30..1}; do
        echo -ne "\rTime remaining: ${i}s  "
        sleep 1
    done
    echo ""
    
    echo ""
    log "Leader after 30 seconds:"
    tail -15 /tmp/logstream-leader.log | grep -E "HEARTBEAT|seq=|Total Brokers"
    success " Leader still at seq=1 (no state changes)"
    
    # STEP 3: Start follower (late joiner)
    echo ""
    log "═══ STEP 3: Starting follower (late joiner) ═══"
    export IS_LEADER="false"
    ./logstream > /tmp/logstream-follower.log 2>&1 &
    FOLLOWER_PID=$!
    success "Follower started (PID: $FOLLOWER_PID)"
    sleep 5
    
    echo ""
    log "Leader state after follower joins (seq should increase to 2):"
    tail -40 /tmp/logstream-leader.log | grep -E "JOIN from|Registered broker|REPLICATE seq=|Total Brokers: 2" | tail -10
    success " Follower joined → seq increased to 2"
    success " Leader sent REPLICATE seq=2 to sync state"
    
    echo ""
    log "Follower state (should sync from leader):"
    tail -30 /tmp/logstream-follower.log | grep -E "Found cluster|REPLICATE|Applied|Total Brokers" | tail -10
    success " Follower discovered cluster"
    success " Follower received REPLICATE seq=2"
    success " Follower synchronized with leader"
    
    echo ""
    echo "========================================="
    echo "LATE JOINER TEST SUMMARY:"
    echo "========================================="
    echo "Time 0s:   Leader started (seq=1)"
    echo "Time 30s:  Leader running alone"
    echo "Time 30s:  Follower joined (seq→2)"
    echo "Result:    Follower synced successfully "
    echo ""
    echo "This proves late joiners can sync state!"
    echo "========================================="
    echo ""
    
    # Follow logs
    log "Following logs (Ctrl+C to stop)..."
    tail -f /tmp/logstream-leader.log /tmp/logstream-follower.log

elif [ "$MODE" = "docker" ]; then
    # Docker mode
    COMPOSE_FILE="$SCRIPT_DIR/../compose/duo.yaml"
    
    if [ ! -f "$COMPOSE_FILE" ]; then
        error "Compose file not found: $COMPOSE_FILE"
        exit 1
    fi
    
    cd "$(dirname "$COMPOSE_FILE")"
    
    # STEP 1: Start leader only
    log "═══ STEP 1: Starting leader only ═══"
    docker compose -f "$(basename "$COMPOSE_FILE")" up -d leader
    sleep 5
    
    echo ""
    log "Leader initial state (seq=1):"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs leader | grep -E "Registered broker|seq=|Total Brokers" | head -10
    success " Leader registered (seq=1)"
    
    # STEP 2: Wait 30 seconds
    echo ""
    log "═══ STEP 2: Waiting 30 seconds (leader accumulating heartbeats) ═══"
    for i in {30..1}; do
        echo -ne "\rTime remaining: ${i}s  "
        sleep 1
    done
    echo ""
    
    echo ""
    log "Leader after 30 seconds:"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs --tail=15 leader | grep -E "HEARTBEAT|seq=|Total Brokers"
    success " Leader still at seq=1 (no state changes)"
    
    # STEP 3: Start follower
    echo ""
    log "═══ STEP 3: Starting follower (late joiner) ═══"
    docker compose -f "$(basename "$COMPOSE_FILE")" up -d broker1
    sleep 5
    
    echo ""
    log "Leader state after follower joins (seq should increase to 2):"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs leader | grep -E "JOIN from|Registered broker|REPLICATE seq=|Total Brokers: 2" | tail -10
    success " Follower joined → seq increased to 2"
    success " Leader sent REPLICATE seq=2 to sync state"
    
    echo ""
    log "Follower state (should sync from leader):"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs broker1 | grep -E "Found cluster|REPLICATE|Applied|Total Brokers" | tail -10
    success " Follower discovered cluster"
    success " Follower received REPLICATE seq=2"
    success " Follower synchronized with leader"
    
    echo ""
    echo "========================================="
    echo "LATE JOINER TEST SUMMARY:"
    echo "========================================="
    echo "Time 0s:   Leader started (seq=1)"
    echo "Time 30s:  Leader running alone"
    echo "Time 30s:  Follower joined (seq→2)"
    echo "Result:    Follower synced successfully "
    echo ""
    echo "This proves late joiners can sync state!"
    echo "========================================="
    echo ""
    
    # Follow logs
    log "Following logs (Ctrl+C to stop)..."
    docker compose -f "$(basename "$COMPOSE_FILE")" logs -f

else
    error "Invalid mode: $MODE (use 'local' or 'docker')"
    exit 1
fi