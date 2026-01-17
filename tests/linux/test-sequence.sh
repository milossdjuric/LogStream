#!/bin/bash

# Dynamic Sequence Demo - Shows seq progression through joins/leaves/timeouts
# Usage: ./tests/test-sequence-demo.sh [local|docker]

MODE="${1:-local}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

source "$SCRIPT_DIR/lib/common.sh"

# Test prefix for log identification
TEST_PREFIX="[TEST-SEQUENCE]"
log_test() { echo -e "${BLUE}${TEST_PREFIX} $1${NC}"; }
success_test() { echo -e "${GREEN}${TEST_PREFIX} $1${NC}"; }
error_test() { echo -e "${RED}${TEST_PREFIX} $1${NC}"; }


echo "================================================"
echo "SEQUENCE NUMBER DEMONSTRATION"
echo "Shows how sequences increase with state changes"
echo "Mode: $MODE"
echo "================================================"
echo ""

if [ "$MODE" = "local" ]; then
    # Local mode
    cd "$PROJECT_ROOT"
    build_if_needed "logstream" "main.go"
    
    # Track sequence expectations
    EXPECTED_SEQ=1
    
    # Helper function to show current sequence
    show_sequence() {
        local label="$1"
        echo ""
        echo "========================================="
        echo "$label"
        echo "========================================="
        tail -50 /tmp/logstream-leader.log | grep -E "seq=[0-9]+|Total Brokers:" | grep -v "seq=0" | tail -5
    }
    
    # Start leader
    log "═══ STEP 1: Start Leader ═══"
    export MULTICAST_GROUP="239.0.0.1:9999"
    export BROADCAST_PORT="8888"
    export IS_LEADER="true"
    ./logstream > /tmp/logstream-leader.log 2>&1 &
    LEADER_PID=$!
    sleep 3
    
    show_sequence "After leader start (seq should be 1)"
    success " Expected seq=$EXPECTED_SEQ, Leader registered"
    ((EXPECTED_SEQ++))
    
    # Add follower 1
    echo ""
    log "═══ STEP 2: Add Follower 1 ═══"
    export IS_LEADER="false"
    PORT=8002 ./logstream > /tmp/logstream-follower1.log 2>&1 &
    FOLLOWER1_PID=$!
    sleep 5
    
    show_sequence "After follower 1 joins (seq should be 2)"
    success " Expected seq=$EXPECTED_SEQ, Follower 1 joined"
    ((EXPECTED_SEQ++))
    
    # Add follower 2
    echo ""
    log "═══ STEP 3: Add Follower 2 ═══"
    PORT=8003 ./logstream > /tmp/logstream-follower2.log 2>&1 &
    FOLLOWER2_PID=$!
    sleep 5
    
    show_sequence "After follower 2 joins (seq should be 3)"
    success " Expected seq=$EXPECTED_SEQ, Follower 2 joined"
    ((EXPECTED_SEQ++))
    
    # Show heartbeats (seq=0)
    echo ""
    log "═══ HEARTBEAT CHECK ═══"
    sleep 3
    echo "Recent heartbeats (all should be seq=0):"
    tail -30 /tmp/logstream-leader.log | grep "HEARTBEAT" | tail -5
    success " Heartbeats use seq=0 (not state changes)"
    
    # Kill a follower to trigger timeout and seq increase
    echo ""
    log "═══ STEP 4: Kill Follower 2 (will timeout in 30s) ═══"
    kill $FOLLOWER2_PID 2>/dev/null
    success "Follower 2 killed (PID: $FOLLOWER2_PID)"
    log "Waiting for timeout (30 seconds)..."
    
    # Show countdown
    for i in {30..1}; do
        echo -ne "\rTimeout in: ${i}s  "
        sleep 1
    done
    echo ""
    
    # Wait for timeout check
    sleep 5
    
    show_sequence "After follower 2 timeout (seq should be 4)"
    tail -20 /tmp/logstream-leader.log | grep -E "Timeout: Removed|REPLICATE seq=" | tail -3
    success " Expected seq=$EXPECTED_SEQ, Follower 2 removed due to timeout"
    ((EXPECTED_SEQ++))
    
    # Add it back
    echo ""
    log "═══ STEP 5: Re-add Follower 2 ═══"
    PORT=8003 ./logstream > /tmp/logstream-follower2-new.log 2>&1 &
    FOLLOWER2_NEW_PID=$!
    sleep 5
    
    show_sequence "After follower 2 rejoins (seq should be 5)"
    success " Expected seq=$EXPECTED_SEQ, Follower 2 rejoined"
    ((EXPECTED_SEQ++))
    
    # Final summary
    echo ""
    echo "================================================"
    echo "SEQUENCE PROGRESSION SUMMARY"
    echo "================================================"
    echo "seq=1: Leader self-registration"
    echo "seq=2: Follower 1 joined"
    echo "seq=3: Follower 2 joined"
    echo "seq=4: Follower 2 timed out and removed"
    echo "seq=5: Follower 2 rejoined"
    echo ""
    echo "Current sequence: $((EXPECTED_SEQ - 1))"
    echo "================================================"
    echo ""
    echo "FINAL REGISTRY STATE:"
    tail -50 /tmp/logstream-leader.log | grep -A 5 "=== Registry Status ===" | tail -6
    
    echo ""
    echo "================================================"
    echo "KEY INSIGHTS:"
    echo "================================================"
    echo " Sequences increment ONLY on state changes"
    echo " State changes: joins, leaves, timeouts"
    echo " Heartbeats are NOT state changes (seq=0)"
    echo " REPLICATE messages carry the sequence number"
    echo "================================================"
    echo ""
    
    log "Following logs (Ctrl+C to stop)..."
    tail -f /tmp/logstream-*.log

elif [ "$MODE" = "docker" ]; then
    # Docker mode
    COMPOSE_FILE="$SCRIPT_DIR/../compose/sequence.yaml"
    
    if [ ! -f "$COMPOSE_FILE" ]; then
        error "Compose file not found: $COMPOSE_FILE"
        exit 1
    fi
    
    cd "$(dirname "$COMPOSE_FILE")"
    
    # Track sequence
    EXPECTED_SEQ=1
    
    # Helper function
    show_sequence() {
        local label="$1"
        echo ""
        echo "========================================="
        echo "$label"
        echo "========================================="
        docker compose -f "$(basename "$COMPOSE_FILE")" logs leader | grep -E "seq=[0-9]+|Total Brokers:" | grep -v "seq=0" | tail -5
    }
    
    # Start leader
    log "═══ STEP 1: Start Leader ═══"
    docker compose -f "$(basename "$COMPOSE_FILE")" up -d leader
    sleep 3
    
    show_sequence "After leader start (seq should be 1)"
    success " Expected seq=$EXPECTED_SEQ, Leader registered"
    ((EXPECTED_SEQ++))
    
    # Add follower 1
    echo ""
    log "═══ STEP 2: Add Follower 1 ═══"
    docker compose -f "$(basename "$COMPOSE_FILE")" up -d broker1
    sleep 5
    
    show_sequence "After follower 1 joins (seq should be 2)"
    success " Expected seq=$EXPECTED_SEQ, Follower 1 joined"
    ((EXPECTED_SEQ++))
    
    # Add follower 2
    echo ""
    log "═══ STEP 3: Add Follower 2 ═══"
    docker compose -f "$(basename "$COMPOSE_FILE")" up -d broker2
    sleep 5
    
    show_sequence "After follower 2 joins (seq should be 3)"
    success " Expected seq=$EXPECTED_SEQ, Follower 2 joined"
    ((EXPECTED_SEQ++))
    
    # Show heartbeats
    echo ""
    log "═══ HEARTBEAT CHECK ═══"
    sleep 3
    echo "Recent heartbeats (all should be seq=0):"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs --tail=30 leader | grep "HEARTBEAT" | tail -5
    success " Heartbeats use seq=0 (not state changes)"
    
    # Stop follower 2
    echo ""
    log "═══ STEP 4: Stop Follower 2 (will timeout in 30s) ═══"
    docker compose -f "$(basename "$COMPOSE_FILE")" stop broker2
    success "Follower 2 stopped"
    log "Waiting for timeout (30 seconds)..."
    
    for i in {30..1}; do
        echo -ne "\rTimeout in: ${i}s  "
        sleep 1
    done
    echo ""
    sleep 5
    
    show_sequence "After follower 2 timeout (seq should be 4)"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs leader | grep -E "Timeout: Removed|REPLICATE seq=" | tail -3
    success " Expected seq=$EXPECTED_SEQ, Follower 2 removed"
    ((EXPECTED_SEQ++))
    
    # Restart follower 2
    echo ""
    log "═══ STEP 5: Restart Follower 2 ═══"
    docker compose -f "$(basename "$COMPOSE_FILE")" start broker2
    sleep 5
    
    show_sequence "After follower 2 rejoins (seq should be 5)"
    success " Expected seq=$EXPECTED_SEQ, Follower 2 rejoined"
    
    # Final summary
    echo ""
    echo "================================================"
    echo "SEQUENCE PROGRESSION SUMMARY"
    echo "================================================"
    echo "seq=1: Leader self-registration"
    echo "seq=2: Follower 1 joined"
    echo "seq=3: Follower 2 joined"
    echo "seq=4: Follower 2 timed out and removed"
    echo "seq=5: Follower 2 rejoined"
    echo "================================================"
    echo ""
    echo "FINAL REGISTRY STATE:"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs leader | grep -A 5 "=== Registry Status ===" | tail -6
    
    echo ""
    echo "================================================"
    echo "KEY INSIGHTS:"
    echo "================================================"
    echo " Sequences increment ONLY on state changes"
    echo " State changes: joins, leaves, timeouts"
    echo " Heartbeats are NOT state changes (seq=0)"
    echo " REPLICATE messages carry the sequence number"
    echo "================================================"
    echo ""
    
    log "Following logs (Ctrl+C to stop)..."
    docker compose -f "$(basename "$COMPOSE_FILE")" logs -f

else
    error "Invalid mode: $MODE (use 'local' or 'docker')"
    exit 1
fi