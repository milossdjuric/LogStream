#!/bin/bash

# Enhanced Trio Test - Shows Sequence 1 → 2 → 3
# Usage: ./tests/test-trio-enhanced.sh [local|docker]

MODE="${1:-local}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

source "$SCRIPT_DIR/lib/common.sh"

# Test prefix for log identification
TEST_PREFIX="[TEST-TRIO]"
log_test() { echo -e "${BLUE}${TEST_PREFIX} $1${NC}"; }
success_test() { echo -e "${GREEN}${TEST_PREFIX} $1${NC}"; }
error_test() { echo -e "${RED}${TEST_PREFIX} $1${NC}"; }


echo "================================================"
echo "Test: Sequential Broker Joins (seq=1→2→3)"
echo "Mode: $MODE"
echo "================================================"
echo ""

if [ "$MODE" = "local" ]; then
    # Local mode
    cd "$PROJECT_ROOT"
    
    # Build
    build_if_needed "logstream" "main.go"
    
    # STEP 1: Leader only (seq=1)
    log "STEP 1: Starting leader (seq will be 1)..."
    export MULTICAST_GROUP="239.0.0.1:9999"
    export BROADCAST_PORT="8888"
    export IS_LEADER="true"
    ./logstream > /tmp/logstream-leader.log 2>&1 &
    LEADER_PID=$!
    success "Leader started (PID: $LEADER_PID)"
    sleep 3
    
    echo ""
    log "Current state - Leader only:"
    tail -20 /tmp/logstream-leader.log | grep -E "Registered broker|seq=" | head -5
    success " seq=1: Leader registered itself"
    
    sleep 3
    
    # STEP 2: Add follower 1 (seq=2)
    echo ""
    log "STEP 2: Adding follower 1 (seq will increase to 2)..."
    export IS_LEADER="false"
    PORT=8002 ./logstream > /tmp/logstream-follower1.log 2>&1 &
    FOLLOWER1_PID=$!
    success "Follower 1 started (PID: $FOLLOWER1_PID)"
    sleep 5
    
    echo ""
    log "Current state - Leader + Follower 1:"
    tail -40 /tmp/logstream-leader.log | grep -E "JOIN from|Registered broker|REPLICATE seq=|Total Brokers: 2" | head -10
    success " seq=2: Follower 1 joined, REPLICATE seq=2 sent"
    
    sleep 3
    
    # STEP 3: Add follower 2 (seq=3)
    echo ""
    log "STEP 3: Adding follower 2 (seq will increase to 3)..."
    PORT=8003 ./logstream > /tmp/logstream-follower2.log 2>&1 &
    FOLLOWER2_PID=$!
    success "Follower 2 started (PID: $FOLLOWER2_PID)"
    sleep 5
    
    echo ""
    log "Current state - Leader + 2 Followers:"
    tail -40 /tmp/logstream-leader.log | grep -E "JOIN from|Registered broker|REPLICATE seq=|Total Brokers: 3" | head -10
    success " seq=3: Follower 2 joined, REPLICATE seq=3 sent"
    
    # Show final state on all nodes
    echo ""
    echo "========================================="
    echo "FINAL STATE - ALL NODES:"
    echo "========================================="
    echo ""
    echo "Leader registry:"
    tail -50 /tmp/logstream-leader.log | grep -A 5 "=== Registry Status ===" | tail -6
    
    echo ""
    echo "Follower 1 registry:"
    tail -50 /tmp/logstream-follower1.log | grep -A 5 "=== Registry Status ===" | tail -6
    
    echo ""
    echo "Follower 2 registry:"
    tail -50 /tmp/logstream-follower2.log | grep -A 5 "=== Registry Status ===" | tail -6
    
    echo ""
    echo "========================================="
    echo "SEQUENCE PROGRESSION:"
    echo "========================================="
    echo "seq=1: Leader self-registration"
    echo "seq=2: Follower 1 joined"
    echo "seq=3: Follower 2 joined"
    echo ""
    echo "All nodes synchronized at seq=3 "
    echo "========================================="
    echo ""
    
    # Show heartbeats
    log "Current heartbeat exchange (seq=0 for all):"
    sleep 3
    echo ""
    echo "Last 3 heartbeats from each node:"
    echo ""
    echo "Leader:"
    tail -20 /tmp/logstream-leader.log | grep "-> HEARTBEAT" | tail -3
    echo ""
    echo "Follower 1:"
    tail -20 /tmp/logstream-follower1.log | grep "-> HEARTBEAT" | tail -3
    echo ""
    echo "Follower 2:"
    tail -20 /tmp/logstream-follower2.log | grep "-> HEARTBEAT" | tail -3
    
    echo ""
    echo "========================================="
    echo "KEY TAKEAWAY:"
    echo "========================================="
    echo " REPLICATE messages: seq=1,2,3... (state changes)"
    echo " HEARTBEAT messages: seq=0 (periodic pings)"
    echo ""
    echo "Sequences only increase when state changes!"
    echo "========================================="
    echo ""
    
    # Follow all logs
    log "Following all logs (Ctrl+C to stop)..."
    tail -f /tmp/logstream-leader.log /tmp/logstream-follower1.log /tmp/logstream-follower2.log

elif [ "$MODE" = "docker" ]; then
    # Docker mode
    COMPOSE_FILE="$SCRIPT_DIR/../compose/trio.yaml"
    
    if [ ! -f "$COMPOSE_FILE" ]; then
        error "Compose file not found: $COMPOSE_FILE"
        exit 1
    fi
    
    cd "$(dirname "$COMPOSE_FILE")"
    
    # STEP 1: Leader only
    log "STEP 1: Starting leader (seq will be 1)..."
    docker compose -f "$(basename "$COMPOSE_FILE")" up -d leader
    sleep 3
    
    echo ""
    log "Current state - Leader only:"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs leader | grep -E "Registered broker|seq=" | head -5
    success " seq=1: Leader registered itself"
    
    sleep 3
    
    # STEP 2: Add follower 1
    echo ""
    log "STEP 2: Adding follower 1 (seq will increase to 2)..."
    docker compose -f "$(basename "$COMPOSE_FILE")" up -d broker1
    sleep 5
    
    echo ""
    log "Current state - Leader + Follower 1:"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs leader | grep -E "JOIN|Registered broker|REPLICATE seq=|Total Brokers: 2" | tail -10
    success " seq=2: Follower 1 joined, REPLICATE seq=2 sent"
    
    sleep 3
    
    # STEP 3: Add follower 2
    echo ""
    log "STEP 3: Adding follower 2 (seq will increase to 3)..."
    docker compose -f "$(basename "$COMPOSE_FILE")" up -d broker2
    sleep 5
    
    echo ""
    log "Current state - Leader + 2 Followers:"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs leader | grep -E "JOIN|Registered broker|REPLICATE seq=|Total Brokers: 3" | tail -10
    success " seq=3: Follower 2 joined, REPLICATE seq=3 sent"
    
    # Show final state
    echo ""
    echo "========================================="
    echo "FINAL STATE - ALL NODES:"
    echo "========================================="
    echo ""
    echo "Leader registry:"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs leader | grep -A 5 "=== Registry Status ===" | tail -6
    
    echo ""
    echo "Follower 1 registry:"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs broker1 | grep -A 5 "=== Registry Status ===" | tail -6
    
    echo ""
    echo "Follower 2 registry:"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs broker2 | grep -A 5 "=== Registry Status ===" | tail -6
    
    echo ""
    echo "========================================="
    echo "SEQUENCE PROGRESSION:"
    echo "========================================="
    echo "seq=1: Leader self-registration"
    echo "seq=2: Follower 1 joined"
    echo "seq=3: Follower 2 joined"
    echo ""
    echo "All nodes synchronized at seq=3 "
    echo "========================================="
    echo ""
    
    # Show heartbeats
    log "Current heartbeat exchange (seq=0 for all):"
    sleep 3
    echo ""
    echo "Last 3 heartbeats from each node:"
    echo ""
    echo "Leader:"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs --tail=20 leader | grep "-> HEARTBEAT" | tail -3
    echo ""
    echo "Follower 1:"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs --tail=20 broker1 | grep "-> HEARTBEAT" | tail -3
    echo ""
    echo "Follower 2:"
    docker compose -f "$(basename "$COMPOSE_FILE")" logs --tail=20 broker2 | grep "-> HEARTBEAT" | tail -3
    
    echo ""
    echo "========================================="
    echo "KEY TAKEAWAY:"
    echo "========================================="
    echo " REPLICATE messages: seq=1,2,3... (state changes)"
    echo " HEARTBEAT messages: seq=0 (periodic pings)"
    echo ""
    echo "Sequences only increase when state changes!"
    echo "========================================="
    echo ""
    
    # Follow all logs
    log "Following all logs (Ctrl+C to stop)..."
    docker compose -f "$(basename "$COMPOSE_FILE")" logs -f

else
    error "Invalid mode: $MODE (use 'local' or 'docker')"
    exit 1
fi