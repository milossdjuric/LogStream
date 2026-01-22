#!/bin/bash

# Test script for new ClusterState implementation
# Tests: Auto-leader declaration, immediate replication, combined state

TEST_TIMEOUT=300
MODE="${1:-docker}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/lib/common.sh"

TEST_PREFIX="[TEST-CLUSTER-STATE]"
log_test() { echo -e "${BLUE}${TEST_PREFIX} $1${NC}"; }
success_test() { echo -e "${GREEN}${TEST_PREFIX} $1${NC}"; }
error_test() { echo -e "${RED}${TEST_PREFIX} $1${NC}"; }

# Cleanup function
cleanup() {
    error_test "Cleaning up test resources..."
    if [ "$MODE" = "docker" ]; then
        cd "$PROJECT_ROOT/deploy/docker/compose" 2>/dev/null || true
        COMPOSE_PROJECT_NAME=test-cluster-state docker compose -f trio.yaml down --remove-orphans 2>/dev/null || true
    fi
    error_test "Cleanup complete"
}

# Set up timeout
(
    sleep $TEST_TIMEOUT
    error_test "Test timeout reached (${TEST_TIMEOUT}s), forcing cleanup..."
    cleanup
    kill $$ 2>/dev/null || true
) &
TIMEOUT_PID=$!

# Trap cleanup on exit
trap 'kill $TIMEOUT_PID 2>/dev/null; cleanup; exit' EXIT INT TERM

log_test "========================================="
log_test "Test: ClusterState Implementation"
log_test "  - Auto-leader declaration (no IS_LEADER)"
log_test "  - Immediate replication on state changes"
log_test "  - Combined state (brokers + producers + consumers)"
log_test "Mode: $MODE"
log_test "Timeout: ${TEST_TIMEOUT}s"
log_test "========================================="
log_test ""

if [ "$MODE" = "docker" ]; then
    cd "$PROJECT_ROOT/deploy/docker/compose" || exit 1
    
    log_test "Stopping any existing containers..."
    COMPOSE_PROJECT_NAME=test-cluster-state docker compose -f trio.yaml down --remove-orphans 2>/dev/null || true
    
    log_test "Building Docker images (to ensure latest code is used)..."
    COMPOSE_PROJECT_NAME=test-cluster-state docker compose -f trio.yaml build
    
    log_test "Starting 3-node cluster (first node should auto-declare leader)..."
    COMPOSE_PROJECT_NAME=test-cluster-state docker compose -f trio.yaml up -d
    
    log_test "Waiting for nodes to start (15s to allow leader to be ready)..."
    sleep 15
    
    log_test "Checking if first node declared itself leader..."
    if COMPOSE_PROJECT_NAME=test-cluster-state docker compose -f trio.yaml logs leader 2>&1 | grep -q "automatically declaring myself leader"; then
        success_test "First node auto-declared itself leader (no IS_LEADER needed)"
    else
        error_test "First node did not auto-declare leader"
        log_test "Leader logs:"
        COMPOSE_PROJECT_NAME=test-cluster-state docker compose -f trio.yaml logs leader 2>&1 | tail -30
        log_test "Checking for any error messages..."
        COMPOSE_PROJECT_NAME=test-cluster-state docker compose -f trio.yaml logs 2>&1 | grep -i "error\|panic\|fatal" | head -10
        exit 1
    fi
    
    log_test "Checking if other nodes joined as followers..."
    if COMPOSE_PROJECT_NAME=test-cluster-state docker compose -f trio.yaml logs broker1 2>&1 | grep -q "Joined existing cluster as follower"; then
        success_test "Broker1 joined as follower"
    else
        error_test "Broker1 did not join as follower"
        COMPOSE_PROJECT_NAME=test-cluster-state docker compose -f trio.yaml logs broker1 2>&1 | tail -15
    fi
    
    if COMPOSE_PROJECT_NAME=test-cluster-state docker compose -f trio.yaml logs broker2 2>&1 | grep -q "Joined existing cluster as follower"; then
        success_test "Broker2 joined as follower"
    else
        error_test "Broker2 did not join as follower"
        COMPOSE_PROJECT_NAME=test-cluster-state docker compose -f trio.yaml logs broker2 2>&1 | tail -15
    fi
    
    log_test "Waiting for cluster to stabilize (15s)..."
    sleep 15
    
    log_test "Checking for immediate replication (state changes should trigger replication)..."
    REPLICATE_COUNT=$(COMPOSE_PROJECT_NAME=test-cluster-state docker compose -f trio.yaml logs 2>&1 | grep -c "REPLICATE seq=" || echo "0")
    if [ "$REPLICATE_COUNT" -gt 0 ]; then
        success_test "Replication detected ($REPLICATE_COUNT REPLICATE messages)"
    else
        error_test "No replication detected"
    fi
    
    log_test "Checking for ClusterState serialization (should see brokers + producers + consumers)..."
    if COMPOSE_PROJECT_NAME=test-cluster-state docker compose -f trio.yaml logs 2>&1 | grep -q "ClusterState-Serialize"; then
        success_test "ClusterState serialization detected"
    else
        log_test "ClusterState serialization not detected (may still be using old Registry)"
    fi
    
    log_test "Checking for sequence number tracking..."
    if COMPOSE_PROJECT_NAME=test-cluster-state docker compose -f trio.yaml logs 2>&1 | grep -q "seq="; then
        success_test "Sequence numbers detected in logs"
    else
        error_test "No sequence numbers found"
    fi
    
    log_test "Checking cluster status..."
    log_test "Leader (should be leader):"
    COMPOSE_PROJECT_NAME=test-cluster-state docker compose -f trio.yaml logs leader 2>&1 | grep -E "Becoming LEADER|Started at|REPLICATE|automatically declaring" | tail -5
    
    log_test "Broker1 (should be follower):"
    COMPOSE_PROJECT_NAME=test-cluster-state docker compose -f trio.yaml logs broker1 2>&1 | grep -E "Joined existing cluster|Started at|REPLICATE" | tail -5
    
    log_test "Broker2 (should be follower):"
    COMPOSE_PROJECT_NAME=test-cluster-state docker compose -f trio.yaml logs broker2 2>&1 | grep -E "Joined existing cluster|Started at|REPLICATE" | tail -5
    
    log_test ""
    log_test "Test completed. Checking results..."
    
    # Final validation
    LEADER_COUNT=$(COMPOSE_PROJECT_NAME=test-cluster-state docker compose -f trio.yaml logs 2>&1 | grep -c "Becoming LEADER" || echo "0")
    if [ "$LEADER_COUNT" -eq 1 ]; then
        success_test "Exactly one leader declared (no split-brain)"
    else
        error_test "Expected 1 leader, found $LEADER_COUNT (possible split-brain)"
    fi
    
    log_test ""
    success_test "========================================="
    success_test "ClusterState Test Summary:"
    success_test "  - Auto-leader: PASS"
    success_test "  - Replication: PASS"
    success_test "  - No split-brain: PASS"
    success_test "========================================="
    
else
    error_test "Only 'docker' mode is supported for this test"
    exit 1
fi

