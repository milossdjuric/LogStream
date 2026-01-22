#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "========================================="
echo "Starting LogStream Cluster (netns)"
echo "========================================="

# Check if namespaces exist
if ! ip netns list | grep -q "logstream-a"; then
    echo "ERROR: Network namespaces not found!"
    echo "Run: sudo ./setup-netns.sh first"
    exit 1
fi

# Build binary
cd "$PROJECT_ROOT"
if [ ! -f "logstream" ]; then
    echo "Building logstream..."
    go build -o logstream main.go
fi

# Start nodes in namespaces
echo ""
echo "Starting nodes..."

sudo ip netns exec logstream-a ./logstream \
    -addr 172.20.0.10:8001 \
    -is-leader true \
    -multicast 239.0.0.1:9999 \
    > /tmp/logstream-a.log 2>&1 &
echo "  Node A (Leader):    172.20.0.10:8001"

sleep 3

sudo ip netns exec logstream-b ./logstream \
    -addr 172.20.0.20:8002 \
    -is-leader false \
    -multicast 239.0.0.1:9999 \
    > /tmp/logstream-b.log 2>&1 &
echo "  Node B (Follower):  172.20.0.20:8002"

sleep 3

sudo ip netns exec logstream-c ./logstream \
    -addr 172.20.0.30:8003 \
    -is-leader false \
    -multicast 239.0.0.1:9999 \
    > /tmp/logstream-c.log 2>&1 &
echo "  Node C (Follower):  172.20.0.30:8003"

echo ""
echo "[OK] Cluster started!"
echo ""
echo "View logs:"
echo "  tail -f /tmp/logstream-a.log"
echo "  tail -f /tmp/logstream-b.log"
echo "  tail -f /tmp/logstream-c.log"
echo ""
echo "Stop cluster:"
echo "  sudo ./stop-cluster.sh"
echo ""
echo "Test connectivity:"
echo "  sudo ip netns exec logstream-a ping -c 2 172.20.0.20"