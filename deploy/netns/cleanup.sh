#!/bin/bash

echo "Cleaning up network namespaces..."

# Kill processes in namespaces
for ns in logstream-a logstream-b logstream-c; do
    sudo ip netns pids $ns 2>/dev/null | xargs -r sudo kill -9 2>/dev/null || true
done

# Delete namespaces
for ns in logstream-a logstream-b logstream-c; do
    sudo ip netns delete $ns 2>/dev/null || true
done

# Delete bridge
sudo ip link delete br-logstream 2>/dev/null || true

# Clean logs
rm -f /tmp/logstream-*.log

echo "[OK] Cleanup complete!"