#!/bin/bash

echo "========================================"
echo "Stopping LogStream Cluster (netns)"
echo "========================================"

# Kill processes in namespaces
echo "Stopping nodes..."
for ns in logstream-a logstream-b logstream-c; do
    if ip netns list | grep -q "^$ns"; then
        sudo ip netns pids $ns 2>/dev/null | xargs -r sudo kill -9 2>/dev/null || true
        echo "  Stopped processes in $ns"
    fi
done

echo ""
echo "[OK] Cluster stopped"
echo ""
echo "Note: Network namespaces are still active."
echo "To remove them, run: sudo ./cleanup.sh"

