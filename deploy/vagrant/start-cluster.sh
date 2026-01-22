#!/bin/bash

set -e

echo "========================================"
echo "Starting LogStream Cluster on VMs"
echo "========================================"

# Check VMs are running
if ! vagrant status | grep -q "running"; then
    echo "ERROR: VMs not running!"
    echo "Run: vagrant up"
    exit 1
fi

echo ""
echo "-> Starting leader..."
vagrant ssh leader -c 'cd /vagrant/logstream && \
    NODE_ADDRESS=192.168.100.10:8001 \
    IS_LEADER=true \
    MULTICAST_GROUP=239.0.0.1:9999 \
    BROADCAST_PORT=8888 \
    nohup ./logstream > /tmp/logstream.log 2>&1 &'
sleep 3

echo "-> Starting broker1..."
vagrant ssh broker1 -c 'cd /vagrant/logstream && \
    NODE_ADDRESS=192.168.100.20:8002 \
    IS_LEADER=false \
    MULTICAST_GROUP=239.0.0.1:9999 \
    BROADCAST_PORT=8888 \
    nohup ./logstream > /tmp/logstream.log 2>&1 &'
sleep 3

echo "-> Starting broker2..."
vagrant ssh broker2 -c 'cd /vagrant/logstream && \
    NODE_ADDRESS=192.168.100.30:8003 \
    IS_LEADER=false \
    MULTICAST_GROUP=239.0.0.1:9999 \
    BROADCAST_PORT=8888 \
    nohup ./logstream > /tmp/logstream.log 2>&1 &'
sleep 3

echo ""
echo "[OK] Cluster started!"
echo ""
echo "Check logs:"
echo "  vagrant ssh leader -c 'tail -f /tmp/logstream.log'"
echo "  vagrant ssh broker1 -c 'tail -f /tmp/logstream.log'"
echo "  vagrant ssh broker2 -c 'tail -f /tmp/logstream.log'"