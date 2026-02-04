#!/bin/bash

# Cleanup script for test log files with permission issues
# Run this if you get "Permission denied" errors

echo "Cleaning up old test log files..."

# Remove old log files that might have wrong permissions
sudo rm -f /tmp/leader.log
sudo rm -f /tmp/follower.log
sudo rm -f /tmp/leader_protocol.log
sudo rm -f /tmp/follower_protocol.log

# Kill any stuck logstream processes
pkill -f logstream 2>/dev/null || true

echo "✓ Cleanup complete"
echo ""
echo "You can now run the network tests:"
echo "  sudo ./tests/linux/test-protocol-compliance.sh local"
echo "  sudo ./tests/linux/test-network-analysis.sh local"
