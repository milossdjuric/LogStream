#!/bin/bash

echo "Cleaning up Vagrant VMs..."

# Stop cluster
./stop-cluster.sh 2>/dev/null || true

# Destroy VMs
vagrant destroy -f

echo "[OK] Vagrant cleanup complete!"