#!/bin/bash

echo "========================================"
echo "Stopping LogStream Cluster"
echo "========================================"

vagrant ssh leader -c 'pkill -f logstream' 2>/dev/null || true
vagrant ssh broker1 -c 'pkill -f logstream' 2>/dev/null || true
vagrant ssh broker2 -c 'pkill -f logstream' 2>/dev/null || true

echo "[OK] Cluster stopped"