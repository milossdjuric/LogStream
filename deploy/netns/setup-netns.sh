#!/bin/bash
set -e

echo "========================================="
echo "Setting up Network Namespaces for LogStream"
echo "========================================="

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    echo "ERROR: This script must be run with sudo"
    exit 1
fi

# Check if namespaces already exist
if ip netns list | grep -q "logstream-a"; then
    echo "Network namespaces already exist. Skipping setup."
    echo "To recreate, run: sudo ./cleanup.sh first"
    exit 0
fi

echo ""
echo "Creating network namespaces..."
ip netns add logstream-a
ip netns add logstream-b
ip netns add logstream-c
echo "  [OK] Created logstream-a, logstream-b, logstream-c"

echo ""
echo "Creating virtual bridge..."
ip link add br-logstream type bridge
ip addr add 172.20.0.1/24 dev br-logstream
ip link set br-logstream up
echo "  [OK] Created br-logstream (172.20.0.1/24)"

echo ""
echo "Creating virtual ethernet pairs..."
# Create veth pairs
ip link add veth-a type veth peer name veth-a-br
ip link add veth-b type veth peer name veth-b-br
ip link add veth-c type veth peer name veth-c-br

# Move one end of each pair into namespaces
ip link set veth-a netns logstream-a
ip link set veth-b netns logstream-b
ip link set veth-c netns logstream-c

# Connect other ends to bridge
ip link set veth-a-br master br-logstream
ip link set veth-b-br master br-logstream
ip link set veth-c-br master br-logstream

# Bring up bridge ends
ip link set veth-a-br up
ip link set veth-b-br up
ip link set veth-c-br up

# Configure namespaces
ip netns exec logstream-a ip addr add 172.20.0.10/24 dev veth-a
ip netns exec logstream-a ip link set veth-a up
ip netns exec logstream-a ip link set lo up

ip netns exec logstream-b ip addr add 172.20.0.20/24 dev veth-b
ip netns exec logstream-b ip link set veth-b up
ip netns exec logstream-b ip link set lo up

ip netns exec logstream-c ip addr add 172.20.0.30/24 dev veth-c
ip netns exec logstream-c ip link set veth-c up
ip netns exec logstream-c ip link set lo up

echo "  [OK] Created and configured veth pairs"

echo ""
echo "Enabling IP forwarding..."
echo 1 > /proc/sys/net/ipv4/ip_forward
echo "  [OK] IP forwarding enabled"

echo ""
echo "========================================="
echo "[OK] Network namespaces setup complete!"
echo "========================================="
echo ""
echo "Network topology:"
echo "  Bridge: br-logstream (172.20.0.1/24)"
echo "  +- logstream-a: 172.20.0.10"
echo "  +- logstream-b: 172.20.0.20"
echo "  +- logstream-c: 172.20.0.30"
echo ""
echo "Test connectivity:"
echo "  sudo ip netns exec logstream-a ping -c 2 172.20.0.20"
echo ""
echo "Start cluster:"
echo "  sudo ./start-cluster.sh"
echo ""

