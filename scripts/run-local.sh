#!/bin/bash

# Change to project root directory
cd "$(dirname "$0")/.."

ROLE=$1
ADDRESS=$2

if [ -z "$ROLE" ]; then
    echo "Usage: ./scripts/run-local.sh <leader|follower> [address]"
    echo ""
    echo "Find your IP: go run cmd/netdiag/main.go"
    echo ""
    echo "Examples:"
    echo "  Auto-detect IP:"
    echo "    ./scripts/run-local.sh leader"
    echo "    ./scripts/run-local.sh follower"
    echo ""
    echo "  Specify IP:"
    echo "    ./scripts/run-local.sh leader 192.168.1.25:8001"
    echo "    ./scripts/run-local.sh follower 192.168.1.30:8002"
    exit 1
fi

if [ "$ROLE" = "leader" ]; then
    export IS_LEADER="true"
elif [ "$ROLE" = "follower" ]; then
    export IS_LEADER="false"
else
    echo "Error: role must be 'leader' or 'follower'"
    exit 1
fi

if [ -n "$ADDRESS" ]; then
    export NODE_ADDRESS="$ADDRESS"
fi

export MULTICAST_GROUP="239.0.0.1:9999"
export BROADCAST_PORT="8888"

echo "Starting LogStream as $ROLE"
if [ -n "$NODE_ADDRESS" ]; then
    echo "Address: $NODE_ADDRESS"
else
    echo "Address: auto-detect"
fi
echo "--------------------"
echo ""

go run main.go
