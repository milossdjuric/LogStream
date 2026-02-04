#!/bin/bash
# Complete rebuild script for LogStream project
# Builds all binaries: logstream, producer, consumer, broker

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}========================================"
echo -e "LogStream Complete Rebuild (All Binaries)"
echo -e "========================================${NC}"
echo ""

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

echo -e "${BLUE}Project root: $PROJECT_ROOT${NC}"
echo ""

# Step 1: Kill processes
echo -e "${YELLOW}-> Step 1: Killing all logstream processes...${NC}"
pkill -9 -f logstream 2>/dev/null || true
pkill -9 -f producer 2>/dev/null || true
pkill -9 -f consumer 2>/dev/null || true
pkill -9 -f broker 2>/dev/null || true
sleep 1
echo -e "${GREEN}✓ Processes killed${NC}"

# Step 2: Free ports
echo ""
echo -e "${YELLOW}-> Step 2: Freeing ports 8001-8003, 8888, 9999...${NC}"
for port in 8001 8002 8003 8888 9999; do
    if command -v lsof &> /dev/null; then
        pid=$(lsof -ti:$port 2>/dev/null)
        if [ -n "$pid" ]; then
            echo -e "${RED}  Killing PID $pid on port $port${NC}"
            kill -9 $pid 2>/dev/null || true
        fi
    elif command -v fuser &> /dev/null; then
        fuser -k -9 ${port}/tcp 2>/dev/null || true
        fuser -k -9 ${port}/udp 2>/dev/null || true
    fi
    echo -e "${GREEN}  ✓ Port $port is free${NC}"
done

# Step 3: Remove old binaries
echo ""
echo -e "${YELLOW}-> Step 3: Removing old binaries...${NC}"
rm -f ./logstream ./producer ./consumer ./broker 2>/dev/null || true
echo -e "${GREEN}✓ Old binaries removed${NC}"

# Step 4: Clean logs
echo ""
echo -e "${YELLOW}-> Step 4: Cleaning logs...${NC}"
rm -f /tmp/logstream-*.log /tmp/test-broker-*.log /tmp/test-producer-*.log /tmp/test-consumer-*.log 2>/dev/null || true
echo -e "${GREEN}✓ Logs cleaned${NC}"

# Step 5: Build all binaries
echo ""
echo -e "${YELLOW}-> Step 5: Building all binaries...${NC}"
BUILD_FAILED=0

# Build logstream (main broker node)
echo -e "${BLUE}  Building logstream...${NC}"
if go build -o logstream main.go; then
    SIZE=$(stat -c%s logstream 2>/dev/null || stat -f%z logstream 2>/dev/null || echo "unknown")
    echo -e "${GREEN}  ✓ logstream built successfully ($SIZE bytes)${NC}"
    ls -lh logstream | awk '{print "    " $5 " " $9}'
else
    echo -e "${RED}  ✗ logstream build FAILED!${NC}"
    BUILD_FAILED=1
fi

echo ""

# Build broker (CLI-friendly version with flags)
echo -e "${BLUE}  Building broker (alternative entry point)...${NC}"
if go build -o broker cmd/broker/main.go; then
    SIZE=$(stat -c%s broker 2>/dev/null || stat -f%z broker 2>/dev/null || echo "unknown")
    echo -e "${GREEN}  ✓ broker built successfully ($SIZE bytes)${NC}"
    ls -lh broker | awk '{print "    " $5 " " $9}'
else
    echo -e "${RED}  ✗ broker build FAILED!${NC}"
    BUILD_FAILED=1
fi

echo ""

# Build producer
echo -e "${BLUE}  Building producer...${NC}"
if go build -o producer cmd/producer/main.go; then
    SIZE=$(stat -c%s producer 2>/dev/null || stat -f%z producer 2>/dev/null || echo "unknown")
    echo -e "${GREEN}  ✓ producer built successfully ($SIZE bytes)${NC}"
    ls -lh producer | awk '{print "    " $5 " " $9}'
else
    echo -e "${RED}  ✗ producer build FAILED!${NC}"
    BUILD_FAILED=1
fi

echo ""

# Build consumer
echo -e "${BLUE}  Building consumer...${NC}"
if go build -o consumer cmd/consumer/main.go; then
    SIZE=$(stat -c%s consumer 2>/dev/null || stat -f%z consumer 2>/dev/null || echo "unknown")
    echo -e "${GREEN}  ✓ consumer built successfully ($SIZE bytes)${NC}"
    ls -lh consumer | awk '{print "    " $5 " " $9}'
else
    echo -e "${RED}  ✗ consumer build FAILED!${NC}"
    BUILD_FAILED=1
fi

echo ""

# Summary
if [ "$BUILD_FAILED" -eq 0 ]; then
    echo -e "${GREEN}========================================"
    echo -e "✓ All binaries built successfully!"
    echo -e "========================================${NC}"
    echo ""
    echo -e "${BLUE}Built binaries:${NC}"
    ls -lh logstream broker producer consumer 2>/dev/null | awk '{print "  " $9 " (" $5 ")"}'
    echo ""
    echo -e "${GREEN}✓ Ready to test!${NC}"
    echo ""
    echo -e "${BLUE}Quick start:${NC}"
    echo -e "  # Start broker node:"
    echo -e "  NODE_ADDRESS=192.168.1.10:8001 ./logstream"
    echo ""
    echo -e "  # Or use broker with flags:"
    echo -e "  ./broker -addr 192.168.1.10:8001"
    echo ""
    echo -e "  # Start producer:"
    echo -e "  ./producer -leader 192.168.1.10:8001 -topic logs"
    echo ""
    echo -e "  # Start consumer:"
    echo -e "  ./consumer -leader 192.168.1.10:8001 -topic logs"
    echo ""
    exit 0
else
    echo -e "${RED}========================================"
    echo -e "✗ Some builds FAILED!"
    echo -e "========================================${NC}"
    echo ""
    echo -e "${RED}Check errors above and fix compilation issues.${NC}"
    echo ""
    exit 1
fi
