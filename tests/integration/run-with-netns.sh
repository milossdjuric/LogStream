#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
YELLOW='\033[0;33m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Go Integration Tests with Network Namespaces${NC}"
echo -e "${BLUE}========================================${NC}"
echo

# Check if running with sudo
if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}Error: This script must be run with sudo${NC}"
    echo "Usage: sudo ./run-with-netns.sh [test-pattern]"
    echo
    echo "Examples:"
    echo "  sudo ./run-with-netns.sh              # Run all tests"
    echo "  sudo ./run-with-netns.sh Single       # Run single broker tests"
    echo "  sudo ./run-with-netns.sh Three        # Run three broker tests"
    exit 1
fi

# Get project root (two levels up from tests/integration)
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

# Parse arguments
TEST_PATTERN="${1:-}"
TEST_FLAGS="-v"
if [ -n "$TEST_PATTERN" ]; then
    TEST_FLAGS="-v -run $TEST_PATTERN"
fi

echo -e "${BLUE}[1/5] Checking prerequisites...${NC}"

# Check if ip command exists
if ! command -v ip &> /dev/null; then
    echo -e "${RED}Error: 'ip' command not found${NC}"
    echo "Install: sudo apt-get install iproute2"
    exit 1
fi
echo -e "${GREEN}  [OK] iproute2 available${NC}"

# Check kernel support for network namespaces
if [ ! -e /proc/self/ns/net ]; then
    echo -e "${RED}Error: Network namespaces not supported by kernel${NC}"
    exit 1
fi
echo -e "${GREEN}  [OK] Kernel supports network namespaces${NC}"

echo
echo -e "${BLUE}[2/5] Setting up network namespaces...${NC}"

# Check if namespaces exist
if ip netns list 2>/dev/null | grep -q "logstream-a"; then
    echo -e "${GREEN}  [OK] Network namespaces already exist${NC}"
else
    echo "  Setting up namespaces..."
    if [ -f "$PROJECT_ROOT/deploy/netns/setup-netns.sh" ]; then
        if bash "$PROJECT_ROOT/deploy/netns/setup-netns.sh"; then
            echo -e "${GREEN}  [OK] Network namespaces created${NC}"
        else
            echo -e "${RED}Error: Failed to setup namespaces${NC}"
            exit 1
        fi
    else
        echo -e "${RED}Error: setup-netns.sh not found${NC}"
        exit 1
    fi
fi

# Verify connectivity
echo "  Testing namespace connectivity..."
if ip netns exec logstream-a ping -c 1 -W 1 172.20.0.20 &> /dev/null; then
    echo -e "${GREEN}  [OK] Network connectivity verified${NC}"
else
    echo -e "${YELLOW}  Warning: Connectivity test failed, but continuing...${NC}"
fi

echo
echo -e "${BLUE}[3/5] Building logstream binary...${NC}"

# Check if binary exists
if [ -f "$PROJECT_ROOT/logstream" ]; then
    # Check if it's recent (modified in last hour)
    if [ $(find "$PROJECT_ROOT/logstream" -mmin -60 2>/dev/null | wc -l) -gt 0 ]; then
        echo -e "${GREEN}  [OK] Using existing binary (built recently)${NC}"
    else
        echo "  Rebuilding (binary is old)..."
        if go build -o logstream main.go; then
            echo -e "${GREEN}  [OK] Binary rebuilt${NC}"
        else
            echo -e "${RED}Error: Build failed${NC}"
            exit 1
        fi
    fi
else
    echo "  Building binary..."
    if go build -o logstream main.go; then
        echo -e "${GREEN}  [OK] Binary built${NC}"
    else
        echo -e "${RED}Error: Build failed${NC}"
        exit 1
    fi
fi

echo
echo -e "${BLUE}[4/5] Cleaning up any existing processes...${NC}"

# Kill processes in namespaces
for ns in logstream-a logstream-b logstream-c; do
    if ip netns list 2>/dev/null | grep -q "^$ns"; then
        PIDS=$(ip netns pids $ns 2>/dev/null | tr '\n' ' ')
        if [ -n "$PIDS" ]; then
            echo "  Killing processes in $ns: $PIDS"
            echo "$PIDS" | xargs -r kill -9 2>/dev/null || true
        fi
    fi
done

# Also kill any orphaned processes on host
pkill -f "logstream" 2>/dev/null || true

echo -e "${GREEN}  [OK] Cleanup complete${NC}"

echo
echo -e "${BLUE}[5/5] Running tests...${NC}"
echo -e "${BLUE}========================================${NC}"
echo

# Change to tests/integration directory
cd "$PROJECT_ROOT/tests/integration"

# Run tests with USE_NETNS=1
if USE_NETNS=1 go test $TEST_FLAGS 2>&1; then
    EXIT_CODE=0
    echo
    echo -e "${BLUE}========================================${NC}"
    echo -e "${GREEN}[OK] Tests PASSED${NC}"
    echo -e "${BLUE}========================================${NC}"
else
    EXIT_CODE=1
    echo
    echo -e "${BLUE}========================================${NC}"
    echo -e "${RED}[X] Tests FAILED${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo
    echo -e "${YELLOW}Troubleshooting:${NC}"
    echo "  - Check logs: ls -la /tmp/logstream-integration-test-*/"
    echo "  - Check processes: sudo ip netns pids logstream-a"
    echo "  - Check connectivity: sudo ip netns exec logstream-a ping -c 2 172.20.0.20"
fi

echo
echo -e "${BLUE}Cleanup:${NC}"
echo "  To kill processes (keeps namespaces): sudo pkill -f logstream"
echo "  To remove namespaces: sudo $PROJECT_ROOT/deploy/netns/cleanup.sh"
echo

exit $EXIT_CODE
