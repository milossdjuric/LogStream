#!/bin/bash
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}========================================"
echo -e "LogStream Complete Cleanup & Rebuild"
echo -e "========================================${NC}"
echo ""

# Kill processes
echo -e "${YELLOW}→ Killing all logstream processes...${NC}"
pkill -9 -f logstream 2>/dev/null
sleep 1
echo -e "${GREEN} Processes killed${NC}"

# Free ports
echo ""
echo -e "${YELLOW}→ Freeing ports 8001-8003...${NC}"
for port in 8001 8002 8003; do
    pid=$(lsof -ti:$port 2>/dev/null)
    if [ -n "$pid" ]; then
        echo -e "${RED}  Killing PID $pid on port $port${NC}"
        kill -9 $pid 2>/dev/null
    fi
    echo -e "${GREEN}  Port $port is free${NC}"
done

# Remove binary
echo ""
echo -e "${YELLOW}→ Removing old binary...${NC}"
rm -f ./logstream
echo -e "${GREEN} Binary removed${NC}"

# Clean logs
echo ""
echo -e "${YELLOW}→ Cleaning logs...${NC}"
rm -f /tmp/logstream-*.log 2>/dev/null
echo -e "${GREEN} Logs cleaned${NC}"

# Rebuild
echo ""
echo -e "${YELLOW}→ Rebuilding...${NC}"
go build -o logstream main.go
if [ $? -eq 0 ]; then
    echo -e "${GREEN} Build successful!${NC}"
    ls -lh logstream
else
    echo -e "${RED}✗ Build failed!${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN} Ready to test!${NC}"
echo ""
