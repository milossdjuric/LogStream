#!/bin/bash

GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}========================================"
echo "LogStream Docker Cleanup"
echo "========================================${NC}"
echo ""

# Kill only LogStream tmux sessions
echo -e "${YELLOW}→ Killing LogStream tmux sessions...${NC}"
tmux kill-session -t dockertests 2>/dev/null
tmux kill-session -t alltests 2>/dev/null
echo -e "${GREEN} Done${NC}"

# Get all LogStream containers
echo ""
echo -e "${YELLOW}→ Finding LogStream containers...${NC}"
LOGSTREAM_CONTAINERS=$(docker ps -a --filter "name=logstream" --format "{{.ID}}")

if [ -z "$LOGSTREAM_CONTAINERS" ]; then
    echo -e "${GREEN} No LogStream containers found${NC}"
else
    echo "Found LogStream containers:"
    docker ps -a --filter "name=logstream" --format "table {{.ID}}\t{{.Names}}\t{{.Status}}"
    
    # Stop containers
    echo ""
    echo -e "${YELLOW}→ Stopping LogStream containers...${NC}"
    echo "$LOGSTREAM_CONTAINERS" | xargs docker stop 2>/dev/null
    echo -e "${GREEN} Stopped${NC}"
    
    # Remove containers
    echo ""
    echo -e "${YELLOW}→ Removing LogStream containers...${NC}"
    echo "$LOGSTREAM_CONTAINERS" | xargs docker rm 2>/dev/null
    echo -e "${GREEN} Removed${NC}"
fi

# Get all LogStream networks
echo ""
echo -e "${YELLOW}→ Finding LogStream networks...${NC}"
LOGSTREAM_NETWORKS=$(docker network ls --filter "name=logstream" --format "{{.ID}}")

if [ -z "$LOGSTREAM_NETWORKS" ]; then
    echo -e "${GREEN} No LogStream networks found${NC}"
else
    echo "Found LogStream networks:"
    docker network ls --filter "name=logstream" --format "table {{.ID}}\t{{.Name}}"
    
    echo ""
    echo -e "${YELLOW}→ Removing LogStream networks...${NC}"
    echo "$LOGSTREAM_NETWORKS" | xargs docker network rm 2>/dev/null
    echo -e "${GREEN} Removed${NC}"
fi

# Optional: Remove LogStream images
echo ""
echo -e "${YELLOW}→ LogStream Docker images:${NC}"
LOGSTREAM_IMAGES=$(docker images --filter "reference=logstream*" --format "{{.ID}}")

if [ -z "$LOGSTREAM_IMAGES" ]; then
    echo -e "${GREEN} No LogStream images found${NC}"
else
    docker images --filter "reference=logstream*" --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}\t{{.ID}}"
    echo ""
    read -p "Remove LogStream images? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}→ Removing LogStream images...${NC}"
        echo "$LOGSTREAM_IMAGES" | xargs docker rmi -f 2>/dev/null
        echo -e "${GREEN} Removed${NC}"
    else
        echo "Skipping image removal"
    fi
fi

# Show summary
echo ""
echo -e "${BLUE}========================================"
echo "CLEANUP SUMMARY"
echo "========================================${NC}"
echo ""
echo -e "${GREEN} LogStream containers: Removed${NC}"
echo -e "${GREEN} LogStream networks: Removed${NC}"
echo ""

echo "Remaining containers (should not include logstream):"
docker ps -a --format "table {{.Names}}\t{{.Status}}" | head -10

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN} Cleanup complete!${NC}"
echo -e "${BLUE}========================================${NC}"
