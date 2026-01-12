#!/bin/bash

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

log() { echo -e "${BLUE}→${NC} $1"; }
success() { echo -e "${GREEN}${NC} $1"; }
error() { echo -e "${RED}✗${NC} $1"; }
error_msg() { echo -e "${RED}⚠${NC} $1"; }

# Get project root (3 levels up from tests/linux/lib/)
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

# Just check if binary exists
build_if_needed() {
    local binary=$1
    # Binaries should be in project root
    if [ ! -f "$PROJECT_ROOT/$binary" ]; then
        error "Binary $binary not found in $PROJECT_ROOT"
        error "Run: ./rebuild.sh from project root"
        exit 1
    fi
}

# Cleanup
cleanup() {
    log "Cleaning up..."
    pkill -f "logstream" 2>/dev/null
    pkill -f "producer" 2>/dev/null
    pkill -f "consumer" 2>/dev/null
    rm -f /tmp/logstream-*.log /tmp/producer-*.pipe 2>/dev/null
}

trap cleanup EXIT

# Make sure we're in project root when running commands
cd "$PROJECT_ROOT"
