#!/bin/bash

# Change to project root directory
cd "$(dirname "$0")/.."

echo "Stopping LogStream Docker containers..."
echo "--------------------------------------"

docker compose down

echo ""
echo "✓ All containers stopped and removed"
echo "✓ Network removed"
echo ""
echo "To restart: ./scripts/run-docker.sh"
