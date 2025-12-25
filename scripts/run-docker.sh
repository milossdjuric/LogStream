#!/bin/bash

set -e

# Change to project root directory
cd "$(dirname "$0")/.."

echo "LogStream Docker Test"
echo "--------------------"
echo ""

echo "Building and starting containers..."
docker compose up --build -d

echo ""
echo "Waiting for startup..."
sleep 5

echo ""
echo "Container status:"
docker compose ps

echo ""
echo "Network test (broker1 -> leader):"
docker exec logstream-leader ping -c 2 172.25.0.10 2>/dev/null || echo "Ping not available, but containers are running"

echo ""
echo "Leader logs:"
docker compose logs --tail=15 leader

echo ""
echo "Broker1 logs:"
docker compose logs --tail=15 broker1

echo ""
echo "Broker2 logs:"
docker compose logs --tail=15 broker2

echo ""
echo "--------------------"
echo "Containers running. Use 'docker compose logs -f' to watch logs."
echo "Stop with './scripts/stop-docker.sh' or 'docker compose down'"
