#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_DIR="$SCRIPT_DIR/compose"
COMPOSE_FILE="${1:-trio.yaml}"

echo "========================================="
echo "Starting LogStream Cluster (Docker)"
echo "========================================="

# Validate compose file exists
if [ ! -f "$COMPOSE_DIR/$COMPOSE_FILE" ]; then
    echo "ERROR: Compose file not found: $COMPOSE_DIR/$COMPOSE_FILE"
    echo ""
    echo "Available compose files:"
    ls -1 "$COMPOSE_DIR"/*.yaml | xargs -n1 basename
    echo ""
    echo "Usage: $0 [compose-file.yaml]"
    echo "Default: trio.yaml (3-node cluster)"
    exit 1
fi

cd "$COMPOSE_DIR"

echo ""
echo "Using compose file: $COMPOSE_FILE"
echo ""

# Check if cluster is already running
if docker compose -f "$COMPOSE_FILE" ps | grep -q "Up"; then
    echo "[!] Cluster appears to be running already"
    echo ""
    read -p "Stop and restart? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Stopping existing cluster..."
        docker compose -f "$COMPOSE_FILE" down
        sleep 2
    else
        echo "Exiting..."
        exit 0
    fi
fi

echo "Building images..."
docker compose -f "$COMPOSE_FILE" build

echo ""
echo "Starting cluster..."
docker compose -f "$COMPOSE_FILE" up -d

sleep 5

echo ""
echo "[OK] Cluster started!"
echo ""

# Show service status
echo "Service status:"
docker compose -f "$COMPOSE_FILE" ps

echo ""
echo "View logs:"
echo "  cd $COMPOSE_DIR"
echo "  docker compose -f $COMPOSE_FILE logs -f"
echo ""
echo "Individual service logs:"
for service in $(docker compose -f "$COMPOSE_FILE" ps --services); do
    echo "  docker compose -f $COMPOSE_FILE logs -f $service"
done

echo ""
echo "Stop cluster:"
echo "  cd $COMPOSE_DIR"
echo "  docker compose -f $COMPOSE_FILE down"