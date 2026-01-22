#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_DIR="$SCRIPT_DIR/compose"
COMPOSE_FILE="${1:-trio.yaml}"

echo "========================================="
echo "Stopping LogStream Cluster (Docker)"
echo "========================================="

if [ ! -f "$COMPOSE_DIR/$COMPOSE_FILE" ]; then
    echo "ERROR: Compose file not found: $COMPOSE_DIR/$COMPOSE_FILE"
    echo ""
    echo "Available compose files:"
    ls -1 "$COMPOSE_DIR"/*.yaml | xargs -n1 basename
    exit 1
fi

cd "$COMPOSE_DIR"

echo ""
echo "Using compose file: $COMPOSE_FILE"
echo ""

if ! docker compose -f "$COMPOSE_FILE" ps | grep -q "Up"; then
    echo "No running cluster found for $COMPOSE_FILE"
    exit 0
fi

echo "Stopping cluster..."
docker compose -f "$COMPOSE_FILE" down

echo ""
echo "[OK] Cluster stopped!"

# Optional: Clean up volumes
read -p "Remove volumes? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    docker compose -f "$COMPOSE_FILE" down -v
    echo "[OK] Volumes removed"
fi