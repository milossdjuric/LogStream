#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_DIR="$SCRIPT_DIR/compose"

echo "Cleaning up Docker resources..."

# Stop and remove all LogStream containers from compose files
if [ -d "$COMPOSE_DIR" ]; then
    echo "Stopping compose-based clusters..."
    cd "$COMPOSE_DIR"
    
    # Clean up test-specific projects (used by parallel tests)
    for project in test-single test-trio test-sequence test-election test-producer-consumer; do
        for compose_file in *.yaml; do
            if [ -f "$compose_file" ]; then
                COMPOSE_PROJECT_NAME="$project" docker compose -f "$compose_file" down --remove-orphans 2>/dev/null || true
            fi
        done
    done
    
    # Clean up default project name (for manual runs)
    for compose_file in *.yaml; do
        if [ -f "$compose_file" ]; then
            compose_name=$(basename "$compose_file")
            docker compose -f "$compose_name" down --remove-orphans 2>/dev/null || true
        fi
    done
fi

# Stop and remove any remaining LogStream containers (by name or label)
echo "Stopping remaining LogStream containers..."
docker ps -a --filter "name=logstream" --format "{{.ID}}" | xargs -r docker stop 2>/dev/null || true
docker ps -a --filter "name=logstream" --format "{{.ID}}" | xargs -r docker rm 2>/dev/null || true

# Remove containers by label
docker ps -a --filter "label=logstream.test" --format "{{.ID}}" | xargs -r docker stop 2>/dev/null || true
docker ps -a --filter "label=logstream.test" --format "{{.ID}}" | xargs -r docker rm 2>/dev/null || true

# Remove LogStream networks
echo "Removing LogStream networks..."
docker network ls --filter "name=logstream" --format "{{.ID}}" | xargs -r docker network rm 2>/dev/null || true

# Remove orphaned networks (networks with logstream-net pattern)
docker network ls --format "{{.Name}}" | grep -E "logstream|compose" | xargs -r -I {} docker network rm {} 2>/dev/null || true

echo "[OK] Docker cleanup complete!"
