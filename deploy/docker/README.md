# Docker Deployment

Test LogStream using Docker containers with isolated networks.

## Quick Start
```bash
# Run tests using main test scripts
cd ../../tests/linux
./test-single.sh docker
./test-trio.sh docker
./test-election-automatic.sh docker

# Or run all Docker tests
cd ../../tests/linux/integration
./test-all-docker.sh

# Cleanup
cd ../../deploy/docker
./cleanup.sh
```

## Compose Files

All compose files are in `compose/` directory:

| File | Network | Description |
|------|---------|-------------|
| `single.yaml` | 172.25.0.0/24 | Single leader |
| `trio.yaml` | 172.25.0.0/24 | Leader + 2 followers |
| `sequence.yaml` | 172.26.0.0/24 | Sequence demo |
| `producer-consumer.yaml` | 172.28.0.0/24 | Producer->Consumer |
| `election.yaml` | 172.29.0.0/24 | 3-node election |

## Manual Testing
```bash
cd compose

# Start specific test
docker compose -f trio.yaml up -d

# View logs
docker compose -f trio.yaml logs -f

# Stop
docker compose -f trio.yaml down
```

## Network Isolation

Each compose file uses its own network to prevent conflicts when running multiple tests.

## Requirements

- Docker 20.10+
- Docker Compose V2

## Tips

- Use test scripts in `tests/linux/` with `docker` argument
- Compose files use multi-stage builds to minimize image size
- Networks auto-cleanup with `docker compose down`