# LogStream

A distributed log streaming platform implementing view-synchronous replication with passive replication semantics.

## Overview

LogStream is a distributed system for log streaming that provides:
- Automatic leader election using LCR ring algorithm
- View-synchronous membership management
- FIFO ordered message delivery
- Phi accrual failure detection
- Consistent hash ring for load balancing
- Producer-consumer stream assignment

Each node:
- Joins the multicast group on startup
- Receives messages from all other nodes
- Leader sends periodic heartbeats and replicate messages
- Followers listen and process incoming messages

## Quick Start

### Prerequisites

- Go 1.21 or higher
- Docker and Docker Compose (for containerized testing)
- Network connectivity (multicast-capable network interface)

### Build

```bash
go build -o logstream main.go
go build -o producer cmd/producer/main.go
go build -o consumer cmd/consumer/main.go
```

### Run

```bash
# Single node (becomes leader)
NODE_ADDRESS=192.168.1.10:8001 MULTICAST_GROUP=239.0.0.1:9999 ./logstream

# Additional nodes (become followers)
NODE_ADDRESS=192.168.1.20:8001 MULTICAST_GROUP=239.0.0.1:9999 ./logstream
```

### Find Your Network Interface

```bash
go run cmd/netdiag/main.go
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| NODE_ADDRESS | Auto-detect | Node's IP:PORT |
| MULTICAST_GROUP | 239.0.0.1:9999 | Multicast address for heartbeats |
| BROADCAST_PORT | 8888 | UDP broadcast port for discovery |

### Auto-Detection

If NODE_ADDRESS is not specified, the system automatically:
1. Scans available network interfaces
2. Selects the primary non-loopback interface
3. Assigns default port 8001

## Architecture

### Core Components

- **Node**: Central broker implementation handling leader/follower roles
- **Protocol**: Protobuf-based messaging layer
- **State**: Cluster state management with FIFO ordering
- **Storage**: In-memory log storage per topic
- **Failure Detection**: Phi accrual detector for automatic failure detection
- **Load Balancing**: Consistent hash ring for topic assignment

### Message Types

| Message | Direction | Transport | Purpose |
|---------|-----------|-----------|---------|
| JOIN | Node to Cluster | UDP Broadcast | Cluster discovery |
| JOIN_RESPONSE | Leader to Node | UDP Unicast | Discovery response |
| HEARTBEAT | Leader to Followers | UDP Multicast | Liveness |
| REPLICATE | Leader to Followers | UDP Multicast | State replication |
| REPLICATE_ACK | Follower to Leader | TCP | Replication ack |
| ELECTION | Node to Next | TCP | LCR election |
| STATE_EXCHANGE | Leader to Followers | TCP | Recovery state request |
| VIEW_INSTALL | Leader to Followers | TCP | Install new view |

### Key Behaviors

**Leader Election**: LCR ring-based algorithm
1. Nodes detect reachable peers in parallel
2. Ring computed from sorted reachable node IDs
3. Election message circulates with highest ID
4. Node receiving its own ID becomes leader

**View-Synchronous Recovery**: After leader election
1. New leader freezes operations
2. Collects state from all followers (full log entries)
3. Merges logs using UNION semantics (all entries preserved)
4. Installs new view with merged state
5. Operations resume

**Failure Detection**: Phi Accrual + Timeout
- Suspicion threshold: phi > 8.0
- Failure threshold: phi > 10.0
- Fallback timeout: 15 seconds

## Testing

See TESTS_QUICK_REFERENCE.md for quick commands or TESTS_COMPLETE_GUIDE.md for detailed documentation.

### Quick Test

```bash
# Unit tests
go test -v ./tests/unit/

# Integration tests (requires network namespaces)
sudo ./deploy/netns/setup-netns.sh
cd tests/linux && ./test-trio.sh local
```

### Test Modes

| Mode | Platform | Best For |
|------|----------|----------|
| local | Network namespaces | Development |
| docker | Docker containers | CI/CD |
| vagrant | KVM VMs | Pre-production |

## Project Structure

```
LogStream/
├── main.go                    # Main entry point
├── cmd/
│   ├── broker/main.go         # Broker entry point
│   ├── producer/main.go       # Producer client
│   ├── consumer/main.go       # Consumer client
│   └── netdiag/main.go        # Network diagnostics
├── internal/
│   ├── node/                  # Core broker implementation
│   ├── protocol/              # Protobuf messaging
│   ├── state/                 # Cluster state management
│   ├── storage/               # Log storage
│   ├── failure/               # Failure detection
│   └── loadbalance/           # Consistent hash ring
├── tests/
│   ├── unit/                  # Go unit tests
│   ├── integration/           # Go integration tests
│   └── linux/                 # Bash integration tests
└── deploy/
    ├── docker/                # Docker deployment
    ├── netns/                 # Network namespace scripts
    └── vagrant/               # Vagrant VM deployment
```

## Troubleshooting

### Common Issues

**All nodes become leaders:**
- UDP multicast/broadcast not working on localhost
- Use network namespaces or Docker

**Election never completes:**
- Check TCP connectivity between nodes
- Verify ring topology consistency

**Consumer not receiving data:**
- Verify producer is sending to correct broker
- Check topic assignment via hash ring

### Debug Commands

```bash
# Check cluster state
grep "ClusterState" node.log

# Check election progress
grep -E "ELECTION|I WIN|COORDINATOR" node.log

# Kill all processes
pkill -f logstream
```

## License

MIT License
