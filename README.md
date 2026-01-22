# LogStream

A distributed log streaming platform 


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

### Find Your Network Interface
```bash
go run cmd/netdiag/main.go
```

This displays available network interfaces and their IP addresses.

## Testing Scenarios

### 1. Docker Testing (Recommended for Development)

Uses Docker Compose with isolated networks. See `deploy/docker/` for details.

```bash
# Run test scripts with docker mode
cd tests/linux
./test-single.sh docker
./test-trio.sh docker
./test-election-automatic.sh docker

# Or use compose directly
cd deploy/docker/compose
docker compose -f trio.yaml up -d
docker compose -f trio.yaml logs -f
docker compose -f trio.yaml down
```

Available compose files in `deploy/docker/compose/`:
- `single.yaml` - Single leader node
- `trio.yaml` - Leader + 2 followers
- `sequence.yaml` - Sequence number demo
- `producer-consumer.yaml` - Producer/consumer test
- `election.yaml` - Leader election test

### 2. Local Testing with Network Namespaces (Linux)

Uses isolated network namespaces for multicast testing. See `deploy/netns/` for details.

```bash
# Setup namespaces (one-time)
cd deploy/netns
sudo ./setup-netns.sh

# Run tests with local mode
cd tests/linux
sudo ./test-single.sh local
sudo ./test-trio.sh local

# Cleanup
cd deploy/netns
sudo ./cleanup.sh
```

### 3. Vagrant Testing (Full VM Isolation)

Uses Vagrant with libvirt/KVM for complete VM isolation. See `deploy/vagrant/` for details.

```bash
# Install vagrant-libvirt plugin (one-time)
./scripts/install-vagrant-libvirt.sh

# Start VMs
cd deploy/vagrant
vagrant up

# Run tests with vagrant mode
cd tests/linux
./test-single.sh vagrant
./test-trio.sh vagrant

# Stop VMs
cd deploy/vagrant
vagrant halt
```

### 4. Manual Testing

For manual testing or physical machines:

```bash
# Build
go build -o logstream main.go

# Terminal 1 (Leader)
NODE_ADDRESS=192.168.1.10:8001 MULTICAST_GROUP=239.0.0.1:9999 ./logstream

# Terminal 2 (Follower)
NODE_ADDRESS=192.168.1.20:8002 MULTICAST_GROUP=239.0.0.1:9999 ./logstream
```

**Important**: Configure firewall rules on all machines (see Firewall Configuration below).


## Configuration

### Environment Variables

- `NODE_ADDRESS`: Node's IP:PORT (auto-detected if not provided)
- `MULTICAST_GROUP`: Multicast address (default: 239.0.0.1:9999)
- `BROADCAST_PORT`: UDP broadcast port (default: 8888)

### Auto-Detection

If NODE_ADDRESS is not specified, the system automatically:
1. Scans available network interfaces
2. Selects the primary non-loopback interface
3. Assigns default port 8001

### Leader Election

Leaders are determined automatically through election protocol - no manual configuration needed. The first node to start typically becomes the leader, and leadership transfers automatically if the leader fails.

## Expected Output

### Leader Node
```
=== LogStream Configuration ===
Node Address:      192.168.1.74:8001
Multicast Group:   239.0.0.1:9999
Broadcast Port:    8888
Network Interface: 192.168.1.74
================================

[Multicast] Joined group 239.0.0.1 on interface wlp4s0 (port reuse enabled)
[Node a1b2c3d4] Started at 192.168.1.74:8001
[Node a1b2c3d4] Elected as LEADER
[Leader a1b2c3d4] -> HEARTBEAT
[Leader a1b2c3d4] -> REPLICATE seq=1 type=REGISTRY
```

The leader sends messages and also receives its own via multicast loopback (this is expected behavior).

### Follower Node
```
=== LogStream Configuration ===
Node Address:      192.168.1.74:8002
Multicast Group:   239.0.0.1:9999
Broadcast Port:    8888
Network Interface: 192.168.1.74
================================

[Multicast] Joined group 239.0.0.1 on interface wlp4s0 (port reuse enabled)
[Node x9y8z7w6] Started at 192.168.1.74:8002
[Node x9y8z7w6] Discovered leader: a1b2c3d4
[x9y8z7w6] <- HEARTBEAT from a1b2c3d4
[x9y8z7w6] <- REPLICATE seq=5 type=REGISTRY from a1b2c3d4
```

Follower joined at seq=5 (late joining). It receives messages from the current sequence onwards.

## Key Behaviors

### Late Joining

When a follower starts after the leader:
- It joins the multicast group
- Receives messages from the current sequence number onwards
- Does NOT receive messages sent before it joined

Example:
```
Leader sends: seq 1, 2, 3, 4, 5, 6...
Follower joins at time when leader is at seq=5
Follower receives: seq 5, 6, 7, 8...
```


### SO_REUSEPORT

Multiple processes can listen on port 9999 simultaneously:
- Leader receives its own multicast messages (loopback)
- Followers receive leader's messages
- All nodes share the same multicast port


## Firewall Configuration (TODO: Check for sure)

### Linux (UFW)
```bash
sudo ufw allow 8001/tcp
sudo ufw allow 8002/tcp
sudo ufw allow 8888/udp
sudo ufw allow 9999/udp
```

### Windows
```cmd
netsh advfirewall firewall add rule name="LogStream TCP" dir=in action=allow protocol=TCP localport=8001,8002
netsh advfirewall firewall add rule name="LogStream UDP" dir=in action=allow protocol=UDP localport=8888,9999
```

## Troubleshooting

### Check Network Connectivity
```bash
# Ping other machine
ping 192.168.1.30

# Check if ports are open
telnet 192.168.1.30 8001

# View network interfaces
ip addr show          # Linux
ipconfig /all         # Windows
```

### Docker Logs
```bash
# All containers
docker compose logs -f

# Specific container
docker compose logs -f leader
docker compose logs -f broker1
docker compose logs -f broker2

# Check container status
docker compose ps
```

### Common Issues (TODO: Check again)

**No messages received:**
- Check firewall rules on all machines
- Verify multicast is enabled on network interfaces
- Ensure both nodes are on the same network subnet
- Check router supports multicast (some don't)

**Port already in use:**
- Stop other instances: `docker compose down`
- Kill process: `lsof -ti:8001 | xargs kill -9` (Linux/macOS)

**Docker permission denied:**
- Add user to docker group: `sudo usermod -aG docker $USER`
- Then log out and back in for changes to take effect

## Development

### Build Locally
```bash
go build -o logstream main.go

# Or use the rebuild script (cleans up processes/ports first)
./scripts/rebuild.sh      # Linux/macOS
scripts\rebuild.bat       # Windows
```

### Run with Custom Config
```bash
export NODE_ADDRESS="192.168.1.25:8001"
export MULTICAST_GROUP="239.0.0.1:9999"
export BROADCAST_PORT="8888"

go run main.go
```

### Regenerate Protocol Buffers
```bash
protoc --go_out=. --go_opt=paths=source_relative \
    internal/protocol/broker.proto
```

## Testing Summary

| Test Type | Platform | Isolation | Best For |
|-----------|----------|-----------|----------|
| Docker | Any | Container networks | Development, CI/CD |
| Network Namespaces | Linux | Virtual interfaces | Quick local testing |
| Vagrant | Linux | Full VMs (KVM) | Pre-production validation |
| Manual | Any | Physical/VM | Production deployment |



### Protocol Buffers Schema
```protobuf
message Message {
    MessageType type = 1;
    string node_id = 2;
    uint64 sequence = 3;
    bytes data = 4;
}

enum MessageType {
    HEARTBEAT = 0;
    REPLICATE = 1;
}
```

### Node ID Generation

Each node generates a unique 8-character ID using:
```
SHA256(IP:PORT + timestamp + random_bytes)[:8]
```

Example: `a1b2c3d4`, `x9y8z7w6`


## License

MIT License