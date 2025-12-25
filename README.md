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

Creates 3 isolated containers on a virtual network (172.25.0.0/24):
- Leader: 172.25.0.10:8001
- Broker1: 172.25.0.20:8002
- Broker2: 172.25.0.30:8003

Linux/macOS:
```bash
# Start containers
sudo bash scripts/run-docker.sh

# Watch live logs
sudo docker compose logs -f

# Stop containers
sudo bash scripts/stop-docker.sh
```

Windows:
```cmd
# Start containers
scripts\run-docker.bat

# Watch live logs
docker compose logs -f

# Stop containers
scripts\stop-docker.bat
```

### 2. Local Testing (Same Machine, Two Terminals)

Tests multicast loopback on a single machine using different ports.

Linux/macOS:

Terminal 1 (Leader):
```bash
bash scripts/run-local.sh leader
```

Terminal 2 (Follower):
```bash
bash scripts/run-local.sh follower
```

Windows:

Command Prompt 1 (Leader):
```cmd
scripts\run-local.bat leader
```

Command Prompt 2 (Follower):
```cmd
scripts\run-local.bat follower
```

Stop: Press Ctrl+C in each terminal

### 3. Physical Machines 

Tests across actual network infrastructure with multiple computers.

Machine 1 (Leader):
```bash
# Linux/macOS
bash scripts/run-local.sh leader 192.168.1.25:8001

# Windows
scripts\run-local.bat leader 192.168.1.25:8001
```

Machine 2 (Follower):
```bash
# Linux/macOS
bash scripts/run-local.sh follower 192.168.1.30:8002

# Windows
scripts\run-local.bat follower 192.168.1.30:8002
```

**Important**: Configure firewall rules on both machines (see Firewall Configuration below).

### 4. Virtual Machines 

Use VirtualBox or VMware with bridged networking:

1. Create 2 VMs with Ubuntu Server
2. Set network mode to "Bridged Adapter" (not NAT)
3. Install Go 1.21+ on each VM
4. Clone/copy LogStream to both VMs
5. Configure firewall on each VM
6. Run leader on VM1, follower on VM2


## Configuration

### Environment Variables

- `IS_LEADER`: Set to "true" for leader node, "false" for followers
- `NODE_ADDRESS`: Node's IP:PORT (auto-detected if not provided)
- `MULTICAST_GROUP`: Multicast address (default: 239.0.0.1:9999)
- `BROADCAST_PORT`: UDP broadcast port (default: 8888)

### Auto-Detection

If NODE_ADDRESS is not specified, the system automatically:
1. Scans available network interfaces
2. Selects the primary non-loopback interface
3. Assigns default port (8001 for leader, 8002 for follower)

## Expected Output

### Leader Node
```
=== LogStream Configuration ===
Node Address:      192.168.1.74:8001
Is Leader:         true
Multicast Group:   239.0.0.1:9999
Broadcast Port:    8888
Network Interface: 192.168.1.74
================================

[Multicast] Joined group 239.0.0.1 on interface wlp4s0 (port reuse enabled)
[Broker a1b2c3d4] Started at 192.168.1.74:8001 (leader=true)
[Leader a1b2c3d4] → HEARTBEAT
[Leader a1b2c3d4] → REPLICATE seq=1 type=REGISTRY
[a1b2c3d4] ← HEARTBEAT from a1b2c3d4 (sender: 192.168.1.74:xxxxx)
[a1b2c3d4] ← REPLICATE seq=1 type=REGISTRY from a1b2c3d4
```

The leader sends messages and also receives its own via multicast loopback (this is expected behavior).

### Follower Node
```
=== LogStream Configuration ===
Node Address:      192.168.1.74:8002
Is Leader:         false
Multicast Group:   239.0.0.1:9999
Broadcast Port:    8888
Network Interface: 192.168.1.74
================================

[Multicast] Joined group 239.0.0.1 on interface wlp4s0 (port reuse enabled)
[Broker x9y8z7w6] Started at 192.168.1.74:8002 (leader=false)
[x9y8z7w6] ← HEARTBEAT from a1b2c3d4 (sender: 192.168.1.74:xxxxx)
[x9y8z7w6] ← REPLICATE seq=5 type=REGISTRY from a1b2c3d4
[x9y8z7w6] ← HEARTBEAT from a1b2c3d4
[x9y8z7w6] ← REPLICATE seq=6 type=REGISTRY from a1b2c3d4
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
- Use `sudo` on Linux: `sudo bash scripts/run-docker.sh`
- Or add user to docker group: `sudo usermod -aG docker $USER`

## Development

### Build Locally
```bash
go build -o logstream main.go
```

### Run with Custom Config
```bash
export IS_LEADER="true"
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

| Test Type | Machines | Setup Time | Realism | Best For |
|-----------|----------|------------|---------|----------|
| Docker | 1 (host) | 1 min | Medium | Development, CI/CD |
| Local | 1 (host) | 30 sec | Low | Quick testing |
| VMs | 1 (host + VMs) | 30 min | High | Pre-production validation |
| Physical | 2+ | 5 min | Highest | Production deployment |



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