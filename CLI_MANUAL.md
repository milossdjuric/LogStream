# LogStream CLI Manual

Command-line interface reference for LogStream cluster management tool, broker, producer, and consumer.

---

## Table of Contents

1. [logstreamctl - Cluster Management](#logstreamctl---cluster-management)
2. [Build Commands](#build-commands)
3. [Broker Commands](#broker-commands)
4. [Producer Commands](#producer-commands)
5. [Consumer Commands](#consumer-commands)
6. [Network Diagnostics](#network-diagnostics)
7. [Environment Variables](#environment-variables)
8. [Debug Commands](#debug-commands)

---

## logstreamctl - Cluster Management

The `logstreamctl` tool manages LogStream processes across machines. Each machine runs its own `logstreamctl` instance, and processes communicate over LAN via the LogStream protocol (UDP broadcast for discovery, UDP multicast for replication, TCP for clients).

### Installation

```bash
go build -o logstreamctl cmd/logstreamctl/main.go
```

### Command Reference

| Command | Description |
|---------|-------------|
| `logstreamctl start broker` | Start a broker node |
| `logstreamctl start producer` | Start a producer client |
| `logstreamctl start consumer` | Start a consumer client |
| `logstreamctl stop <name>` | Stop a specific process by name |
| `logstreamctl stop --all` | Stop all managed processes |
| `logstreamctl list` | List all managed processes with status |
| `logstreamctl logs <name>` | View logs for a process |
| `logstreamctl logs <name> --follow` | Follow logs in real-time (like tail -f) |
| `logstreamctl status` | Show status of local processes |
| `logstreamctl election <name>` | Trigger election on a broker (sends SIGUSR1) |
| `logstreamctl config init` | Initialize state directory |
| `logstreamctl config show` | Show state directory paths |
| `logstreamctl help` | Show help |
| `logstreamctl version` | Show version |

### First-Time Setup

```bash
# Initialize state directory (~/.logstream/)
logstreamctl config init

# Verify the directory was created
logstreamctl config show
# Output:
# State directory: /home/user/.logstream
# PID directory:   /home/user/.logstream/pids
# Log directory:   /home/user/.logstream/logs
# State file:      /home/user/.logstream/processes.json
```

### Foreground vs Background Mode

**Foreground (default)**: Process runs in your terminal. You see output directly. Ctrl+C stops it. Terminal must stay open.

**Background (--background flag)**: Process runs as daemon. Output goes to log file. Terminal is free. Process survives terminal close.

```bash
# Foreground - blocks terminal, see output directly
logstreamctl start broker --name node1 --address 192.168.1.10:8001
# Press Ctrl+C to stop

# Background - returns immediately, output to ~/.logstream/logs/node1.log
logstreamctl start broker --name node1 --address 192.168.1.10:8001 --background
```

### Starting Processes

**Start a broker:**
```bash
# Foreground (default) - see output directly
logstreamctl start broker --name node1 --address 192.168.1.10:8001

# Background - runs as daemon
logstreamctl start broker --name node1 --address 192.168.1.10:8001 --background
```

**Start a producer:**
```bash
# Interactive mode (type messages manually)
logstreamctl start producer --name prod1 --leader 192.168.1.10:8001 --topic logs

# Auto mode - send 10 messages per second
logstreamctl start producer --name prod1 --leader 192.168.1.10:8001 --topic logs --rate 10

# Background mode (requires --rate > 0)
logstreamctl start producer --name prod1 --leader 192.168.1.10:8001 --topic logs --rate 10 --background
```

**Start a consumer:**
```bash
# Foreground - see received messages
logstreamctl start consumer --name cons1 --leader 192.168.1.10:8001 --topic logs

# Background - output to log file
logstreamctl start consumer --name cons1 --leader 192.168.1.10:8001 --topic logs --background
```

### Broker Options

| Option | Short | Description |
|--------|-------|-------------|
| `--name` | `-n` | Process name (required, must be unique) |
| `--address` | `-a` | Node address IP:PORT (required) |
| `--multicast` | `-m` | Multicast group (default: 239.0.0.1:9999) |
| `--broadcast-port` | `-b` | Broadcast port (default: 8888) |
| `--background` | `-bg` | Run in background (default: foreground) |

### Producer Options

| Option | Short | Description |
|--------|-------|-------------|
| `--name` | `-n` | Process name (required, must be unique) |
| `--leader` | `-l` | Leader broker address IP:PORT (required) |
| `--topic` | `-t` | Topic name (default: logs) |
| `--rate` | `-r` | Messages per second (default: 0 = interactive mode) |
| `--count` | `-c` | Total messages to send (default: 0 = unlimited) |
| `--message` | `-m` | Message template for auto mode |
| `--background` | `-bg` | Run in background (requires --rate > 0) |

### Consumer Options

| Option | Short | Description |
|--------|-------|-------------|
| `--name` | `-n` | Process name (required, must be unique) |
| `--leader` | `-l` | Leader broker address IP:PORT (required) |
| `--topic` | `-t` | Topic name (default: logs) |
| `--no-analytics` | | Disable analytics processing |
| `--background` | `-bg` | Run in background |

### Process Management

```bash
# List all managed processes
logstreamctl list
# Output:
# NAME    TYPE      STATUS   PID     ADDRESS/LEADER     TOPIC   UPTIME
# node1   broker    running  12345   192.168.1.10:8001  -       5m30s
# prod1   producer  running  12346   192.168.1.10:8001  logs    5m28s
# cons1   consumer  stopped  12347   192.168.1.10:8001  logs    -

# Stop a specific process
logstreamctl stop node1
# Output: Stopping 'node1' (PID 12345)...
#         Stopped 'node1'

# Stop all managed processes
logstreamctl stop --all

# View last 50 lines of logs
logstreamctl logs node1

# View last 100 lines
logstreamctl logs node1 --lines 100

# Follow logs in real-time (Ctrl+C to stop)
logstreamctl logs node1 --follow

# Show local process status
logstreamctl status
```

### Election Control

```bash
# Trigger election on a broker (sends SIGUSR1 signal)
logstreamctl election node1
# Output: Triggering election on 'node1' (PID 12345)...
#         Election signal sent
#         Check logs with: logstreamctl logs node1 --follow
```

### State Directory Structure

All logstreamctl state is stored in `~/.logstream/`:

```
~/.logstream/
├── pids/                    # PID files for process tracking
│   ├── node1.pid            # Contains: 12345
│   ├── prod1.pid            # Contains: 12346
│   └── cons1.pid            # Contains: 12347
├── logs/                    # Log files for background processes
│   ├── node1.log            # Broker stdout/stderr
│   ├── prod1.log            # Producer stdout/stderr
│   └── cons1.log            # Consumer stdout/stderr
└── processes.json           # Process metadata (JSON)
```

**PID Files** (`~/.logstream/pids/<name>.pid`):
- Plain text file containing the process ID
- Used to check if process is still running
- Automatically cleaned up when process stops

**Log Files** (`~/.logstream/logs/<name>.log`):
- Stdout and stderr from background processes
- Appended to (not overwritten) on restart
- View with: `logstreamctl logs <name>`
- Follow with: `logstreamctl logs <name> --follow`

**Process Metadata** (`~/.logstream/processes.json`):
```json
{
  "processes": {
    "node1": {
      "name": "node1",
      "type": "broker",
      "pid": 12345,
      "address": "192.168.1.10:8001",
      "started_at": "2026-02-04T10:30:00Z",
      "args": ["--name", "node1", "--address", "192.168.1.10:8001"]
    },
    "prod1": {
      "name": "prod1",
      "type": "producer",
      "pid": 12346,
      "leader": "192.168.1.10:8001",
      "topic": "logs",
      "started_at": "2026-02-04T10:31:00Z",
      "args": ["--name", "prod1", "--leader", "192.168.1.10:8001", "--topic", "logs"]
    }
  }
}
```

### Multi-Machine LAN Deployment

Each machine runs its own `logstreamctl`. Brokers discover each other via UDP broadcast on the LAN.

**Machine A (192.168.1.10) - Leader + Producer:**
```bash
# Initialize (first time only)
logstreamctl config init

# Start broker (will become leader as first node)
logstreamctl start broker --name node1 --address 192.168.1.10:8001

# In another terminal, or use --background:
logstreamctl start producer --name prod1 --leader 192.168.1.10:8001 --topic logs --rate 10 --background

# Check what's running
logstreamctl list
```

**Machine B (192.168.1.11) - Followers + Consumer:**
```bash
# Initialize (first time only)
logstreamctl config init

# Start two follower brokers (they discover node1 via broadcast)
logstreamctl start broker --name node2 --address 192.168.1.11:8001 --background
logstreamctl start broker --name node3 --address 192.168.1.11:8002 --background

# Start consumer (connects to leader on Machine A)
logstreamctl start consumer --name cons1 --leader 192.168.1.10:8001 --topic logs --background

# Check what's running
logstreamctl list
# NAME    TYPE      STATUS   PID     ADDRESS/LEADER     TOPIC   UPTIME
# node2   broker    running  23456   192.168.1.11:8001  -       2m30s
# node3   broker    running  23457   192.168.1.11:8002  -       2m28s
# cons1   consumer  running  23458   192.168.1.10:8001  logs    2m25s

# View consumer output
logstreamctl logs cons1 --follow
```

**Testing Failover:**
```bash
# On Machine A - kill the leader
logstreamctl stop node1

# On Machine B - watch the logs for election
logstreamctl logs node2 --follow
# You should see: "LEADER FAILURE DETECTED", "Starting election", "I am the new leader"

# Or trigger election manually
logstreamctl election node2
```

### Cleanup

```bash
# Stop all processes on this machine
logstreamctl stop --all

# View remaining state
ls -la ~/.logstream/

# Clear all logs (optional)
rm ~/.logstream/logs/*.log

# Full reset (removes all state)
rm -rf ~/.logstream/
```

### Troubleshooting

**Process won't start - "already running":**
```bash
# Check if it's really running
logstreamctl list

# If status shows "stopped" but error persists, the state is stale
# Stop will clean it up:
logstreamctl stop <name>
```

**Can't find binary:**
```bash
# logstreamctl looks for binaries in:
# 1. Current directory (./logstream, ./producer, ./consumer)
# 2. System PATH

# Make sure binaries are built:
go build -o logstream main.go
go build -o producer cmd/producer/main.go
go build -o consumer cmd/consumer/main.go
```

**Brokers not discovering each other:**
```bash
# Check network connectivity
ping 192.168.1.11

# Check UDP broadcast is working (same subnet required)
# Brokers use UDP broadcast on port 8888 for discovery

# Check multicast (used for heartbeats)
# Brokers use UDP multicast 239.0.0.1:9999
```

**View raw log file:**
```bash
# Log files are plain text
cat ~/.logstream/logs/node1.log

# Or use tail
tail -f ~/.logstream/logs/node1.log
```

---

## Build Commands

### Build All Components

```bash
# Build the cluster management tool
go build -o logstreamctl cmd/logstreamctl/main.go

# Build the main broker node
go build -o logstream main.go

# Build the producer client
go build -o producer cmd/producer/main.go

# Build the consumer client
go build -o consumer cmd/consumer/main.go

# Build all at once
go build -o logstreamctl cmd/logstreamctl/main.go && \
go build -o logstream main.go && \
go build -o producer cmd/producer/main.go && \
go build -o consumer cmd/consumer/main.go
```

### Regenerate Protocol Buffers

```bash
# After modifying internal/protocol/proto/messages.proto
cd internal/protocol/proto
./genproto.sh

# Or manually:
protoc --go_out=. --go_opt=paths=source_relative internal/protocol/proto/messages.proto
```

---

## Broker Commands

The broker is the main LogStream node that handles cluster membership, leader election, state replication, and data routing.

### Basic Startup

```bash
# Auto-detect network interface
./logstream

# Specify node address
NODE_ADDRESS=192.168.1.10:8001 ./logstream

# Specify multicast group
MULTICAST_GROUP=239.0.0.1:9999 ./logstream

# Full configuration
NODE_ADDRESS=192.168.1.10:8001 \
MULTICAST_GROUP=239.0.0.1:9999 \
BROADCAST_PORT=8888 \
./logstream
```

### Run in Network Namespace (Testing)

```bash
# Setup network namespaces first
sudo ./deploy/netns/setup-netns.sh

# Run leader in namespace logstream-a
sudo ip netns exec logstream-a env \
    NODE_ADDRESS=172.20.0.10:8001 \
    MULTICAST_GROUP=239.0.0.1:9999 \
    BROADCAST_PORT=8888 \
    ./logstream

# Run follower in namespace logstream-b
sudo ip netns exec logstream-b env \
    NODE_ADDRESS=172.20.0.20:8001 \
    MULTICAST_GROUP=239.0.0.1:9999 \
    BROADCAST_PORT=8888 \
    ./logstream

# Run follower in namespace logstream-c
sudo ip netns exec logstream-c env \
    NODE_ADDRESS=172.20.0.30:8001 \
    MULTICAST_GROUP=239.0.0.1:9999 \
    BROADCAST_PORT=8888 \
    ./logstream
```

### Run with Docker

```bash
cd deploy/docker/compose

# Single broker
docker compose -f single.yaml up -d
docker compose -f single.yaml logs -f
docker compose -f single.yaml down

# Three broker cluster
docker compose -f trio.yaml up -d
docker compose -f trio.yaml logs -f
docker compose -f trio.yaml down

# With producer and consumer
docker compose -f producer-consumer.yaml up -d
docker compose -f producer-consumer.yaml logs -f
docker compose -f producer-consumer.yaml down
```

### Run with Vagrant

```bash
cd deploy/vagrant

# Start VMs (first time: downloads box, provisions)
vagrant up

# SSH into leader VM
vagrant ssh leader

# SSH into follower VMs
vagrant ssh broker1
vagrant ssh broker2

# Rebuild binary on all VMs
./rebuild-binary.sh

# Check VM status
vagrant status

# Stop VMs (preserves state)
vagrant halt

# Destroy VMs (deletes everything)
vagrant destroy -f
```

---

## Producer Commands

The producer connects to the leader broker, gets assigned to a broker via consistent hash ring, and sends data messages.

### Basic Usage

```bash
# Connect to leader at default address
./producer

# Specify leader address
LEADER_ADDRESS=192.168.1.10:8001 ./producer

# Specify topic
TOPIC=my-topic LEADER_ADDRESS=192.168.1.10:8001 ./producer

# Full configuration
LEADER_ADDRESS=192.168.1.10:8001 \
TOPIC=logs \
PRODUCER_ID=producer-1 \
./producer
```

### Run in Network Namespace (Testing)

```bash
# Run producer in namespace logstream-a (where leader is)
sudo ip netns exec logstream-a env \
    LEADER_ADDRESS=172.20.0.10:8001 \
    TOPIC=test-topic \
    ./producer

# Run producer connecting to remote leader
sudo ip netns exec logstream-b env \
    LEADER_ADDRESS=172.20.0.10:8001 \
    TOPIC=test-topic \
    ./producer
```

### Producer Workflow

1. Producer sends PRODUCE message to leader (TCP)
2. Leader assigns broker via consistent hash ring
3. Leader responds with PRODUCE_ACK containing assigned broker address
4. Producer sends DATA messages to assigned broker (UDP)

---

## Consumer Commands

The consumer connects to the leader broker, gets assigned to a broker, subscribes to a topic, and receives data messages.

### Basic Usage

```bash
# Connect to leader at default address
./consumer

# Specify leader address
LEADER_ADDRESS=192.168.1.10:8001 ./consumer

# Specify topic
TOPIC=my-topic LEADER_ADDRESS=192.168.1.10:8001 ./consumer

# Full configuration
LEADER_ADDRESS=192.168.1.10:8001 \
TOPIC=logs \
CONSUMER_ID=consumer-1 \
./consumer
```

### Run in Network Namespace (Testing)

```bash
# Run consumer in namespace logstream-a
sudo ip netns exec logstream-a env \
    LEADER_ADDRESS=172.20.0.10:8001 \
    TOPIC=test-topic \
    ./consumer

# Run consumer connecting to remote leader
sudo ip netns exec logstream-c env \
    LEADER_ADDRESS=172.20.0.10:8001 \
    TOPIC=test-topic \
    ./consumer
```

### Consumer Workflow

1. Consumer sends CONSUME message to leader (TCP)
2. Leader responds with CONSUME_ACK containing assigned broker address
3. Consumer sends SUBSCRIBE message to assigned broker (TCP)
4. Broker streams RESULT messages to consumer (TCP)

---

## Network Diagnostics

The netdiag tool helps identify network interfaces and diagnose connectivity issues.

### Usage

```bash
# Build the tool
go build -o netdiag cmd/netdiag/main.go

# Run diagnostics
./netdiag

# Or run directly
go run cmd/netdiag/main.go
```

### Output

The tool displays:
- Available network interfaces
- IP addresses for each interface
- Multicast-capable interfaces
- Recommended NODE_ADDRESS for the system

---

## Environment Variables

### Broker Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_ADDRESS` | Auto-detect | Node's IP:PORT (e.g., `192.168.1.10:8001`) |
| `MULTICAST_GROUP` | `239.0.0.1:9999` | Multicast address for heartbeats and replication |
| `BROADCAST_PORT` | `8888` | UDP broadcast port for cluster discovery |

### Producer Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `LEADER_ADDRESS` | `127.0.0.1:8001` | Leader broker address |
| `TOPIC` | `default` | Topic to produce messages to |
| `PRODUCER_ID` | Auto-generated | Unique producer identifier |

### Consumer Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `LEADER_ADDRESS` | `127.0.0.1:8001` | Leader broker address |
| `TOPIC` | `default` | Topic to consume messages from |
| `CONSUMER_ID` | Auto-generated | Unique consumer identifier |

---

## Debug Commands

### Process Management

```bash
# List running LogStream processes
ps aux | grep logstream

# Kill all LogStream processes
pkill -f logstream

# Check port usage
sudo lsof -i :8001    # TCP broker port
sudo lsof -i :8888    # UDP broadcast port
sudo lsof -i :9999    # UDP multicast port
```

### Log Analysis

```bash
# Check cluster state
grep "ClusterState" node.log

# Check election progress
grep -E "ELECTION|I WIN|COORDINATOR" node.log

# Check heartbeats
grep "HEARTBEAT" node.log

# Check leader failure detection
grep "LEADER FAILURE" node.log

# Check state exchange
grep -E "STATE_EXCHANGE|VIEW_INSTALL" node.log

# Check replication
grep "REPLICATE" node.log

# Check client connections
grep -E "PRODUCE|CONSUME|SUBSCRIBE" node.log
```

### Network Namespace Management

```bash
# List namespaces
sudo ip netns list

# Check namespace IPs
sudo ip netns exec logstream-a ip addr
sudo ip netns exec logstream-b ip addr
sudo ip netns exec logstream-c ip addr

# Test connectivity
sudo ip netns exec logstream-a ping -c 2 172.20.0.20

# List processes in namespace
sudo ip netns pids logstream-a

# Kill processes in namespace
sudo ip netns pids logstream-a | xargs -r sudo kill -9

# Check bridge status
ip link show br-logstream

# Setup namespaces
sudo ./deploy/netns/setup-netns.sh

# Cleanup namespaces
sudo ./deploy/netns/cleanup.sh
```

### Docker Management

```bash
# List containers
docker ps -a

# View logs
docker logs <container-id>

# Stop all LogStream containers
cd deploy/docker/compose
docker compose down --remove-orphans

# Clean Docker resources
docker system prune -f
```

### Vagrant Management

```bash
cd deploy/vagrant

# Check VM status
vagrant status

# SSH into VMs
vagrant ssh leader
vagrant ssh broker1
vagrant ssh broker2

# Restart VMs
vagrant reload

# Rebuild from scratch
vagrant destroy -f && vagrant up
```

### Packet Capture

```bash
# Capture on bridge (local mode)
sudo tcpdump -i br-logstream -w capture.pcap

# Capture specific ports
sudo tcpdump -i br-logstream 'port 8001 or port 8888 or port 9999' -w capture.pcap

# Analyze with tshark
tshark -r capture.pcap -q -z io,phs

# Open in Wireshark
wireshark capture.pcap
```

### Wireshark Filters

```
# All LogStream traffic
(udp.port == 9999) or (tcp.port == 8001) or (udp.port == 8888)

# Multicast only (heartbeats/replication)
udp.dstport == 9999 && ip.dst == 239.0.0.1

# Broadcast only (discovery)
udp.port == 8888

# TCP only (client connections, elections)
tcp.port == 8001
```

---

## Quick Reference

### Start a 3-Node Cluster (Local Mode)

```bash
# Setup (one-time)
sudo ./deploy/netns/setup-netns.sh

# Build
go build -o logstream main.go

# Start leader
sudo ip netns exec logstream-a env NODE_ADDRESS=172.20.0.10:8001 MULTICAST_GROUP=239.0.0.1:9999 ./logstream &

# Start follower 1
sudo ip netns exec logstream-b env NODE_ADDRESS=172.20.0.20:8001 MULTICAST_GROUP=239.0.0.1:9999 ./logstream &

# Start follower 2
sudo ip netns exec logstream-c env NODE_ADDRESS=172.20.0.30:8001 MULTICAST_GROUP=239.0.0.1:9999 ./logstream &
```

### Start Producer and Consumer

```bash
# Build
go build -o producer cmd/producer/main.go
go build -o consumer cmd/consumer/main.go

# Start producer
sudo ip netns exec logstream-a env LEADER_ADDRESS=172.20.0.10:8001 TOPIC=test ./producer &

# Start consumer
sudo ip netns exec logstream-a env LEADER_ADDRESS=172.20.0.10:8001 TOPIC=test ./consumer &
```

### Clean Up Everything

```bash
# Kill all processes
pkill -f logstream
pkill -f producer
pkill -f consumer

# Kill processes in namespaces
for ns in logstream-a logstream-b logstream-c; do
    sudo ip netns pids $ns 2>/dev/null | xargs -r sudo kill -9
done

# Clean namespaces
sudo ./deploy/netns/cleanup.sh

# Clean Docker
cd deploy/docker/compose && docker compose down --remove-orphans

# Clean Vagrant
cd deploy/vagrant && vagrant destroy -f
```
