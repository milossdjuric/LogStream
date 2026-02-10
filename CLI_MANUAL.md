# LogStream CLI Manual

Command-line interface reference for LogStream cluster management tool, broker, producer, and consumer.

---

## Table of Contents

1. [Quick Start - Copy-Paste Examples](#quick-start---copy-paste-examples)
2. [Cluster Discovery via UDP Broadcast](#cluster-discovery-via-udp-broadcast)
3. [Build All Binaries](#build-all-binaries)
4. [logstreamctl - Cluster Management](#logstreamctl---cluster-management)
5. [All Broker Start Combinations](#all-broker-start-combinations)
6. [All Producer Start Combinations](#all-producer-start-combinations)
7. [All Consumer Start Combinations](#all-consumer-start-combinations)
8. [Process Management](#process-management)
9. [Direct Binary Usage (Without logstreamctl)](#direct-binary-usage-without-logstreamctl)
10. [Environment Variables](#environment-variables)
11. [Debug Commands](#debug-commands)

---

## Quick Start - Copy-Paste Examples

**Build everything first:**
```bash
go build -o logstreamctl cmd/logstreamctl/main.go && go build -o logstream main.go && go build -o producer cmd/producer/main.go && go build -o consumer cmd/consumer/main.go
```

**Initialize (first time only):**
```bash
./logstreamctl config init
```

**Terminal 1 - Start broker (auto-detect IP):**
```bash
./logstreamctl start broker --name node1
```

**Terminal 2 - Start producer (discovers cluster via UDP broadcast):**
```bash
./logstreamctl start producer --name prod1 --topic logs --rate 5
```

**Terminal 3 - Start consumer (discovers cluster via UDP broadcast):**
```bash
./logstreamctl start consumer --name cons1 --topic logs
```

**Stop everything:**
```bash
./logstreamctl stop --all
```

---

## Cluster Discovery via UDP Broadcast

All LogStream components (brokers, producers, consumers) use **UDP broadcast** for automatic cluster discovery. No manual IP configuration is required when all nodes are on the same subnet.

### How It Works

```
Client (Producer/Consumer)              Leader Broker
        |                                      |
        |---- JOIN (UDP broadcast) ----------->| port 8888
        |     (subnet broadcast address)       |
        |                                      |
        |<--- JOIN_RESPONSE (UDP unicast) -----|
        |     (leader address returned)        |
        |                                      |
        |---- PRODUCE/CONSUME (TCP) ---------> | port 8001
        |     (connect to discovered leader)   |
```

### Discovery Features

| Feature | Description |
|---------|-------------|
| **Auto-detect broadcast address** | Calculates subnet-specific broadcast (e.g., 172.28.226.255) |
| **Circuit breaker pattern** | Exponential backoff on failures (1s, 1.5s, 2.25s... max 10s) |
| **Split-brain detection** | Detects multiple leaders, waits for election to resolve |
| **Retry logic** | 5 attempts with increasing delays |

### When to Use Explicit `--leader`

Use the `--leader` flag explicitly when:
- Clients are on a **different subnet** than brokers (broadcast doesn't cross subnets)
- Firewall blocks **UDP port 8888** (broadcast port)
- Running in **Docker** with host networking disabled
- Testing with **localhost** (broadcast unreliable on loopback)

```bash
# Cross-subnet or firewall scenarios - specify leader explicitly
./logstreamctl start producer --name prod1 --leader 192.168.1.10:8001 --topic logs
```

---

## Build All Binaries

```bash
# Build all at once (copy-paste this)
go build -o logstreamctl cmd/logstreamctl/main.go && \
go build -o logstream main.go && \
go build -o producer cmd/producer/main.go && \
go build -o consumer cmd/consumer/main.go

# Verify builds
ls -la logstreamctl logstream producer consumer
```

---

## logstreamctl - Cluster Management

### Command Reference

| Command | Description |
|---------|-------------|
| `./logstreamctl start broker` | Start a broker node |
| `./logstreamctl start producer` | Start a producer client |
| `./logstreamctl start consumer` | Start a consumer client |
| `./logstreamctl stop <name>` | Stop a specific process by name |
| `./logstreamctl stop --all` | Stop all managed processes |
| `./logstreamctl list` | List all managed processes with status |
| `./logstreamctl logs <name>` | View logs for a process |
| `./logstreamctl logs <name> --follow` | Follow logs in real-time |
| `./logstreamctl status` | Show status of local processes |
| `./logstreamctl election <name>` | Trigger election on a broker |
| `./logstreamctl config init` | Initialize state directory |
| `./logstreamctl config show` | Show state directory paths |
| `./logstreamctl help` | Show help |

### First-Time Setup

```bash
# Initialize state directory (~/.logstream/)
./logstreamctl config init

# Verify
./logstreamctl config show
```

---

## All Broker Start Combinations

### Address Options

The broker `--address` flag supports three formats:

| Format | Example | Description |
|--------|---------|-------------|
| Omitted | `--name node1` | Auto-detect IP, use port 8001 |
| Port only | `--address 8002` | Auto-detect IP, use specified port |
| Full IP:PORT | `--address 172.28.226.255:8001` | Use exact address |

### Broker Examples (Copy-Paste Ready)

**Simplest - auto-detect everything:**
```bash
./logstreamctl start broker --name node1
```

**Auto-detect IP, custom port:**
```bash
./logstreamctl start broker --name node1 --address 8001
```

**Explicit IP and port:**
```bash
./logstreamctl start broker --name node1 --address 172.28.226.255:8001
```

**Run in background (daemon mode):**
```bash
./logstreamctl start broker --name node1 --background
```

**Background with explicit address:**
```bash
./logstreamctl start broker --name node1 --address 172.28.226.255:8001 --background
```

**Custom multicast group:**
```bash
./logstreamctl start broker --name node1 --multicast 239.0.0.5:9999
```

**Custom broadcast port:**
```bash
./logstreamctl start broker --name node1 --broadcast-port 8889
```

**Explicit leader (bypass broadcast discovery):**
```bash
./logstreamctl start broker --name node2 --leader 192.168.1.10:8001
```

**Custom timing (faster failure detection):**
```bash
./logstreamctl start broker --name node1 --broker-hb-interval 3 --suspicion-timeout 6 --failure-timeout 10
```

**Full options:**
```bash
./logstreamctl start broker --name node1 --address 172.28.226.255:8001 --multicast 239.0.0.1:9999 --broadcast-port 8888 --max-records 50000 --background
```

### Multi-Broker Cluster (Same Machine)

```bash
# Terminal 1 - Leader (port 8001)
./logstreamctl start broker --name node1 --address 8001

# Terminal 2 - Follower (port 8002)
./logstreamctl start broker --name node2 --address 8002

# Terminal 3 - Follower (port 8003)
./logstreamctl start broker --name node3 --address 8003
```

### Broker Options Reference

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--name` | `-n` | (required) | Unique process name |
| `--address` | `-a` | auto:8001 | IP:PORT, or just PORT, or omit for auto |
| `--multicast` | `-m` | 239.0.0.1:9999 | Multicast group for heartbeats |
| `--broadcast-port` | `-b` | 8888 | UDP broadcast port for discovery |
| `--leader` | `-l` | (auto-discover) | Explicit leader address (bypasses broadcast) |
| `--max-records` | | 10000 | Max records per topic log (0 = unlimited) |
| `--broker-hb-interval` | | 5 | Leader->follower heartbeat interval in seconds |
| `--client-hb-interval` | | 10 | Leader->client heartbeat interval in seconds |
| `--broker-timeout` | | 30 | Dead broker removal timeout in seconds |
| `--client-timeout` | | 30 | Dead client removal timeout in seconds |
| `--follower-hb-interval` | | 2 | Follower->leader heartbeat interval in seconds |
| `--suspicion-timeout` | | 10 | Leader suspicion timeout in seconds |
| `--failure-timeout` | | 15 | Leader failure timeout in seconds |
| `--background` | `-bg` | false | Run as daemon |

---

## All Producer Start Combinations

Producers discover the cluster leader automatically via **UDP broadcast on port 8888**. The `--leader` flag is optional.

### Producer Modes

| Mode | Flag | Description |
|------|------|-------------|
| Auto-discover | (default) | Discovers cluster leader via UDP broadcast |
| Interactive | `--rate 0` | Type messages manually in terminal |
| Auto rate | `--rate N` | Send N messages per second automatically |
| Interval | `--interval N` | Send 1 message every N milliseconds (for slow rates) |
| Limited count | `--count N` | Stop after sending N messages |
| Background | `--background` | Run as daemon (requires --rate or --interval) |

### Producer Examples (Copy-Paste Ready)

**Auto-discover via UDP broadcast, interactive mode:**
```bash
./logstreamctl start producer --name prod1 --topic logs
```

**Auto-discover via UDP broadcast, 5 messages per second:**
```bash
./logstreamctl start producer --name prod1 --topic logs --rate 5
```

**Auto-discover via UDP broadcast, 10 messages per second:**
```bash
./logstreamctl start producer --name prod1 --topic logs --rate 10
```

**Slow rate - 1 message every 2 seconds (using --interval in milliseconds):**
```bash
./logstreamctl start producer --name prod1 --topic logs --interval 2000
```

**Slow rate - 1 message every 5 seconds:**
```bash
./logstreamctl start producer --name prod1 --topic logs --interval 5000
```

**Slow rate - 1 message every 10 seconds:**
```bash
./logstreamctl start producer --name prod1 --topic logs --interval 10000
```

**Explicit leader address (when UDP broadcast doesn't work - cross-subnet, firewall, etc.):**
```bash
./logstreamctl start producer --name prod1 --leader 172.28.226.255:8001 --topic logs --rate 5
```

**Send exactly 100 messages then stop:**
```bash
./logstreamctl start producer --name prod1 --topic logs --rate 5 --count 100
```

**Custom message template:**
```bash
./logstreamctl start producer --name prod1 --topic logs --rate 5 --message "sensor-data"
```

**Background mode (daemon):**
```bash
./logstreamctl start producer --name prod1 --topic logs --rate 5 --background
```

**Custom heartbeat and reconnection timing:**
```bash
./logstreamctl start producer --name prod1 --topic logs --rate 5 --hb-interval 5 --hb-timeout 30 --reconnect-attempts 20 --reconnect-delay 2
```

**Different topics:**
```bash
./logstreamctl start producer --name prod1 --topic sensors --rate 5
./logstreamctl start producer --name prod2 --topic metrics --rate 5
./logstreamctl start producer --name prod3 --topic events --rate 5
```

**Full options with explicit leader:**
```bash
./logstreamctl start producer --name prod1 --leader 172.28.226.255:8001 --topic logs --port 8002 --rate 10 --count 1000 --message "log-entry" --background
```

### Producer Options Reference

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--name` | `-n` | (required) | Unique process name |
| `--leader` | `-l` | (auto-discover) | Leader broker IP:PORT (discovers via UDP broadcast if omitted) |
| `--topic` | `-t` | logs | Topic name |
| `--port` | `-p` | (auto-assign) | Client TCP port |
| `--address` | `-a` | (auto-detect) | Advertised IP (for WSL/NAT: set to Windows host LAN IP) |
| `--rate` | `-r` | 0 | Messages/sec (0 = interactive) |
| `--interval` | `-i` | 0 | Interval in ms between messages (overrides --rate) |
| `--count` | `-c` | 0 | Total messages (0 = unlimited) |
| `--message` | `-m` | "log message" | Message template |
| `--hb-interval` | | 2 | Heartbeat interval in seconds |
| `--hb-timeout` | | 10 | Leader timeout in seconds before reconnect |
| `--reconnect-attempts` | | 10 | Max reconnection attempts |
| `--reconnect-delay` | | 5 | Delay between reconnection attempts in seconds |
| `--background` | `-bg` | false | Run as daemon |

### Rate vs Interval

Use `--rate` for fast rates (1+ messages per second):
- `--rate 1` = 1 message/sec
- `--rate 5` = 5 messages/sec
- `--rate 10` = 10 messages/sec

Use `--interval` for slow rates (less than 1 message per second):
- `--interval 2000` = 1 message every 2 seconds (0.5 msg/sec)
- `--interval 5000` = 1 message every 5 seconds (0.2 msg/sec)
- `--interval 10000` = 1 message every 10 seconds (0.1 msg/sec)

If both are specified, `--interval` takes precedence.

---

## All Consumer Start Combinations

Consumers discover the cluster leader automatically via **UDP broadcast on port 8888**. The `--leader` flag is optional.

### Consumer Modes

| Mode | Flag | Description |
|------|------|-------------|
| Auto-discover | (default) | Discovers cluster leader via UDP broadcast |
| With analytics | (default) | Receive processed data with stats |
| Raw data | `--no-analytics` | Receive raw data only |
| Custom window | `--window N` | Analytics window in seconds (0 = broker default) |
| Custom interval | `--interval N` | Analytics update interval in ms (0 = broker default) |
| Background | `--background` | Run as daemon |

### Consumer Examples (Copy-Paste Ready)

**Auto-discover via UDP broadcast, with analytics:**
```bash
./logstreamctl start consumer --name cons1 --topic logs
```

**Auto-discover via UDP broadcast, raw data only:**
```bash
./logstreamctl start consumer --name cons1 --topic logs --no-analytics
```

**Explicit leader address (when UDP broadcast doesn't work - cross-subnet, firewall, etc.):**
```bash
./logstreamctl start consumer --name cons1 --leader 172.28.226.255:8001 --topic logs
```

**Background mode (daemon):**
```bash
./logstreamctl start consumer --name cons1 --topic logs --background
```

**Custom analytics window (30s) and update interval (2s):**
```bash
./logstreamctl start consumer --name cons1 --topic logs --window 30 --interval 2000
```

**Background without analytics:**
```bash
./logstreamctl start consumer --name cons1 --topic logs --no-analytics --background
```

**Custom heartbeat and reconnection timing:**
```bash
./logstreamctl start consumer --name cons1 --topic logs --hb-interval 5 --reconnect-attempts 20 --reconnect-delay 2
```

**Different topics with different analytics settings:**
```bash
./logstreamctl start consumer --name cons-sensors --topic sensors --window 30 --interval 500
./logstreamctl start consumer --name cons-metrics --topic metrics --window 120 --interval 5000
./logstreamctl start consumer --name cons-events --topic events
```

### Consumer Options Reference

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--name` | `-n` | (required) | Unique process name |
| `--leader` | `-l` | (auto-discover) | Leader broker IP:PORT (discovers via UDP broadcast if omitted) |
| `--topic` | `-t` | logs | Topic name |
| `--port` | `-p` | (auto-assign) | Client TCP port |
| `--address` | `-a` | (auto-detect) | Advertised IP (for WSL/NAT: set to Windows host LAN IP) |
| `--no-analytics` | | false | Disable analytics processing |
| `--window` | `-w` | 0 (broker default) | Analytics window in seconds |
| `--interval` | | 0 (broker default) | Analytics update interval in ms |
| `--hb-interval` | | 2 | Heartbeat interval in seconds |
| `--reconnect-attempts` | | 10 | Max reconnection attempts |
| `--reconnect-delay` | | 5 | Delay between reconnection attempts in seconds |
| `--background` | `-bg` | false | Run as daemon |

---

## Process Management

### List Processes

```bash
./logstreamctl list
```

Output:
```
NAME    TYPE      STATUS   PID     ADDRESS/LEADER       TOPIC   UPTIME
node1   broker    running  12345   172.28.226.255:8001  -       5m30s
prod1   producer  running  12346   172.28.226.255:8001  logs    5m28s
cons1   consumer  running  12347   172.28.226.255:8001  logs    5m25s
```

### Stop Processes

```bash
# Stop specific process
./logstreamctl stop node1

# Stop all processes
./logstreamctl stop --all
```

### View Logs

```bash
# Last 50 lines
./logstreamctl logs node1

# Last 100 lines
./logstreamctl logs node1 --lines 100

# Follow in real-time (like tail -f)
./logstreamctl logs node1 --follow
```

### Trigger Election

```bash
# Force a new leader election
./logstreamctl election node1

# Then watch the logs
./logstreamctl logs node1 --follow
```

### Status

```bash
./logstreamctl status
```

---

## Complete Workflow Examples

### Example 1: Single Broker with Producer and Consumer (UDP Broadcast Discovery)

```bash
# Terminal 1 - Broker (listens for JOIN broadcasts on UDP port 8888)
./logstreamctl start broker --name node1

# Terminal 2 - Producer (discovers broker via UDP broadcast, 5 msg/sec)
./logstreamctl start producer --name prod1 --topic logs --rate 5

# Terminal 3 - Consumer (discovers broker via UDP broadcast)
./logstreamctl start consumer --name cons1 --topic logs
```

### Example 2: Three-Broker Cluster (UDP Broadcast Discovery)

```bash
# Terminal 1 - Leader (first broker becomes leader)
./logstreamctl start broker --name node1 --address 8001

# Terminal 2 - Follower 1 (discovers leader via UDP broadcast)
./logstreamctl start broker --name node2 --address 8002

# Terminal 3 - Follower 2 (discovers leader via UDP broadcast)
./logstreamctl start broker --name node3 --address 8003

# Terminal 4 - Producer (discovers leader via UDP broadcast)
./logstreamctl start producer --name prod1 --topic logs --rate 10

# Terminal 5 - Consumer (discovers leader via UDP broadcast)
./logstreamctl start consumer --name cons1 --topic logs
```

### Example 3: Background Daemons (UDP Broadcast Discovery)

```bash
# Start everything in background (all discover via UDP broadcast)
./logstreamctl start broker --name node1 --background
./logstreamctl start producer --name prod1 --topic logs --rate 5 --background
./logstreamctl start consumer --name cons1 --topic logs --background

# Check status
./logstreamctl list

# View logs
./logstreamctl logs node1 --follow
./logstreamctl logs prod1 --follow
./logstreamctl logs cons1 --follow

# Stop when done
./logstreamctl stop --all
```

### Example 4: Multiple Topics (UDP Broadcast Discovery)

```bash
# Start broker (listens for JOIN broadcasts)
./logstreamctl start broker --name node1

# Different terminals for different topics (all discover via UDP broadcast)
./logstreamctl start producer --name prod-sensors --topic sensors --rate 5
./logstreamctl start producer --name prod-metrics --topic metrics --rate 10
./logstreamctl start producer --name prod-events --topic events --rate 2

# Each consumer can have its own analytics window and update interval
./logstreamctl start consumer --name cons-sensors --topic sensors --window 30 --interval 500
./logstreamctl start consumer --name cons-metrics --topic metrics --window 120 --interval 5000
./logstreamctl start consumer --name cons-events --topic events
```

---

## State Directory Structure

All logstreamctl state is stored in `~/.logstream/`:

```
~/.logstream/
├── pids/                    # PID files for process tracking
│   ├── node1.pid
│   ├── prod1.pid
│   └── cons1.pid
├── logs/                    # Log files for background processes
│   ├── node1.log
│   ├── prod1.log
│   └── cons1.log
└── processes.json           # Process metadata
```

---

## Troubleshooting

**Process won't start - "already running":**
```bash
./logstreamctl list
./logstreamctl stop <name>
```

**Can't find binary:**
```bash
go build -o logstreamctl cmd/logstreamctl/main.go && go build -o logstream main.go && go build -o producer cmd/producer/main.go && go build -o consumer cmd/consumer/main.go
```

**Brokers not discovering each other:**
- Ensure all brokers are on the same subnet
- Check UDP broadcast port 8888 is not blocked
- Check UDP multicast 239.0.0.1:9999 is working

**View raw log file:**
```bash
cat ~/.logstream/logs/node1.log
tail -f ~/.logstream/logs/node1.log
```

---

## Direct Binary Usage (Without logstreamctl)

You can also run binaries directly without logstreamctl.

### Broker (./logstream)

The broker uses environment variables only (no CLI flags):

```bash
# Auto-detect address
./logstream

# With explicit address
NODE_ADDRESS=172.28.226.255:8001 ./logstream

# Full config
NODE_ADDRESS=172.28.226.255:8001 MULTICAST_GROUP=239.0.0.1:9999 BROADCAST_PORT=8888 ./logstream

# With explicit leader (bypass broadcast)
NODE_ADDRESS=172.28.226.255:8002 LEADER_ADDRESS=172.28.226.255:8001 ./logstream

# Custom timing
BROKER_HB_INTERVAL=3 CLIENT_TIMEOUT=60 FAILURE_TIMEOUT=20 ./logstream
```

### Producer (./producer)

The producer uses CLI flags (with LEADER_ADDRESS and TOPIC env var fallback):

```bash
# Auto-discover cluster, interactive mode
./producer -topic logs

# Auto-discover, auto mode at 10 msg/sec
./producer -topic logs -rate 10

# Explicit leader
./producer -leader 172.28.226.255:8001 -topic logs -rate 5

# Fixed port (for NAT/firewall port forwarding)
./producer -topic logs -port 8002

# Advertised address (WSL/NAT)
./producer -topic logs -port 8002 -address 192.168.1.50

# Custom heartbeat timing
./producer -topic logs -hb-interval 5 -hb-timeout 30

# Custom reconnection
./producer -topic logs -reconnect-attempts 20 -reconnect-delay 2

# Send 100 messages then exit
./producer -topic logs -rate 10 -count 100

# Slow rate (1 message every 5 seconds)
./producer -topic logs -interval 5000
```

### Consumer (./consumer)

The consumer uses CLI flags (with LEADER_ADDRESS and TOPIC env var fallback):

```bash
# Auto-discover cluster, with analytics
./consumer -topic logs

# Explicit leader
./consumer -leader 172.28.226.255:8001 -topic logs

# Raw data only (no analytics)
./consumer -topic logs -analytics=false

# Custom analytics window and interval
./consumer -topic logs -window 30 -interval 2000

# Fixed port (for NAT/firewall port forwarding)
./consumer -topic logs -port 8003

# Custom heartbeat timing
./consumer -topic logs -hb-interval 5

# Custom reconnection
./consumer -topic logs -reconnect-attempts 20 -reconnect-delay 2
```

---

## Network Namespace Testing

For multi-broker testing on a single machine:

```bash
# Setup (one-time)
sudo ./deploy/netns/setup-netns.sh

# Start brokers
sudo ip netns exec logstream-a env NODE_ADDRESS=172.20.0.10:8001 ./logstream &
sudo ip netns exec logstream-b env NODE_ADDRESS=172.20.0.20:8001 ./logstream &
sudo ip netns exec logstream-c env NODE_ADDRESS=172.20.0.30:8001 ./logstream &

# Start producer (explicit leader since broadcast may not cross namespaces)
sudo ip netns exec logstream-a ./producer -leader 172.20.0.10:8001 -topic test -rate 5 &

# Start consumer
sudo ip netns exec logstream-a ./consumer -leader 172.20.0.10:8001 -topic test &

# Cleanup
sudo ./deploy/netns/cleanup.sh
```

---

## Docker Testing

```bash
cd deploy/docker/compose

# Three broker cluster
docker compose -f trio.yaml up -d
docker compose -f trio.yaml logs -f
docker compose -f trio.yaml down
```

---

## Network Diagnostics

Find your IP address:

```bash
go run cmd/netdiag/main.go
```

---

## Environment Variables

### Broker Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_ADDRESS` | (auto-detect) | Node's IP:PORT (e.g., `192.168.1.10:8001`) |
| `MULTICAST_GROUP` | `239.0.0.1:9999` | Multicast address for heartbeats and replication |
| `BROADCAST_PORT` | `8888` | UDP broadcast port for cluster discovery |
| `LEADER_ADDRESS` | (auto-discover) | Explicit leader address (bypass broadcast) |
| `ANALYTICS_WINDOW_SECONDS` | `60` | Analytics window size in seconds |
| `MAX_RECORDS_PER_TOPIC` | `10000` | Max records per topic log (0 = unlimited) |
| `BROKER_HB_INTERVAL` | `5` | Leader->follower heartbeat interval in seconds |
| `CLIENT_HB_INTERVAL` | `10` | Leader->client heartbeat interval in seconds |
| `BROKER_TIMEOUT` | `30` | Dead broker removal timeout in seconds |
| `CLIENT_TIMEOUT` | `30` | Dead client removal timeout in seconds |
| `FOLLOWER_HB_INTERVAL` | `2` | Follower->leader heartbeat interval in seconds |
| `SUSPICION_TIMEOUT` | `10` | Seconds without leader heartbeat before suspicion |
| `FAILURE_TIMEOUT` | `15` | Seconds without leader heartbeat before failure |

### Producer Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `LEADER_ADDRESS` | (auto-discover) | Leader broker address (fallback for `-leader` flag) |
| `TOPIC` | `logs` | Topic to produce to (fallback for `-topic` flag) |

### Consumer Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `LEADER_ADDRESS` | (auto-discover) | Leader broker address (fallback for `-leader` flag) |
| `TOPIC` | `logs` | Topic to consume from (fallback for `-topic` flag) |

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
sudo ip netns exec logstream-a ./producer -leader 172.20.0.10:8001 -topic test -rate 5 &

# Start consumer
sudo ip netns exec logstream-a ./consumer -leader 172.20.0.10:8001 -topic test &
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
