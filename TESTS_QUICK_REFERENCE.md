# LogStream Tests - Quick Reference

**Quick command reference for running all LogStream tests**

---

## Build First

```bash
go build -o logstream main.go
```

---

## Unit Tests (72 tests)

```bash
# All unit tests
go test -v ./tests/unit/

# With race detection
go test -v -race ./tests/unit/

# With coverage
go test -v -coverprofile=coverage.out ./tests/unit/
go tool cover -html=coverage.out -o coverage.html
```

---

## Integration Tests - Local Mode (Network Namespaces)

### One-time Setup
```bash
sudo ./deploy/netns/setup-netns.sh
sudo ip netns list  # Verify: should show logstream-a, logstream-b, logstream-c
```

### Individual Tests
```bash
cd tests/linux

# Core functionality
./test-single.sh local               # Single node startup
./test-trio.sh local                 # 3-node cluster formation
./test-sequence.sh local             # Sequence number progression
./test-producer-consumer.sh local    # Producer → Broker → Consumer flow

# Advanced features
./test-election-automatic.sh local   # Automatic leader election
./test-stream-failover.sh local      # Stream failover on broker failure
./test-view-sync.sh local           # View-synchronous protocol
./test-state-exchange.sh local      # State exchange protocol

# Client features
./test-client-cleanup.sh local       # Client timeout/cleanup
./test-fifo-ordering.sh local        # FIFO ordering guarantees
./test-one-to-one-mapping.sh local   # Producer exclusivity

# Network tests
./test-protocol-compliance.sh local  # Protocol verification (creates PCAP)
./test-network-analysis.sh local    # Network behavior analysis (creates PCAP)
```

### All Tests at Once
```bash
cd tests/linux/integration
./test-all-local.sh  # Runs all 11 functional + 2 network tests
```

---

## Integration Tests - Docker Mode

### Individual Tests
```bash
cd tests/linux
./test-single.sh docker
./test-trio.sh docker
./test-election-automatic.sh docker
./test-producer-consumer.sh docker
./test-stream-failover.sh docker
# ... (same test names, just use 'docker' instead of 'local')
```

### All Tests
```bash
cd tests/linux/integration
./test-all-docker.sh
```

### Cleanup
```bash
cd deploy/docker/compose
docker compose down --remove-orphans
```

---

## Integration Tests - Vagrant Mode

### Setup (One-time)
```bash
cd deploy/vagrant
vagrant up  # Takes 10-15 minutes first time
vagrant status  # Verify VMs are running
```

### Run Tests
```bash
cd tests/linux/integration
./test-all-vagrant.sh

# Or individual tests
cd tests/linux
./test-single.sh vagrant
./test-trio.sh vagrant
```

### Vagrant Management
```bash
cd deploy/vagrant
vagrant halt           # Stop VMs
vagrant up            # Start VMs
vagrant destroy -f    # Delete VMs completely
vagrant status        # Check VM status
```

---

## Network Tests + Wireshark

### Run Network Tests
```bash
cd tests/linux
./test-protocol-compliance.sh local  # Creates PCAP file
./test-network-analysis.sh local    # Detailed traffic analysis
```

### Find PCAP Files
```bash
# Protocol compliance test
ls -dt /tmp/protocol_compliance_* | head -1
# File: /tmp/protocol_compliance_TIMESTAMP/protocol_capture.pcap

# Network analysis test
ls -dt /tmp/logstream_captures_* | head -1
# File: /tmp/logstream_captures_TIMESTAMP/logstream_network.pcap
```

### Analyze with Wireshark
```bash
# Protocol compliance PCAP
PCAP_DIR=$(ls -dt /tmp/protocol_compliance_* | head -1)
wireshark "$PCAP_DIR/protocol_capture.pcap"

# Network analysis PCAP
PCAP_DIR=$(ls -dt /tmp/logstream_captures_* | head -1)
wireshark "$PCAP_DIR/logstream_network.pcap"

# Command-line analysis
tshark -r "$PCAP_DIR/protocol_capture.pcap" -q -z io,phs  # Protocol hierarchy
tshark -r "$PCAP_DIR/protocol_capture.pcap" -Y "udp.dstport == 9999" | wc -l  # Count multicast
```

### Wireshark Filters
```
All LogStream:    (udp.port == 9999) or (tcp.port == 8001) or (udp.port == 8888)
Multicast only:   udp.dstport == 9999 && ip.dst == 239.0.0.1
Broadcast only:   udp.port == 8888
TCP only:         tcp.port == 8001
```

---

## Stress/Performance Tests

```bash
go test -v ./tests/stress/       # Stress tests
go test -v ./tests/chaos/        # Chaos engineering tests
go test -v -bench=. ./tests/unit/  # Benchmarks
```

---

## Test Logs

```bash
# Integration test logs
ls -la test-logs/local-all-*/
ls -la test-logs/docker-all-*/
ls -la test-logs/vagrant-all-*/

# View specific test log
cat test-logs/local-all-*/election-automatic/leader-node-a.log
cat test-logs/local-all-*/election-automatic/broker1-node-b.log
```

---

## Cleanup

```bash
# Kill processes
pkill -f logstream

# Clean network namespaces
sudo ./deploy/netns/cleanup.sh

# Clean Docker
cd deploy/docker/compose && docker compose down

# Clean Vagrant
cd deploy/vagrant && vagrant destroy -f
```

---

## Quick Test Combinations

### Fastest Verification (< 2 minutes)
```bash
go test -v ./tests/unit/
cd tests/linux && ./test-single.sh local
```

### Core Functionality (< 5 minutes)
```bash
cd tests/linux
./test-single.sh local
./test-trio.sh local
./test-producer-consumer.sh local
./test-election-automatic.sh local
```

### Complete Local Test Suite (10-15 minutes)
```bash
go build -o logstream main.go
go test -v ./tests/unit/
cd tests/linux/integration && ./test-all-local.sh
```

### Full Multi-Mode Testing (30-45 minutes)
```bash
go build -o logstream main.go
go test -v ./tests/unit/
cd tests/linux/integration
./test-all-local.sh
./test-all-docker.sh
./test-all-vagrant.sh  # Requires: cd deploy/vagrant && vagrant up
```

---

## Expected Results (After All Fixes)

**Unit Tests**: 72/72 PASS
**Integration Tests**: 11/11 PASS
- test-single.sh PASS
- test-trio.sh PASS
- test-sequence.sh PASS
- test-producer-consumer.sh PASS
- test-election-automatic.sh PASS
- test-client-cleanup.sh PASS
- test-fifo-ordering.sh PASS
- test-one-to-one-mapping.sh PASS
- test-stream-failover.sh PASS
- test-view-sync.sh PASS
- test-state-exchange.sh PASS

**Network Tests**: 2/2 PASS (false negatives on loopback clearly marked)
- test-protocol-compliance.sh PASS
- test-network-analysis.sh PASS

---

## One-Liner Commands

```bash
# Build and test everything (local mode)
go build -o logstream main.go && go test -v ./tests/unit/ && cd tests/linux/integration && ./test-all-local.sh

# Just the 4 critical tests (election/failover/recovery)
cd tests/linux && ./test-election-automatic.sh local && ./test-stream-failover.sh local && ./test-view-sync.sh local && ./test-state-exchange.sh local

# Clean everything and rebuild
pkill -f logstream; sudo ./deploy/netns/cleanup.sh; go build -o logstream main.go

# Network test with immediate Wireshark analysis
cd tests/linux && ./test-protocol-compliance.sh local && wireshark "$(ls -dt /tmp/protocol_compliance_* | head -1)/protocol_capture.pcap"
```

---

## Troubleshooting

```bash
# Network namespaces not set up
sudo ./deploy/netns/setup-netns.sh
ip link show br-logstream  # Should show UP state

# Processes stuck
ps aux | grep logstream
pkill -f logstream
sudo lsof -i :8001
sudo lsof -i :8888

# Binary not rebuilt
ls -lh logstream
go build -o logstream main.go

# Docker issues
docker ps -a
cd deploy/docker/compose && docker compose down
docker system prune -f

# Vagrant issues
cd deploy/vagrant
vagrant status
vagrant reload  # Restart VMs
vagrant destroy -f && vagrant up  # Full reset
```

---

## Environment Variables

```bash
# Node configuration (for manual testing)
export NODE_ADDRESS=172.20.0.10:8001
export MULTICAST_GROUP=239.0.0.1:9999
export BROADCAST_PORT=8888

# Producer/Consumer configuration
export LEADER_ADDRESS=172.20.0.10:8001
export TOPIC=my-topic
```

---

## CI/CD Script Example

```bash
#!/bin/bash
set -e

echo "=== Building ==="
go build -o logstream main.go

echo "=== Unit Tests ==="
go test -v -race ./tests/unit/

echo "=== Integration Tests ==="
cd tests/linux/integration
./test-all-local.sh

echo "=== All Tests Passed! ==="
```
