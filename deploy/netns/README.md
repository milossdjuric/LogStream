# Network Namespace Deployment

Lightweight localhost testing with isolated virtual networks.

## Requirements
- Linux
- `sudo` access
- `iproute2` package (pre-installed on most distros)

## Quick Start
```bash
# 1. Setup (one-time)
sudo ./setup-netns.sh

# 2. Start cluster
sudo ./start-cluster.sh

# 3. Test connectivity
sudo ip netns exec logstream-a ping -c 2 172.20.0.20

# 4. Cleanup
sudo ./cleanup.sh
```

## Network Topology
```
Bridge: br-logstream (172.20.0.1/24)
+- logstream-a: 172.20.0.10 (Leader)
+- logstream-b: 172.20.0.20 (Follower)
+- logstream-c: 172.20.0.30 (Follower)
```

## What Gets Created

1. **Virtual bridge** (`br-logstream`) - acts as a switch
2. **3 network namespaces** - isolated network stacks
3. **Virtual ethernet pairs** - connect namespaces to bridge

## Safety

- Completely isolated from your main network  
- Uses private IP ranges (172.20.0.0/24)  
- Does NOT affect WiFi/Ethernet  
- Cleans up completely with cleanup script  

## Running Tests

Use the main test scripts with `local` mode:
```bash
cd ../../tests/linux
sudo ./test-single.sh local
sudo ./test-trio.sh local
```

## Manual Commands
```bash
# List namespaces
ip netns list

# Execute command in namespace
sudo ip netns exec logstream-a <command>

# View processes in namespace
sudo ip netns pids logstream-a

# Enter namespace shell
sudo ip netns exec logstream-a bash
```

## Troubleshooting

**Bridge not found:**
```bash
sudo ./setup-netns.sh
```

**Processes still running:**
```bash
sudo ./stop-cluster.sh
sudo ./cleanup.sh
sudo ./start-cluster.sh
```

**Check logs:**
```bash
tail -f /tmp/logstream-a.log
```