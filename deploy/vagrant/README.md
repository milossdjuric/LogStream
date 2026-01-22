# Vagrant VM Deployment

Test LogStream on real Ubuntu VMs.

## Requirements

- [Vagrant](https://www.vagrantup.com/) 2.3+
- [VirtualBox](https://www.virtualbox.org/) 6.1+
- 4GB RAM available
- 10GB disk space

## Quick Start
```bash
# First time (downloads Ubuntu, ~5-10 min)
vagrant up

# Start cluster
./start-cluster.sh

# Run tests
cd ../../tests/linux
./test-trio.sh vagrant

# Stop cluster
cd ../../deploy/vagrant
./stop-cluster.sh

# Destroy VMs
./cleanup.sh
```

## VM Configuration

| VM | IP | Role |
|----|-----|------|
| leader | 192.168.56.10 | Leader |
| broker1 | 192.168.56.20 | Follower |
| broker2 | 192.168.56.30 | Follower |

## Manual Operations

### SSH into VMs
```bash
vagrant ssh leader
vagrant ssh broker1
vagrant ssh broker2
```

### View logs
```bash
vagrant ssh leader -c 'tail -f /tmp/logstream.log'
```

### Rebuild
```bash
vagrant ssh leader
cd /vagrant/logstream
go build -o logstream main.go
```

## Troubleshooting

### VMs won't start
```bash
vagrant destroy -f
vagrant up
```

### Network issues
```bash
vagrant ssh leader -c 'ping -c 2 192.168.56.20'
```

### Out of disk space
```bash
vagrant box prune
```

## Tips

- First `vagrant up` takes 5-10 min
- Use `vagrant halt` to stop, `vagrant up` to resume
- Code changes are synced via shared folder
- Use `vagrant snapshot save <name>` to save state