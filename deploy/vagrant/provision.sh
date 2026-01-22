#!/bin/bash
set -e

echo "========================================"
echo "Provisioning LogStream VM"
echo "========================================"

# Update package list only (don't upgrade - saves time and bandwidth)
echo "-> Updating package list..."
sudo apt-get update

# Install Go 1.21 (check if already installed to save time)
if [ ! -f /usr/local/go/bin/go ]; then
    echo "-> Installing Go 1.21..."
    cd /tmp
    wget -q https://go.dev/dl/go1.21.6.linux-amd64.tar.gz
    sudo rm -rf /usr/local/go
    sudo tar -C /usr/local -xzf go1.21.6.linux-amd64.tar.gz
    rm go1.21.6.linux-amd64.tar.gz
    
    # Setup Go environment for vagrant user
    if ! grep -q "/usr/local/go/bin" /home/vagrant/.bashrc; then
        echo 'export PATH=$PATH:/usr/local/go/bin' >> /home/vagrant/.bashrc
        echo 'export GOPATH=/home/vagrant/go' >> /home/vagrant/.bashrc
    fi
else
    echo "-> Go already installed, skipping..."
fi

# Install minimal dependencies (removed upgrade -y)
echo "-> Installing dependencies..."
sudo apt-get install -y git build-essential net-tools vim

# Build LogStream binaries
echo "-> Building LogStream binaries..."
if [ -d "/vagrant/logstream" ]; then
    cd /vagrant/logstream
    
    # Build main binary
    sudo -u vagrant /usr/local/go/bin/go build -o logstream main.go
    echo "[OK] Binary built: /vagrant/logstream/logstream"
    
    # Build producer
    sudo -u vagrant /usr/local/go/bin/go build -o producer cmd/producer/main.go
    echo "[OK] Binary built: /vagrant/logstream/producer"
    
    # Build consumer
    sudo -u vagrant /usr/local/go/bin/go build -o consumer cmd/consumer/main.go
    echo "[OK] Binary built: /vagrant/logstream/consumer"
fi

# Show network info
echo ""
echo "[OK] Provisioning complete!"
echo ""
echo "VM Info:"
echo "  Hostname: $(hostname)"
echo "  IP:       $(hostname -I | awk '{print $2}')"
echo "  Go:       $(/usr/local/go/bin/go version)"
echo ""