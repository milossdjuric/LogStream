#!/bin/bash

# Script to fix Vagrant and install vagrant-libvirt plugin
# This fixes the Ruby gem dependency conflicts with Ubuntu's packaged Vagrant

set -e

echo "=========================================="
echo "Vagrant + vagrant-libvirt Installation"
echo "=========================================="
echo ""

# Check if running as root for some commands
if [ "$EUID" -eq 0 ]; then 
    echo "Please run this script as a regular user (not root)"
    echo "It will prompt for sudo when needed"
    exit 1
fi

echo "Step 1: Checking current Vagrant installation..."
if command -v vagrant &> /dev/null; then
    echo "  Vagrant found at: $(which vagrant)"
    if vagrant --version 2>/dev/null; then
        echo "  Current version works, checking plugin..."
        if vagrant plugin list 2>/dev/null | grep -q "vagrant-libvirt"; then
            echo "  [OK] vagrant-libvirt plugin already installed!"
            exit 0
        fi
    else
        echo "  [!] Vagrant is installed but not working (Ruby gem conflicts)"
        echo "  Will reinstall from HashiCorp repository"
    fi
else
    echo "  Vagrant not found, will install"
fi

echo ""
echo "Step 2: Installing system dependencies..."
sudo apt-get update
sudo apt-get install -y \
    curl \
    gnupg \
    software-properties-common \
    lsb-release \
    libvirt-dev \
    qemu-system \
    libvirt-daemon-system \
    libvirt-clients \
    bridge-utils \
    build-essential \
    libxslt1-dev \
    libxml2-dev \
    zlib1g-dev \
    ruby-dev

echo ""
echo "Step 3: Adding HashiCorp GPG key and repository..."
# Modern method for Ubuntu 20.04+
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/hashicorp-archive-keyring.gpg
echo "deb [signed-by=/etc/apt/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list

echo ""
echo "Step 5: Updating package list..."
sudo apt-get update

echo ""
echo "Step 6: Installing Vagrant from HashiCorp..."
sudo apt-get install -y vagrant

echo ""
echo "Step 7: Verifying Vagrant installation..."
if vagrant --version; then
    echo "  [OK] Vagrant installed successfully!"
else
    echo "  [X] Vagrant installation failed"
    exit 1
fi

echo ""
echo "Step 8: Installing vagrant-libvirt plugin..."
vagrant plugin install vagrant-libvirt

echo ""
echo "Step 9: Verifying plugin installation..."
if vagrant plugin list | grep -q "vagrant-libvirt"; then
    echo "  [OK] vagrant-libvirt plugin installed successfully!"
else
    echo "  [X] Plugin installation failed"
    exit 1
fi

echo ""
echo "Step 10: Adding user to libvirt and kvm groups..."
sudo usermod -aG libvirt $USER
sudo usermod -aG kvm $USER
echo "  [OK] User added to libvirt and kvm groups"
echo ""
echo "  [!] IMPORTANT: You need to log out and back in (or reboot) for group changes to take effect!"
echo "  After logging back in, verify with: groups | grep libvirt"

echo ""
echo "=========================================="
echo "Installation Complete!"
echo "=========================================="
echo ""
echo "Vagrant version: $(vagrant --version)"
echo "Installed plugins:"
vagrant plugin list
echo ""
echo "Next steps:"
echo "  1. Log out and back in (or reboot) for group changes"
echo "  2. Verify groups: groups | grep libvirt"
echo "  3. Then you can use: ./tests/linux/integration/test-all-vagrant.sh"

