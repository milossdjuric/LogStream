#!/bin/bash

# Ensure Vagrant VMs are running
# This script checks if VMs are running and starts them if needed
# Usage: ./ensure-vms-running.sh

# Don't use set -e because vagrant commands may return non-zero on warnings

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check if Vagrant is installed
if ! command -v vagrant &> /dev/null; then
    echo "Error: Vagrant is not installed"
    echo "Install with: sudo apt-get install vagrant"
    exit 1
fi

# Check if vagrant-libvirt plugin is installed
if ! vagrant plugin list | grep -q "vagrant-libvirt"; then
    echo "Error: vagrant-libvirt plugin is not installed"
    echo "Install with: vagrant plugin install vagrant-libvirt"
    exit 1
fi

echo "Checking Vagrant VMs status..."

# Check each VM individually (more reliable than counting)
# Match "running" only in the status line (format: "vmname    running (libvirt)")
# Not in error messages like "The Libvirt domain is not running"
VMS_RUNNING=0
for vm in leader broker1 broker2; do
    if vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "^${vm}\s+.*running" >/dev/null 2>&1; then
        ((VMS_RUNNING++))
    fi
done

if [ "$VMS_RUNNING" -lt 3 ]; then
    echo "Starting Vagrant VMs (this may take 5-10 minutes)..."
    echo "Note: First boot takes longer as VMs need to boot and provision"
    
    # Start VMs - redirect stderr to filter permission warnings
    if ! vagrant up --provider=libvirt 2>&1 | grep -vE "Permission denied|libvirt_ip_command"; then
        echo "Warning: vagrant up may have encountered issues"
    fi
    
    # Wait for VMs to fully boot and provision
    echo "Waiting for VMs to boot and provision (this may take 5-10 minutes)..."
    sleep 30
    
    # Check again with retries
    MAX_RETRIES=10
    RETRY_COUNT=0
    while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
        VMS_RUNNING=0
        for vm in leader broker1 broker2; do
            if vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "^${vm}\s+.*running" >/dev/null 2>&1; then
                ((VMS_RUNNING++))
            fi
        done
        
        if [ "$VMS_RUNNING" -eq 3 ]; then
            break
        fi
        
        RETRY_COUNT=$((RETRY_COUNT + 1))
        if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
            echo "Waiting for VMs to start... ($RETRY_COUNT/$MAX_RETRIES)"
            sleep 10
        fi
    done
    
    # Final check
    if [ "$VMS_RUNNING" -lt 3 ]; then
        echo "Warning: Only $VMS_RUNNING/3 VMs appear to be running"
        echo "Checking individual VM status..."
        for vm in leader broker1 broker2; do
            vm_status=$(vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "running|shutoff|not created" | head -1 || echo "unknown")
            echo "  $vm: $vm_status"
        done
        
        if [ "$VMS_RUNNING" -eq 0 ]; then
            echo "Error: No VMs are running. Please check:"
            echo "  1. Are you in the libvirt group? (run: groups | grep libvirt)"
            echo "  2. If not, log out and back in, or run: newgrp libvirt"
            echo "  3. Check libvirt service: systemctl status libvirtd"
            echo "  4. Check VM logs: journalctl -u libvirtd"
            exit 1
        fi
        
        echo "Warning: Not all VMs are running, but continuing with available VMs..."
    else
        echo "[OK] All Vagrant VMs are running"
    fi
else
    echo "[OK] All Vagrant VMs are already running"
fi

# Verify SSH connectivity with retries
echo "Verifying SSH connectivity..."
SSH_OK=0
MAX_SSH_RETRIES=5
SSH_RETRY_COUNT=0

while [ $SSH_RETRY_COUNT -lt $MAX_SSH_RETRIES ]; do
    SSH_OK=0
    for vm in leader broker1 broker2; do
        if vagrant ssh $vm -c 'echo "SSH OK"' 2>&1 | grep -v "Permission denied" | grep -q "SSH OK"; then
            ((SSH_OK++))
        fi
    done
    
    if [ "$SSH_OK" -eq 3 ]; then
        echo "[OK] SSH connectivity verified for all VMs"
        exit 0
    fi
    
    SSH_RETRY_COUNT=$((SSH_RETRY_COUNT + 1))
    if [ $SSH_RETRY_COUNT -lt $MAX_SSH_RETRIES ]; then
        echo "Waiting for SSH connectivity... ($SSH_OK/3 VMs reachable, attempt $SSH_RETRY_COUNT/$MAX_SSH_RETRIES)"
        sleep 10
    fi
done

# Final check - if still not all VMs are reachable, report error
if [ "$SSH_OK" -lt 3 ]; then
    echo "Error: SSH connectivity issues detected ($SSH_OK/3 VMs reachable after $MAX_SSH_RETRIES attempts)"
    echo "VMs may not be fully booted or there may be network issues."
    echo ""
    echo "Checking VM status:"
    for vm in leader broker1 broker2; do
        vm_status=$(vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "running|shutoff|not created" | head -1 || echo "unknown")
        echo "  $vm: $vm_status"
    done
    echo ""
    echo "Please check:"
    echo "  1. VMs are actually running: cd deploy/vagrant && vagrant status"
    echo "  2. Try manually: vagrant ssh leader"
    echo "  3. Check libvirt: systemctl status libvirtd"
    exit 1
fi

exit 0

