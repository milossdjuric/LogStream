#!/bin/bash
# Rebuild LogStream binary on all Vagrant VMs
# This ensures VMs have the latest code compiled

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "========================================"
echo "Rebuilding LogStream Binary on VMs"
echo "========================================"
echo ""

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

log() { echo -e "${BLUE}->${NC} $1"; }
success() { echo -e "${GREEN}[OK]${NC} $1"; }
error() { echo -e "${RED}[X]${NC} $1"; }

# Step 0: Shut down existing VMs to ensure clean state
log "Step 0: Shutting down existing VMs to ensure clean state..."
VMS_RUNNING=0
for vm in leader broker1 broker2; do
    if vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "^${vm}\s+.*running" >/dev/null 2>&1; then
        VMS_RUNNING=$((VMS_RUNNING + 1))
        log "  Shutting down $vm..."
        vagrant halt $vm 2>&1 | grep -vE "WARNING|Permission denied|libvirt_ip_command" >/dev/null 2>&1 || true
    fi
done

if [ "$VMS_RUNNING" -gt 0 ]; then
    log "  Waiting for VMs to fully shut down..."
    sleep 3
    success "  All running VMs shut down"
else
    log "  No VMs were running"
fi
echo ""

# Step 0.5: Bring VMs back up
log "Step 0.5: Bringing VMs back up..."
for vm in leader broker1 broker2; do
    log "  Starting $vm..."
    if vagrant up $vm 2>&1 | grep -vE "WARNING|Permission denied|libvirt_ip_command" | grep -qE "running|already running"; then
        success "  $vm is running"
    else
        # Check if it's actually running
        sleep 2
        if vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "^${vm}\s+.*running" >/dev/null 2>&1; then
            success "  $vm is running"
        else
            error "  Failed to start $vm"
            exit 1
        fi
    fi
done
log "  Waiting for VMs to be ready..."
sleep 5
echo ""

# Step 1: Sync files to VMs (now that they're running)
log "Step 1: Syncing files to VMs..."
for vm in leader broker1 broker2; do
    if vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "^${vm}\s+.*running" >/dev/null 2>&1; then
        log "  Syncing $vm..."
        if vagrant rsync $vm 2>&1 | grep -vE "WARNING|Permission denied|libvirt_ip_command" >/dev/null 2>&1 || [ $? -eq 0 ]; then
            success "  $vm synced"
        else
            error "  Failed to sync $vm"
        fi
    fi
done
echo ""

# Step 2: Build binary on each VM
log "Step 2: Building logstream binary on VMs..."
BUILD_FAILED=0

for vm in leader broker1 broker2; do
    if vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "^${vm}\s+.*running" >/dev/null 2>&1; then
        log "  Building on $vm..."
        
        # Build the binary (use "." to build entire module, not just main.go)
        BUILD_OUTPUT=$(vagrant ssh $vm -c "cd /vagrant/logstream && /usr/local/go/bin/go build -o logstream . 2>&1" 2>&1 | grep -vE "WARNING|Permission denied|libvirt_ip_command" || true)
        
        if [ -n "$BUILD_OUTPUT" ]; then
            # Check if there are errors
            if echo "$BUILD_OUTPUT" | grep -qiE "error|undefined|cannot|failed"; then
                error "  Build failed on $vm:"
                echo "$BUILD_OUTPUT" | sed 's/^/    /'
                BUILD_FAILED=1
            else
                # Check if binary was created
                if vagrant ssh $vm -c "test -f /vagrant/logstream/logstream && test -x /vagrant/logstream/logstream" 2>/dev/null; then
                    BINARY_SIZE=$(vagrant ssh $vm -c "stat -c%s /vagrant/logstream/logstream 2>/dev/null" 2>/dev/null || echo "0")
                    success "  $vm: Binary built successfully ($BINARY_SIZE bytes)"
                else
                    error "  $vm: Build completed but binary not found or not executable"
                    BUILD_FAILED=1
                fi
            fi
        else
            # No output usually means success, verify binary exists
            if vagrant ssh $vm -c "test -f /vagrant/logstream/logstream && test -x /vagrant/logstream/logstream" 2>/dev/null; then
                BINARY_SIZE=$(vagrant ssh $vm -c "stat -c%s /vagrant/logstream/logstream 2>/dev/null" 2>/dev/null || echo "0")
                success "  $vm: Binary built successfully ($BINARY_SIZE bytes)"
            else
                error "  $vm: Build completed but binary not found or not executable"
                BUILD_FAILED=1
            fi
        fi
    fi
done

echo ""

# Summary
if [ "$BUILD_FAILED" -eq 0 ]; then
    success "All binaries rebuilt successfully!"
    echo ""
    log "Binary locations:"
    for vm in leader broker1 broker2; do
        if vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "^${vm}\s+.*running" >/dev/null 2>&1; then
            BINARY_SIZE=$(vagrant ssh $vm -c "stat -c%s /vagrant/logstream/logstream 2>/dev/null" 2>/dev/null || echo "0")
            echo "  $vm: /vagrant/logstream/logstream ($BINARY_SIZE bytes)"
        fi
    done
    exit 0
else
    error "Some builds failed. Check output above."
    exit 1
fi

