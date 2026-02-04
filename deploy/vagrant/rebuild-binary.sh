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

# Step 0: Complete cleanup of VMs (kill processes + force halt)
log "Step 0: Complete cleanup of all VMs..."

# Step 0a: Kill ALL processes first (prevents infinite loops during shutdown)
log "  Step 0a: Killing all logstream processes on all VMs..."
for vm in leader broker1 broker2; do
    if vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "^${vm}\s+.*running" >/dev/null 2>&1; then
        log "    Killing processes on $vm..."
        vagrant ssh $vm -c "pkill -9 -f logstream; pkill -9 -f producer; pkill -9 -f consumer" 2>/dev/null || true
        sleep 1
        
        # Verify cleanup
        remaining=$(vagrant ssh $vm -c "pgrep -f logstream | wc -l" 2>/dev/null || echo "0")
        if [ "$remaining" = "0" ]; then
            success "    All processes killed on $vm"
        else
            log "    WARNING: $remaining process(es) still running on $vm (will be killed by halt)"
        fi
    fi
done

sleep 2

# Step 0b: Force halt all running VMs
VMS_RUNNING=0
for vm in leader broker1 broker2; do
    if vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "^${vm}\s+.*running" >/dev/null 2>&1; then
        VMS_RUNNING=$((VMS_RUNNING + 1))
        log "  Force halting $vm..."
        vagrant halt $vm --force 2>&1 | grep -vE "WARNING|Permission denied|libvirt_ip_command" >/dev/null 2>&1 || true
    fi
done

if [ "$VMS_RUNNING" -gt 0 ]; then
    log "  Waiting for VMs to fully shut down..."
    sleep 5
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

# Step 2: Build all binaries on each VM
log "Step 2: Building binaries on VMs (logstream, producer, consumer)..."
BUILD_FAILED=0

for vm in leader broker1 broker2; do
    if vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "^${vm}\s+.*running" >/dev/null 2>&1; then
        log "  Building on $vm..."
        
        # Build logstream (main broker node)
        log "    Building logstream..."
        BUILD_OUTPUT=$(vagrant ssh $vm -c "cd /vagrant/logstream && /usr/local/go/bin/go build -o logstream . 2>&1" 2>&1 | grep -vE "WARNING|Permission denied|libvirt_ip_command" || true)
        
        if [ -n "$BUILD_OUTPUT" ] && echo "$BUILD_OUTPUT" | grep -qiE "error|undefined|cannot|failed"; then
            error "    logstream build failed on $vm:"
            echo "$BUILD_OUTPUT" | sed 's/^/      /'
            BUILD_FAILED=1
        elif vagrant ssh $vm -c "test -f /vagrant/logstream/logstream && test -x /vagrant/logstream/logstream" 2>/dev/null; then
            BINARY_SIZE=$(vagrant ssh $vm -c "stat -c%s /vagrant/logstream/logstream 2>/dev/null" 2>/dev/null || echo "0")
            success "    logstream: $BINARY_SIZE bytes"
        else
            error "    logstream: binary not found or not executable"
            BUILD_FAILED=1
        fi
        
        # Build producer
        log "    Building producer..."
        BUILD_OUTPUT=$(vagrant ssh $vm -c "cd /vagrant/logstream && /usr/local/go/bin/go build -o producer cmd/producer/main.go 2>&1" 2>&1 | grep -vE "WARNING|Permission denied|libvirt_ip_command" || true)
        
        if [ -n "$BUILD_OUTPUT" ] && echo "$BUILD_OUTPUT" | grep -qiE "error|undefined|cannot|failed"; then
            error "    producer build failed on $vm:"
            echo "$BUILD_OUTPUT" | sed 's/^/      /'
            BUILD_FAILED=1
        elif vagrant ssh $vm -c "test -f /vagrant/logstream/producer && test -x /vagrant/logstream/producer" 2>/dev/null; then
            BINARY_SIZE=$(vagrant ssh $vm -c "stat -c%s /vagrant/logstream/producer 2>/dev/null" 2>/dev/null || echo "0")
            success "    producer: $BINARY_SIZE bytes"
        else
            error "    producer: binary not found or not executable"
            BUILD_FAILED=1
        fi
        
        # Build consumer
        log "    Building consumer..."
        BUILD_OUTPUT=$(vagrant ssh $vm -c "cd /vagrant/logstream && /usr/local/go/bin/go build -o consumer cmd/consumer/main.go 2>&1" 2>&1 | grep -vE "WARNING|Permission denied|libvirt_ip_command" || true)
        
        if [ -n "$BUILD_OUTPUT" ] && echo "$BUILD_OUTPUT" | grep -qiE "error|undefined|cannot|failed"; then
            error "    consumer build failed on $vm:"
            echo "$BUILD_OUTPUT" | sed 's/^/      /'
            BUILD_FAILED=1
        elif vagrant ssh $vm -c "test -f /vagrant/logstream/consumer && test -x /vagrant/logstream/consumer" 2>/dev/null; then
            BINARY_SIZE=$(vagrant ssh $vm -c "stat -c%s /vagrant/logstream/consumer 2>/dev/null" 2>/dev/null || echo "0")
            success "    consumer: $BINARY_SIZE bytes"
        else
            error "    consumer: binary not found or not executable"
            BUILD_FAILED=1
        fi
        
        if [ "$BUILD_FAILED" -eq 0 ]; then
            success "  $vm: All binaries built successfully"
        fi
        echo ""
    fi
done

# Step 3: Verify no stale processes are running
log "Step 3: Verifying clean state (no running processes)..."
STALE_PROCS=0
for vm in leader broker1 broker2; do
    if vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "^${vm}\s+.*running" >/dev/null 2>&1; then
        # Check for any logstream processes
        proc_count=$(vagrant ssh $vm -c "pgrep -f logstream 2>/dev/null | wc -l" 2>/dev/null || echo "0")
        
        if [ "$proc_count" = "0" ]; then
            success "  $vm: Clean (no running processes)"
        else
            log "  WARNING: $vm has $proc_count running process(es)"
            
            # Show what processes are running
            log "  Checking what's running on $vm..."
            vagrant ssh $vm -c "ps aux | grep -E 'logstream|PID' | grep -v grep" 2>/dev/null || true
            
            # Kill more aggressively
            log "  Killing processes on $vm..."
            vagrant ssh $vm -c "
                # Kill by process name
                pkill -9 -f logstream 2>/dev/null || true
                pkill -9 -f producer 2>/dev/null || true
                pkill -9 -f consumer 2>/dev/null || true
                
                # Kill by port
                for port in 8001 8002 8003 8888 9999; do
                    fuser -k \${port}/tcp 2>/dev/null || true
                    fuser -k \${port}/udp 2>/dev/null || true
                done
                
                # Final sweep
                killall -9 logstream 2>/dev/null || true
            " 2>/dev/null || true
            
            sleep 2
            
            # Check again
            proc_count=$(vagrant ssh $vm -c "pgrep -f logstream 2>/dev/null | wc -l" 2>/dev/null || echo "0")
            if [ "$proc_count" = "0" ]; then
                success "  $vm: Cleaned up successfully"
            else
                log "  $vm: Still has $proc_count process(es) after cleanup"
                # Show remaining processes
                vagrant ssh $vm -c "ps aux | grep -E 'logstream|PID' | grep -v grep" 2>/dev/null || true
                
                # Don't fail - just warn (processes will be cleaned by test runner)
                log "  [!] Note: Processes will be cleaned again before tests start"
                # STALE_PROCS=1  # Don't fail here - let tests handle it
            fi
        fi
    fi
done

echo ""

# Summary
if [ "$BUILD_FAILED" -eq 0 ]; then
    success "All binaries rebuilt successfully!"
    
    if [ "$STALE_PROCS" -eq 0 ]; then
        success "All VMs are clean with no stale processes"
    else
        log "Note: Some stale processes detected - test runner will clean them"
    fi
    
    echo ""
    log "Binary locations on each VM:"
    for vm in leader broker1 broker2; do
        if vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "^${vm}\s+.*running" >/dev/null 2>&1; then
            echo "  $vm:"
            LOGSTREAM_SIZE=$(vagrant ssh $vm -c "stat -c%s /vagrant/logstream/logstream 2>/dev/null" 2>/dev/null || echo "0")
            PRODUCER_SIZE=$(vagrant ssh $vm -c "stat -c%s /vagrant/logstream/producer 2>/dev/null" 2>/dev/null || echo "0")
            CONSUMER_SIZE=$(vagrant ssh $vm -c "stat -c%s /vagrant/logstream/consumer 2>/dev/null" 2>/dev/null || echo "0")
            echo "    /vagrant/logstream/logstream ($LOGSTREAM_SIZE bytes)"
            echo "    /vagrant/logstream/producer ($PRODUCER_SIZE bytes)"
            echo "    /vagrant/logstream/consumer ($CONSUMER_SIZE bytes)"
        fi
    done
    echo ""
    success "Ready for testing! Run: cd tests/linux/integration && ./test-all-vagrant.sh"
    echo ""
    log "Note: Test runner will perform full cleanup before starting tests"
    exit 0
else
    error "Some builds failed. Check output above."
    exit 1
fi

