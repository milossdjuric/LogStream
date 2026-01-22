#!/bin/bash

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

log() { echo -e "${BLUE}[>]${NC} $1"; }
success() { echo -e "${GREEN}[OK]${NC} $1"; }
error() { echo -e "${RED}[X]${NC} $1"; }
error_msg() { echo -e "${RED}[!]${NC} $1"; }

# Get project root (3 levels up from tests/linux/lib/)
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

# Network namespace helpers for local testing
# These functions manage netns setup for multicast support

# Check if netns is set up, create if needed
ensure_netns_setup() {
    if ! sudo ip netns list 2>/dev/null | grep -q "logstream-a"; then
        log "Network namespaces not found, setting up..."
        local netns_dir="$PROJECT_ROOT/deploy/netns"
        if [ ! -f "$netns_dir/setup-netns.sh" ]; then
            error "setup-netns.sh not found in $netns_dir"
            return 1
        fi
        if ! sudo "$netns_dir/setup-netns.sh"; then
            error "Failed to setup network namespaces"
            return 1
        fi
        success "Network namespaces created"
    fi
}

# Clean up processes in netns (but keep namespaces)
cleanup_netns_processes() {
    for ns in logstream-a logstream-b logstream-c; do
        if sudo ip netns list 2>/dev/null | grep -q "^$ns"; then
            sudo ip netns pids $ns 2>/dev/null | xargs -r sudo kill -9 2>/dev/null || true
        fi
    done
}

# Kill process in specific namespace by PID
kill_netns_process() {
    local ns=$1
    local pid=$2
    if sudo ip netns list 2>/dev/null | grep -q "^$ns"; then
        sudo ip netns exec $ns kill -9 $pid 2>/dev/null || true
    fi
}

# Get all PIDs in a namespace
get_netns_pids() {
    local ns=$1
    if sudo ip netns list 2>/dev/null | grep -q "^$ns"; then
        sudo ip netns pids $ns 2>/dev/null || echo ""
    else
        echo ""
    fi
}

# Just check if binary exists
build_if_needed() {
    local binary=$1
    # Binaries should be in project root
    if [ ! -f "$PROJECT_ROOT/$binary" ]; then
        error "Binary $binary not found in $PROJECT_ROOT"
        error "Run: ./rebuild.sh from project root"
        exit 1
    fi
}

# Cleanup
cleanup() {
    log "Cleaning up..."
    pkill -f "logstream" 2>/dev/null
    pkill -f "producer" 2>/dev/null
    pkill -f "consumer" 2>/dev/null
    rm -f /tmp/logstream-*.log /tmp/producer-*.pipe 2>/dev/null
}

trap cleanup EXIT

# Global cleanup function for Vagrant VMs - kills ALL logstream processes on ALL VMs
# Usage: global_vagrant_cleanup [wait_time]
# This should be called between tests to ensure clean state
global_vagrant_cleanup() {
    local wait_time="${1:-3}"
    local vagrant_dir="$PROJECT_ROOT/deploy/vagrant"
    local original_dir="$(pwd)"

    log "============================================================"
    log "GLOBAL VAGRANT CLEANUP - Killing all processes on all VMs"
    log "============================================================"

    # Check if vagrant directory exists
    if [ ! -f "$vagrant_dir/Vagrantfile" ]; then
        error_msg "Vagrantfile not found in $vagrant_dir - skipping global cleanup"
        return 1
    fi

    # Change to vagrant directory for vagrant commands
    cd "$vagrant_dir" || {
        error_msg "Could not change to $vagrant_dir"
        return 1
    }

    for vm in leader broker1 broker2; do
        log "Cleaning up $vm..."

        # Use vagrant_ssh_retry from the same directory context
        vagrant ssh "$vm" -c "
            # Kill by process name (with process group)
            pids=\$(pgrep -f 'logstream' 2>/dev/null || true)
            if [ -n \"\$pids\" ]; then
                for pid in \$pids; do
                    # Get process group and kill entire group
                    pgid=\$(ps -o pgid= -p \$pid 2>/dev/null | tr -d ' ' || echo '')
                    [ -n \"\$pgid\" ] && kill -9 -\$pgid 2>/dev/null || true
                    kill -9 \$pid 2>/dev/null || true
                    pkill -9 -P \$pid 2>/dev/null || true
                done
            fi

            # Kill wrapper scripts
            pkill -9 -f 'start-logstream' 2>/dev/null || true

            # Final pkill sweep
            pkill -9 -f 'logstream' 2>/dev/null || true
            pkill -9 -f 'producer' 2>/dev/null || true
            pkill -9 -f 'consumer' 2>/dev/null || true

            # Kill by port (catch any remaining)
            for port in 8001 8002 8003 8888 9999; do
                if command -v fuser >/dev/null 2>&1; then
                    fuser -k \${port}/tcp 2>/dev/null || true
                    fuser -k \${port}/udp 2>/dev/null || true
                elif command -v lsof >/dev/null 2>&1; then
                    lsof -ti:\${port} 2>/dev/null | xargs -r kill -9 2>/dev/null || true
                fi
            done

            # Verify cleanup
            remaining=\$(pgrep -f 'logstream' 2>/dev/null | wc -l || echo '0')
            if [ \"\$remaining\" -gt 0 ]; then
                echo \"WARNING: \$remaining processes still running on \$(hostname)\"
            else
                echo \"Clean: No logstream processes on \$(hostname)\"
            fi
        " 2>/dev/null && log "  [OK] $vm cleaned" || log "  Warning: Could not cleanup $vm (VM may be inaccessible)"
    done

    # Return to original directory
    cd "$original_dir" || true

    log "Waiting ${wait_time}s for processes to fully terminate..."
    sleep "$wait_time"

    log "Global cleanup complete"
    log "============================================================"
}

# Make sure we're in project root when running commands
cd "$PROJECT_ROOT"

# Helper function to check VM status with retries (for parallel test execution)
# Must be called from deploy/vagrant directory
check_vm_status() {
    local vm=$1
    local max_attempts=5
    local attempt=1
    local delay=1
    
    # Ensure we're in the vagrant directory
    local vagrant_dir="$PROJECT_ROOT/deploy/vagrant"
    if [ ! -f "$vagrant_dir/Vagrantfile" ]; then
        return 1
    fi
    
    while [ $attempt -le $max_attempts ]; do
        if (cd "$vagrant_dir" && vagrant status $vm 2>/dev/null | grep -q "running"); then
            return 0
        fi
        if [ $attempt -lt $max_attempts ]; then
            sleep $delay
            delay=$((delay * 2))  # Exponential backoff
        fi
        attempt=$((attempt + 1))
    done
    return 1
}

# Helper function to run vagrant ssh with retries
vagrant_ssh_retry() {
    local vm=$1
    shift
    local cmd="$@"
    local max_attempts=3
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        # Ensure we are in the deploy/vagrant directory for vagrant commands
        if (cd "$PROJECT_ROOT/deploy/vagrant" && vagrant ssh $vm -c "$cmd" 2>/dev/null); then
            return 0
        fi
        if [ $attempt -lt $max_attempts ]; then
            sleep 1
        fi
        attempt=$((attempt + 1))
    done
    return 1
}

# Setup log directory based on LOG_BASE_DIR environment variable
# Usage: setup_log_directory MODE
# Sets: LOG_DIR and VM_LOG_DIR variables
setup_log_directory() {
    local mode="$1"
    # Get script name from caller (the test script that called this function)
    # Use BASH_SOURCE to get the actual script file, not the sourced common.sh
    local script_path="${BASH_SOURCE[1]:-$0}"
    local script_name=$(basename "$script_path" .sh | sed 's/test-//')
    
    if [ -n "$LOG_BASE_DIR" ]; then
        LOG_DIR="$LOG_BASE_DIR/$script_name"
        mkdir -p "$LOG_DIR"
        # For vagrant, use synced folder path
        if [ "$mode" = "vagrant" ]; then
            VM_LOG_DIR="/vagrant/logstream/test-logs/$(basename "$LOG_BASE_DIR")/$script_name"
        else
            VM_LOG_DIR="$LOG_DIR"
        fi
    else
        # Fallback: Create log directory in project root for standalone execution
        TIMESTAMP=$(date +%Y%m%d-%H%M%S)
        LOG_DIR="$PROJECT_ROOT/test-logs/standalone-${script_name}-${TIMESTAMP}"
        mkdir -p "$LOG_DIR"
        
        if [ "$mode" = "vagrant" ]; then
            VM_LOG_DIR="/vagrant/logstream/test-logs/standalone-${script_name}-${TIMESTAMP}"
        else
            VM_LOG_DIR="$LOG_DIR"
        fi
    fi
}

# Verify log file exists and is being written to on VM
# Usage: verify_vm_log_file VM LOG_FILE_PATH
# Returns 0 if log exists and has content, 1 otherwise
verify_vm_log_file() {
    local vm="$1"
    local log_file="$2"
    local max_attempts=5
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        # Check if file exists and has content
        if vagrant_ssh_retry "$vm" "test -f '$log_file' && test -s '$log_file'" 2>/dev/null; then
            # Get file size to verify it's growing
            local size=$(vagrant_ssh_retry "$vm" "stat -c%s '$log_file' 2>/dev/null" 2>/dev/null || echo "0")
            if [ -n "$size" ] && [ "$size" -gt 0 ]; then
                return 0
            fi
        fi
        if [ $attempt -lt $max_attempts ]; then
            sleep 1
        fi
        attempt=$((attempt + 1))
    done
    return 1
}

# Wait for log file to be synced from VM to host
# Usage: wait_for_synced_log HOST_LOG_PATH
# Returns 0 when file exists on host, 1 on timeout
wait_for_synced_log() {
    local host_log="$1"
    local max_attempts=10
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if [ -f "$host_log" ] && [ -s "$host_log" ]; then
            return 0
        fi
        if [ $attempt -lt $max_attempts ]; then
            sleep 1
        fi
        attempt=$((attempt + 1))
    done
    return 1
}

# Ensure log directory exists on VM and is writable
# Usage: ensure_vm_log_directory VM VM_LOG_DIR
ensure_vm_log_directory() {
    local vm="$1"
    local vm_log_dir="$2"
    
    if ! vagrant_ssh_retry "$vm" "mkdir -p '$vm_log_dir' && chmod 777 '$vm_log_dir'" 2>/dev/null; then
        error "Failed to create log directory $vm_log_dir on VM $vm"
        return 1
    fi
    return 0
}

# Create and prepare log file on VM
# Usage: prepare_vm_log_file VM LOG_FILE_PATH
prepare_vm_log_file() {
    local vm="$1"
    local log_file="$2"
    local log_dir=$(dirname "$log_file")
    
    # Ensure directory exists
    if ! ensure_vm_log_directory "$vm" "$log_dir"; then
        return 1
    fi
    
    # Create log file and make it writable
    if ! vagrant_ssh_retry "$vm" "touch '$log_file' && chmod 666 '$log_file'" 2>/dev/null; then
        error "Failed to create log file $log_file on VM $vm"
        return 1
    fi
    
    return 0
}

# Get log file content from VM (for debugging)
# Usage: get_vm_log_content VM LOG_FILE_PATH [LINES]
get_vm_log_content() {
    local vm="$1"
    local log_file="$2"
    local lines="${3:-50}"
    
    vagrant_ssh_retry "$vm" "tail -$lines '$log_file' 2>/dev/null" 2>/dev/null || echo "Log file not accessible"
}

# Check if log file is growing (has new content)
# Usage: check_log_growing VM LOG_FILE_PATH
# Returns 0 if log is growing, 1 otherwise
check_log_growing() {
    local vm="$1"
    local log_file="$2"
    local size1=$(vagrant_ssh_retry "$vm" "stat -c%s '$log_file' 2>/dev/null" 2>/dev/null || echo "0")
    sleep 2
    local size2=$(vagrant_ssh_retry "$vm" "stat -c%s '$log_file' 2>/dev/null" 2>/dev/null || echo "0")
    
    if [ -n "$size1" ] && [ -n "$size2" ] && [ "$size2" -gt "$size1" ]; then
        return 0
    fi
    return 1
}

# Copy process log from VM to host
# Usage: copy_vm_log_to_host VM VM_LOG_FILE HOST_LOG_FILE
# Uses SSH to copy (most reliable method, works even if synced folder has delays)
# Has size limit protection to prevent copying huge files (max 100MB)
copy_vm_log_to_host() {
    local vm="$1"
    local vm_log_file="$2"
    local host_log_file="$3"
    local max_size_mb=100  # Maximum file size to copy (100MB)
    local max_size_bytes=$((max_size_mb * 1024 * 1024))
    
    # Ensure host directory exists
    local host_dir=$(dirname "$host_log_file")
    mkdir -p "$host_dir" 2>/dev/null || true
    
    # First, verify file exists on VM and has content
    if ! vagrant_ssh_retry "$vm" "test -f '$vm_log_file' && test -s '$vm_log_file'" 2>/dev/null; then
        return 1
    fi
    
    # Get file size on VM for verification (sanitize output to remove newlines/whitespace)
    local vm_size_raw=$(vagrant_ssh_retry "$vm" "stat -c%s '$vm_log_file' 2>/dev/null" 2>/dev/null || echo "0")
    local vm_size=$(echo "$vm_size_raw" | tr -d '\n\r ' | grep -E '^[0-9]+$' || echo "0")
    vm_size=$((vm_size + 0))  # Convert to integer
    
    if [ "$vm_size" -eq 0 ]; then
        return 1
    fi
    
    # Check if file is too large - if so, copy only the last portion
    if [ "$vm_size" -gt "$max_size_bytes" ]; then
        log "Warning: Log file is very large (${vm_size} bytes > ${max_size_bytes} bytes)"
        log "Copying only the last ${max_size_mb}MB to prevent excessive disk usage..."
        
        # Copy only the last portion of the file (tail -c)
        local bytes_to_copy=$max_size_bytes
        if vagrant_ssh_retry "$vm" "tail -c $bytes_to_copy '$vm_log_file' 2>/dev/null" 2>/dev/null > "$host_log_file" 2>&1; then
            # Add a header to indicate truncation
            {
                echo "=== WARNING: Log file truncated (original size: ${vm_size} bytes) ==="
                echo "=== Only the last ${max_size_mb}MB are shown ==="
                echo ""
            } > "$host_log_file.tmp" 2>/dev/null
            cat "$host_log_file" >> "$host_log_file.tmp" 2>/dev/null
            mv "$host_log_file.tmp" "$host_log_file" 2>/dev/null || true
            
            if [ -f "$host_log_file" ] && [ -s "$host_log_file" ]; then
                log "Copied truncated log file (${max_size_mb}MB) to: $host_log_file"
                return 0
            fi
        fi
        return 1
    fi
    
    # Copy via SSH (most reliable, works even if synced folder has delays)
    # Use head to limit output in case file grows during copy
    if vagrant_ssh_retry "$vm" "head -c $((vm_size + 10000)) '$vm_log_file' 2>/dev/null" 2>/dev/null > "$host_log_file" 2>&1; then
        # Verify the copied file has content
        if [ -f "$host_log_file" ] && [ -s "$host_log_file" ]; then
            local host_size_raw=$(stat -c%s "$host_log_file" 2>/dev/null || echo "0")
            local host_size=$(echo "$host_size_raw" | tr -d '\n\r ' | grep -E '^[0-9]+$' || echo "0")
            host_size=$((host_size + 0))  # Convert to integer
            
            # Allow some tolerance (file might have grown slightly during copy)
            if [ "$host_size" -gt 0 ] && [ "$host_size" -ge $((vm_size - 1000)) ]; then
                return 0
            fi
        fi
    fi
    
    return 1
}

# Verify binary exists on VM, rebuild only if missing or if FORCE_REBUILD=1
# Usage: ensure_binary_on_vm VM [FORCE_REBUILD]
# If FORCE_REBUILD environment variable is set to 1, will always rebuild
# Otherwise, only checks if binary exists and is executable
ensure_binary_on_vm() {
    local vm="$1"
    local force_rebuild="${FORCE_REBUILD:-0}"
    
    # First check if binary exists and is executable
    if [ "$force_rebuild" != "1" ] && vagrant_ssh_retry "$vm" "cd /vagrant/logstream && test -f ./logstream && test -x ./logstream" 2>/dev/null; then
        local binary_size=$(vagrant_ssh_retry "$vm" "cd /vagrant/logstream && stat -c%s ./logstream 2>/dev/null" 2>/dev/null || echo "0")
        log "[OK] Binary exists on $vm (${binary_size} bytes) - skipping rebuild"
        log "  (Run with FORCE_REBUILD=1 or use ./deploy/vagrant/rebuild-binary.sh to rebuild)"
        return 0
    fi
    
    # Binary doesn't exist or force rebuild requested
    if [ "$force_rebuild" = "1" ]; then
        log "Building/rebuilding logstream binary on $vm (FORCE_REBUILD=1)..."
    else
        log "Binary not found on $vm, building..."
    fi
    
    # Remove old binary first to force a clean build
    vagrant_ssh_retry "$vm" "cd /vagrant/logstream && rm -f ./logstream" 2>/dev/null || true
    
    if ! vagrant_ssh_retry "$vm" "cd /vagrant/logstream && rm -f ./logstream && /usr/local/go/bin/go build -o logstream . 2>&1" 2>/dev/null; then
        error "Failed to build binary on $vm!"
        # Try to get build errors for debugging
        log "Attempting to get build output..."
        vagrant_ssh_retry "$vm" "cd /vagrant/logstream && /usr/local/go/bin/go build -o logstream . 2>&1" 2>&1 | head -20 || true
        return 1
    fi
    
    # Verify binary was created and is executable
    if ! vagrant_ssh_retry "$vm" "cd /vagrant/logstream && test -f ./logstream && test -x ./logstream" 2>/dev/null; then
        error "Binary logstream was not created or is not executable on $vm!"
        return 1
    fi
    
    log "[OK] Binary built successfully on $vm"
    return 0
}

# MODIFIED: Comprehensive process verification with better error handling and patience
# Usage: verify_process_running VM LOG_FILE [PROCESS_NAME] [MAX_WAIT_SECONDS]
# Returns 0 if process is running and producing output, 1 otherwise
verify_process_running() {
    local vm="$1"
    local log_file="$2"
    local process_name="${3:-logstream}"
    local max_wait="${4:-7}"  # Reduced to 7 seconds - nodes should start quickly
    
    log "==========================================================="
    log "COMPREHENSIVE PROCESS VERIFICATION"
    log "==========================================================="
    log "VM: $vm"
    log "Process: $process_name"
    log "Log file: $log_file"
    log "Max wait time: ${max_wait}s"
    log ""
    
    # PRE-FLIGHT CHECKS: Verify environment before process starts
    log "--- PRE-FLIGHT CHECKS ------------------------------------"
    
    # Check 1: Binary exists and is executable
    log "  [1/5] Checking binary existence..."
    if ! vagrant_ssh_retry "$vm" "cd /vagrant/logstream && test -f ./logstream && test -x ./logstream" 2>/dev/null; then
        error "  [X] Binary ./logstream not found or not executable on $vm"
        log "  Checking directory contents:"
        vagrant_ssh_retry "$vm" "cd /vagrant/logstream && ls -la logstream* 2>/dev/null" 2>/dev/null || log "    No logstream files found"
        return 1
    fi
    local binary_size=$(vagrant_ssh_retry "$vm" "cd /vagrant/logstream && stat -c%s ./logstream 2>/dev/null" 2>/dev/null || echo "0")
    log "  [OK] Binary exists and is executable (${binary_size} bytes)"
    
    # Check 2: Log file directory exists and is writable
    log "  [2/5] Checking log directory..."
    local log_dir=$(dirname "$log_file")
    if ! vagrant_ssh_retry "$vm" "test -d '$log_dir' && test -w '$log_dir'" 2>/dev/null; then
        error "  [X] Log directory '$log_dir' does not exist or is not writable"
        log "  Attempting to create directory..."
        if ! ensure_vm_log_directory "$vm" "$log_dir"; then
            error "  [X] Failed to create log directory"
            return 1
        fi
    fi
    log "  [OK] Log directory exists and is writable"
    
    # Check 3: Log file is writable (or can be created)
    log "  [3/5] Checking log file..."
    if ! vagrant_ssh_retry "$vm" "test -f '$log_file' && test -w '$log_file' || touch '$log_file' 2>/dev/null && chmod 666 '$log_file' 2>/dev/null" 2>/dev/null; then
        error "  [X] Log file '$log_file' cannot be created or written to"
        local perms=$(vagrant_ssh_retry "$vm" "ls -la '$log_file' 2>/dev/null" 2>/dev/null || echo "file does not exist")
        log "  File status: $perms"
        return 1
    fi
    log "  [OK] Log file is writable"
    
    # Check 4: Test if binary can actually run (dry run with timeout)
    log "  [4/5] Testing binary execution (dry run)..."
    local test_output=$(vagrant_ssh_retry "$vm" "cd /vagrant/logstream && timeout 2 ./logstream 2>&1 | head -20" 2>/dev/null || echo "")
    if [ -z "$test_output" ]; then
        error_msg "  [!] Binary produced no output in test run (may be normal if it requires env vars)"
    else
        log "  [OK] Binary can execute (test output captured)"
        if echo "$test_output" | grep -qi "error\|fatal\|panic\|cannot\|failed"; then
            error_msg "  [!] Binary test run shows potential errors:"
            echo "$test_output" | head -5 | while IFS= read -r line; do
                log "    $line"
            done
        fi
    fi
    
    # Check 5: Check for existing processes (informational only - cleanup already done)
    # NOTE: Cleanup is now done in start_logstream_vm_wrapper BEFORE starting the process
    log "  [5/5] Checking for existing processes..."
    local existing_pids=$(vagrant_ssh_retry "$vm" "pgrep -f '$process_name' 2>/dev/null" 2>/dev/null || echo "")
    if [ -n "$existing_pids" ]; then
        log "  [!] Found existing $process_name processes: $existing_pids"
        log "  (These should be from the process we just started - if not, there may be a problem)"
    else
        log "  [OK] No existing processes found (process may not have started yet)"
    fi
    
    log "--- Pre-flight checks complete"
    log ""
    
    # MONITORING PHASE: Watch for process startup and health
    log "--- MONITORING PROCESS STARTUP ---------------------------"
    log "  Checking every 2 seconds for up to ${max_wait} seconds..."
    log ""
    
    local check_interval=2
    local checks=$((max_wait / check_interval))
    local process_found=false
    local log_growing=false
    local initial_size=0
    local last_size=0
    local process_pid=""
    local consecutive_empty_checks=0
    local max_empty_checks=5  # INCREASED from 3 to 5 for more patience
    local consecutive_crashes=0
    local max_consecutive_crashes=2  # ADDED: Allow up to 2 crashes before giving up
    
    for i in $(seq 1 $checks); do
        local elapsed=$((i * check_interval))
        
        # Check 1: Is process running?
        local current_pids=$(vagrant_ssh_retry "$vm" "pgrep -f '$process_name' 2>/dev/null" 2>/dev/null || echo "")
        
        if [ -n "$current_pids" ]; then
            if [ "$process_found" = false ]; then
                process_found=true
                process_pid=$(echo "$current_pids" | head -1)
                log "  [${elapsed}s] [OK] Process detected! PID: $process_pid"
                
                # Get detailed process info
                local proc_info=$(vagrant_ssh_retry "$vm" "ps -p $process_pid -o pid,user,cmd --no-headers 2>/dev/null" 2>/dev/null || echo "")
                if [ -n "$proc_info" ]; then
                    log "    Process details: $proc_info"
                fi
                
                # Check process state
                local proc_state=$(vagrant_ssh_retry "$vm" "ps -p $process_pid -o state --no-headers 2>/dev/null" 2>/dev/null || echo "")
                if [ -n "$proc_state" ]; then
                    case "$proc_state" in
                        R) log "    State: Running" ;;
                        S) log "    State: Sleeping (waiting for event)" ;;
                        D) error_msg "    State: Uninterruptible sleep (may indicate I/O wait)" ;;
                        Z) error "    State: Zombie (process has terminated)" ;;
                        T) error_msg "    State: Stopped" ;;
                        *) log "    State: $proc_state" ;;
                    esac
                fi
            else
                # Process still running - verify it's the same PID
                local new_pid=$(echo "$current_pids" | head -1)
                if [ -n "$new_pid" ] && [ "$new_pid" != "$process_pid" ]; then
                    error_msg "  [${elapsed}s] [!] Process PID changed! Old: $process_pid, New: $new_pid"
                    log "    Process appears to have crashed and restarted"
                    
                    # MODIFIED: Give it a chance to stabilize
                    consecutive_crashes=$((consecutive_crashes + 1))
                    if [ $consecutive_crashes -ge $max_consecutive_crashes ]; then
                        error "  [${elapsed}s] Process keeps crashing (${consecutive_crashes} times)!"
                        log "    This indicates a fundamental problem with the process"
                        
                        # Get diagnostic info
                        log "    Checking log file for errors..."
                        get_vm_log_content "$vm" "$log_file" 50 | grep -iE "error|fatal|panic|crash" | tail -10 || log "    No obvious errors in log"
                        return 1
                    else
                        log "    Giving process another chance to stabilize (crash $consecutive_crashes/$max_consecutive_crashes)"
                        process_pid=$new_pid
                        sleep 3  # Give it a bit more time
                        continue
                    fi
                else
                    # PID stable - reset crash counter
                    consecutive_crashes=0
                fi
            fi
        else
            if [ "$process_found" = true ]; then
                # Process was running but now it's gone
                error "  [${elapsed}s] [X] Process disappeared (was PID $process_pid)"
                log "    Checking for crash logs..."
                get_vm_log_content "$vm" "$log_file" 50 | tail -20 | while IFS= read -r line; do
                    log "    $line"
                done
                return 1
            else
                # Process not found yet
                if [ "$i" -lt $checks ]; then
                    log "  [${elapsed}s] Process not found yet, waiting..."
                fi
            fi
        fi
        
        # Check 2: Is log file being written to?
        local current_size_raw=$(vagrant_ssh_retry "$vm" "stat -c%s '$log_file' 2>/dev/null" 2>/dev/null || echo "0")
        # Ensure we have a valid integer
        local current_size=$(echo "$current_size_raw" | tr -d '\n\r ' | grep -E '^[0-9]+$' || echo "0")
        current_size=$((current_size + 0))
        
        if [ "$i" -eq 1 ]; then
            initial_size=$current_size
            last_size=$current_size
            if [ "$initial_size" -gt 0 ]; then
                log "  [${elapsed}s] Initial log size: $initial_size bytes"
            else
                log "  [${elapsed}s] Log file is empty, waiting for output..."
            fi
        else
            last_size=$((last_size + 0))
            
            if [ "$current_size" -gt "$last_size" ]; then
                log_growing=true
                local growth=$((current_size - last_size))
                log "  [${elapsed}s] [OK] Log is GROWING! Size: $current_size bytes (+$growth bytes since last check)"
                consecutive_empty_checks=0
                
                # Success condition: Process is running AND log is growing
                if [ "$process_found" = true ]; then
                    log ""
                    log "--- Process verification SUCCESS!"
                    log ""
                    log "==========================================================="
                    log "[OK] VERIFICATION COMPLETE"
                    log "==========================================================="
                    log "  Process: Running (PID: $process_pid)"
                    log "  Log file: Growing ($current_size bytes)"
                    log "  Status: Healthy and producing output"
                    log "==========================================================="
                    return 0
                fi
                
            elif [ "$current_size" -eq 0 ]; then
                consecutive_empty_checks=$((consecutive_empty_checks + 1))
                if [ "$consecutive_empty_checks" -ge $max_empty_checks ]; then
                    error "  [${elapsed}s] [X] Log file still empty after ${elapsed}s (${consecutive_empty_checks} checks)"
                    if [ "$process_found" = true ]; then
                        error "    Process is running but producing no output!"
                        log "    This may indicate:"
                        log "      - Process is stuck or waiting"
                        log "      - Output is not being redirected correctly"
                        log "      - Process exited immediately after start"
                    fi
                    return 1
                else
                    log "  [${elapsed}s] Log file still empty (check $consecutive_empty_checks/$max_empty_checks)..."
                fi
            elif [ "$current_size" -gt 0 ] && [ "$current_size" -eq "$last_size" ]; then
                log "  [${elapsed}s] Log size: $current_size bytes (stable, waiting for growth...)"
            fi
            last_size=$current_size
        fi
        
        # Wait before next check (except on last iteration)
        if [ "$i" -lt $checks ]; then
            sleep $check_interval
        fi
    done
    
    # Final assessment
    log ""
    log "--- Monitoring complete"
    log ""
    log "==========================================================="
    log "[X] VERIFICATION FAILED"
    log "==========================================================="
    
    if [ "$process_found" = false ]; then
        error "  Process never started after ${max_wait}s"
        log "  Possible causes:"
        log "    - Binary not executable or missing dependencies"
        log "    - Environment variables not set correctly"
        log "    - Port already in use"
        log "    - Configuration error"
    elif [ "$log_growing" = false ]; then
        error "  Process is running but log is NOT growing"
        log "  Final log size: $last_size bytes"
        log "  Process PID: $process_pid"
        
        if [ "$last_size" -eq 0 ]; then
            log "  Log is completely empty - output redirection may be failing"
        else
            log "  Log has content but stopped growing - process may be stuck"
        fi
    fi
    
    log "==========================================================="
    return 1
}

# Start logstream process on VM using wrapper script (helper for test scripts)
# Usage: start_logstream_vm_wrapper VM NODE_ADDRESS IS_LEADER LOG_FILE [SKIP_CLEANUP]
# This function creates a wrapper script to ensure proper environment and output redirection
# If SKIP_CLEANUP is set to "1", skips the cleanup step (useful if global cleanup was just done)
start_logstream_vm_wrapper() {
    local vm="$1"
    local node_address="$2"
    local is_leader="$3"
    local log_file="$4"
    local skip_cleanup="${5:-0}"
    
    # CRITICAL: Clean up any existing processes BEFORE starting new one
    # This prevents killing the process we're about to start
    # Skip if global cleanup was just performed (saves time)
    if [ "$skip_cleanup" != "1" ]; then
        log "Cleaning up any existing logstream processes on $vm before starting new one..."

        # Step 1: Kill by port FIRST (most reliable - directly targets what's holding ports)
        vagrant_ssh_retry "$vm" "
            for port in 8001 8002 8003 8888 9999; do
                if command -v fuser >/dev/null 2>&1; then
                    fuser -k -9 \${port}/tcp 2>/dev/null || true
                    fuser -k -9 \${port}/udp 2>/dev/null || true
                fi
                if command -v lsof >/dev/null 2>&1; then
                    lsof -ti:\${port} 2>/dev/null | xargs -r kill -9 2>/dev/null || true
                fi
            done
        " 2>/dev/null || true

        # Step 2: Kill all logstream processes by name
        vagrant_ssh_retry "$vm" "
            pkill -9 -f 'logstream' 2>/dev/null || true
            pkill -9 -f 'start-logstream' 2>/dev/null || true
        " 2>/dev/null || true

        sleep 1  # Brief wait for processes to die

        # Step 3: Quick verification - if processes remain, one more aggressive attempt
        local remaining=$(vagrant_ssh_retry "$vm" "pgrep -f 'logstream' 2>/dev/null | wc -l" 2>/dev/null || echo "0")
        remaining=$(echo "$remaining" | tr -d '\n\r ' | grep -E '^[0-9]+$' || echo "0")
        remaining=$((remaining + 0))

        if [ "$remaining" -gt 0 ]; then
            log "Processes still running ($remaining), final cleanup attempt..."
            # One more aggressive port-based cleanup
            vagrant_ssh_retry "$vm" "
                for port in 8001 8002 8003 8888 9999; do
                    fuser -k -9 \${port}/tcp 2>/dev/null || true
                    fuser -k -9 \${port}/udp 2>/dev/null || true
                done
                pkill -9 -f 'logstream' 2>/dev/null || true
            " 2>/dev/null || true
            sleep 1
        fi

        # Final check (non-fatal - continue anyway)
        local final_check=$(vagrant_ssh_retry "$vm" "pgrep -f 'logstream' 2>/dev/null | wc -l" 2>/dev/null || echo "0")
        final_check=$(echo "$final_check" | tr -d '\n\r ' | grep -E '^[0-9]+$' || echo "0")
        final_check=$((final_check + 0))

        if [ "$final_check" -gt 0 ]; then
            error_msg "WARNING: $final_check process(es) still running - continuing anyway"
        else
            log "Cleanup complete"
        fi
    else
        log "Skipping cleanup (global cleanup was just performed)"
        # Quick check - kill by port if anything is still running
        local quick_check=$(vagrant_ssh_retry "$vm" "pgrep -f 'logstream' 2>/dev/null | wc -l" 2>/dev/null || echo "0")
        quick_check=$(echo "$quick_check" | tr -d '\n\r ' | grep -E '^[0-9]+$' || echo "0")
        quick_check=$((quick_check + 0))
        if [ "$quick_check" -gt 0 ]; then
            error_msg "WARNING: $quick_check process(es) still running, quick cleanup..."
            vagrant_ssh_retry "$vm" "
                for port in 8001 8002 8003 8888 9999; do
                    fuser -k -9 \${port}/tcp 2>/dev/null || true
                    fuser -k -9 \${port}/udp 2>/dev/null || true
                done
                pkill -9 -f 'logstream' 2>/dev/null || true
            " 2>/dev/null || true
            sleep 1
        fi
    fi
    
    # Ensure log file directory exists and file is writable
    local log_dir=$(dirname "$log_file")
    ensure_vm_log_directory "$vm" "$log_dir" || return 1
    prepare_vm_log_file "$vm" "$log_file" || return 1
    
    # Create a wrapper script on the VM that properly detaches the process
    local script_name="/tmp/start-logstream-$$-$(date +%s).sh"

    # Create the script using printf to avoid quoting issues
    # Use nohup and disown to ensure process survives script exit
    # NOTE: We intentionally do NOT use setsid - it makes processes hard to cleanup
    if ! vagrant_ssh_retry "$vm" "
        printf '%s\n' '#!/bin/bash' > '$script_name' && \
        printf '%s\n' 'set +H' >> '$script_name' && \
        printf '%s\n' 'cd /vagrant/logstream' >> '$script_name' && \
        printf '%s\n' 'export NODE_ADDRESS=\"$node_address\"' >> '$script_name' && \
        printf '%s\n' 'export IS_LEADER=\"$is_leader\"' >> '$script_name' && \
        printf '%s\n' 'export MULTICAST_GROUP=\"239.0.0.1:9999\"' >> '$script_name' && \
        printf '%s\n' 'export BROADCAST_PORT=\"8888\"' >> '$script_name' && \
        printf '%s\n' 'trap \"\" HUP INT TERM' >> '$script_name' && \
        printf '%s\n' 'nohup stdbuf -oL -eL ./logstream > \"$log_file\" 2>&1 < /dev/null &' >> '$script_name' && \
        printf '%s\n' 'PID=\$!' >> '$script_name' && \
        printf '%s\n' 'disown -h \$PID' >> '$script_name' && \
        printf '%s\n' 'sleep 2' >> '$script_name' && \
        printf '%s\n' 'if kill -0 \$PID 2>/dev/null; then' >> '$script_name' && \
        printf '%s\n' '  echo \"Process started successfully with PID \$PID\"' >> '$script_name' && \
        printf '%s\n' '  exit 0' >> '$script_name' && \
        printf '%s\n' 'else' >> '$script_name' && \
        printf '%s\n' '  echo \"ERROR: Process failed to start or crashed immediately\"' >> '$script_name' && \
        printf '%s\n' '  tail -20 \"$log_file\" 2>/dev/null || echo \"Log file empty or not readable\"' >> '$script_name' && \
        printf '%s\n' '  exit 1' >> '$script_name' && \
        printf '%s\n' 'fi' >> '$script_name' && \
        chmod +x '$script_name'
    " 2>/dev/null; then
        return 1
    fi
    
    # Execute the script and capture output
    local script_output=$(vagrant_ssh_retry "$vm" "bash '$script_name' 2>&1" 2>/dev/null)
    local script_exit=$?
    
    if [ $script_exit -ne 0 ]; then
        error "Script execution failed (exit code $script_exit): $script_output"
        vagrant_ssh_retry "$vm" "rm -f '$script_name'" 2>/dev/null || true
        return 1
    fi
    
    # Log the script output for debugging
    if [ -n "$script_output" ]; then
        log "Startup script output: $script_output"
    fi
    
    # Clean up the script after a delay
    (sleep 5 && vagrant_ssh_retry "$vm" "rm -f '$script_name'" 2>/dev/null &) &
    
    return 0
}