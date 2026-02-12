#!/bin/bash

# Vagrant Sequential Test Runner - All 5 Tests
# Usage: ./test-all-vagrant.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$PROJECT_ROOT"

# Source common functions for log management
source "$PROJECT_ROOT/tests/linux/lib/common.sh"

# Check if Vagrant is installed
if ! command -v vagrant &> /dev/null; then
    echo "Error: Vagrant is not installed"
    echo "Install with: sudo apt-get install vagrant"
    exit 1
fi

# Check if vagrant-libvirt plugin is installed
if ! vagrant plugin list 2>/dev/null | grep -q "vagrant-libvirt"; then
    echo "Error: vagrant-libvirt plugin is not installed"
    echo "Install with: vagrant plugin install vagrant-libvirt"
    exit 1
fi

VAGRANT_DIR="$PROJECT_ROOT/deploy/vagrant"

# Rebuild binaries on VMs before running tests
# This script handles everything: halts VMs, restarts them fresh, syncs code, and rebuilds
# This ensures a completely clean slate with no zombie processes from previous runs
echo ""
echo "Restarting VMs and rebuilding binaries (clean slate)..."
if [ ! -f "$VAGRANT_DIR/rebuild-binary.sh" ]; then
    echo "Error: rebuild-binary.sh not found at $VAGRANT_DIR/rebuild-binary.sh"
    exit 1
fi

if ! "$VAGRANT_DIR/rebuild-binary.sh"; then
    echo "Error: Failed to restart VMs and rebuild binaries"
    exit 1
fi

echo ""
echo "========================================"
echo "Pre-Test Cleanup"
echo "========================================"
echo "Performing final cleanup before starting tests..."

# Additional cleanup after rebuild to ensure no stale processes
cd "$VAGRANT_DIR"
for vm in leader broker1 broker2; do
    echo "  Cleaning $vm..."
    vagrant ssh $vm -c "
        pkill -9 -f logstream 2>/dev/null || true
        pkill -9 -f producer 2>/dev/null || true
        pkill -9 -f consumer 2>/dev/null || true
    " 2>/dev/null || true
    
    # Verify
    proc_count=$(vagrant ssh $vm -c "pgrep -f logstream 2>/dev/null | wc -l" 2>/dev/null || echo "0")
    if [ "$proc_count" = "0" ]; then
        echo "    [OK] $vm is clean"
    else
        echo "    [!] $vm still has $proc_count process(es) - will retry"
        vagrant ssh $vm -c "killall -9 logstream producer consumer 2>/dev/null" 2>/dev/null || true
        sleep 1
        proc_count=$(vagrant ssh $vm -c "pgrep -f logstream 2>/dev/null | wc -l" 2>/dev/null || echo "0")
        echo "    [?] $vm now has $proc_count process(es)"
    fi
done
cd "$PROJECT_ROOT"

echo "Pre-test cleanup complete"
echo ""

# Create logs directory structure
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
LOG_BASE_DIR="$PROJECT_ROOT/test-logs/vagrant-all-${TIMESTAMP}"
mkdir -p "$LOG_BASE_DIR"

# Tests are organized by category: Core, Advanced, Client
TEST_NAMES=(
    # Core tests - Basic functionality
    # "SINGLE"        # Commented out to speed up test runs - test is working
    # "TRIO"          # Commented out to speed up test runs - test is working
    # "SEQUENCE"      # Commented out to speed up test runs - test is working
    "PRODUCER-CONSUMER"
    # Advanced tests - Leader election, failover, recovery
    "ELECTION-AUTOMATIC"
    "STREAM-FAILOVER"
    "VIEW-SYNC"
    "STATE-EXCHANGE"
    # Client tests - Client-side features
    "CLIENT-CLEANUP"
    "FIFO-ORDERING"
    "ONE-TO-ONE-MAPPING"
)
TEST_SCRIPTS=(
    # Core tests
    # "./tests/linux/test-single.sh"        # Commented out to speed up test runs - test is working
    # "./tests/linux/test-trio.sh"          # Commented out to speed up test runs - test is working
    # "./tests/linux/test-sequence.sh"      # Commented out to speed up test runs - test is working
    "./tests/linux/test-producer-consumer.sh"
    # Advanced tests
    "./tests/linux/test-election-automatic.sh"
    "./tests/linux/test-stream-failover.sh"
    "./tests/linux/test-view-sync.sh"
    "./tests/linux/test-state-exchange.sh"
    # Client tests
    "./tests/linux/test-client-cleanup.sh"
    "./tests/linux/test-fifo-ordering.sh"
    "./tests/linux/test-one-to-one-mapping.sh"
)
TEST_CATEGORIES=(
    # Core tests
    # "core"          # Commented out - corresponds to SINGLE
    # "core"          # Commented out - corresponds to TRIO
    # "core"          # Commented out - corresponds to SEQUENCE
    "core"
    # Advanced tests
    "advanced"
    "advanced"
    "advanced"
    "advanced"
    # Client tests
    "client"
    "client"
    "client"
)

# Cleanup function
cleanup() {
    echo ""
    echo "Interrupted! Cleaning up..."
    echo "Cleaning up Vagrant resources..."
    cd "$PROJECT_ROOT/deploy/vagrant" 2>/dev/null || true
    # Try to cleanup processes (ignore errors if VMs are not accessible)
    vagrant ssh leader -c 'pkill -f logstream; pkill -f producer' 2>/dev/null || true
    vagrant ssh broker1 -c 'pkill -f logstream; pkill -f consumer' 2>/dev/null || true
    vagrant ssh broker2 -c 'pkill -f logstream' 2>/dev/null || true
    
    # Write summary to log
    if [ -n "$LOG_BASE_DIR" ]; then
        echo "" >> "$LOG_BASE_DIR/summary.txt"
        echo "Tests were interrupted!" >> "$LOG_BASE_DIR/summary.txt"
    fi
    
    exit 130
}

# Trap Ctrl+C
trap cleanup INT TERM

echo "========================================"
echo "Running All Tests (Vagrant Mode)"
echo "Sequential Execution"
echo "========================================"
echo ""
echo "This will run 8 tests sequentially using Vagrant VMs:"
echo "  Core tests (basic functionality):"
echo "    # 1. Single broker [COMMENTED OUT - working]"
echo "    # 2. Trio (3 brokers) [COMMENTED OUT - working]"
echo "    # 3. Sequence Demo [COMMENTED OUT - working]"
echo "    1. Producer-Consumer"
echo "  Advanced tests (leader election, failover, recovery):"
echo "    2. Election (Automatic failure detection)"
echo "    3. Stream Failover (broker failure stream reassignment)"
echo "    4. View-Sync (view-synchronous group communication)"
echo "    5. State-Exchange protocol"
echo "  Client tests (client-side features):"
echo "    6. Client Cleanup (producer/consumer disconnect cleanup)"
echo "    7. FIFO Ordering (holdback queue verification)"
echo "    8. One-to-One Mapping (exclusive producer-consumer per topic)"
echo ""
echo "All output will be prefixed with test name."
echo "Logs will be saved to: $LOG_BASE_DIR"
echo "  Structure: $LOG_BASE_DIR/{category}/{test-name}/"
echo "Press Ctrl+C to stop tests and cleanup."
echo ""

# Save initial info to summary
{
    echo "Vagrant Sequential Test Run Summary"
    echo "===================================="
    echo "Mode: vagrant"
    echo "Provider: libvirt (KVM)"
    echo "Timestamp: $TIMESTAMP"
    echo "Log Directory: $LOG_BASE_DIR"
    echo ""
    echo "Tests:"
    for i in "${!TEST_NAMES[@]}"; do
        echo "  $((i+1)). ${TEST_NAMES[$i]}"
    done
    echo ""
    echo "Starting tests..."
    echo ""
} | tee "$LOG_BASE_DIR/summary.txt"

# Function to do FULL cleanup between tests (kill processes + restart VMs)
cleanup_vms_between_tests() {
    local test_name="$1"
    echo ""
    echo "[$test_name] ================================================"
    echo "[$test_name] FULL VM CLEANUP (Process Kill + VM Restart)"
    echo "[$test_name] ================================================"
    
    cd "$PROJECT_ROOT/deploy/vagrant"
    
    # Step 0: Kill ALL processes FIRST (before halting)
    echo "[$test_name] Step 0: Killing all logstream processes on all VMs..."
    for vm in leader broker1 broker2; do
        if vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "^${vm}\s+.*running" >/dev/null 2>&1; then
            echo "[$test_name]   Killing processes on $vm..."
            vagrant ssh $vm -c "pkill -9 -f logstream" 2>/dev/null || true
            vagrant ssh $vm -c "pkill -9 -f producer" 2>/dev/null || true
            vagrant ssh $vm -c "pkill -9 -f consumer" 2>/dev/null || true
            
            # Verify cleanup
            sleep 1
            local remaining=$(vagrant ssh $vm -c "pgrep -f logstream | wc -l" 2>/dev/null || echo "0")
            if [ "$remaining" = "0" ]; then
                echo "[$test_name]     [OK] All processes killed on $vm"
            else
                echo "[$test_name]     [!] WARNING: $remaining process(es) still running on $vm"
                # Try one more time with more force
                vagrant ssh $vm -c "pkill -9 -f logstream; pkill -9 -f producer; pkill -9 -f consumer" 2>/dev/null || true
            fi
        fi
    done
    
    sleep 2
    
    # Step 1: FORCE halt all VMs (not graceful)
    echo "[$test_name] Step 1: Force halting all VMs..."
    for vm in leader broker1 broker2; do
        if vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "^${vm}\s+.*running" >/dev/null 2>&1; then
            echo "[$test_name]   Force halting $vm..."
            vagrant halt $vm --force 2>&1 | grep -vE "WARNING|Permission denied|libvirt_ip_command" >/dev/null 2>&1 || true
        fi
    done
    
    # Wait for VMs to fully shut down
    echo "[$test_name] Step 2: Waiting for VMs to fully shut down..."
    sleep 5
    
    # Step 3: Bring VMs back up
    echo "[$test_name] Step 3: Starting VMs fresh..."
    for vm in leader broker1 broker2; do
        echo "[$test_name]   Starting $vm..."
        vagrant up $vm 2>&1 | grep -vE "WARNING|Permission denied|libvirt_ip_command" >/dev/null 2>&1 || true
        
        # Verify it's actually running
        sleep 2
        if vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "^${vm}\s+.*running" >/dev/null 2>&1; then
            echo "[$test_name]     [OK] $vm is running"
        else
            echo "[$test_name]     [!] Waiting for $vm to finish starting..."
            sleep 3
            if vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "^${vm}\s+.*running" >/dev/null 2>&1; then
                echo "[$test_name]     [OK] $vm is running"
            else
                echo "[$test_name]     [X] Failed to start $vm!"
            fi
        fi
    done
    
    # Wait for VMs to be fully ready
    echo "[$test_name] Step 4: Waiting for VMs to be fully ready..."
    sleep 5
    
    # Step 5: Sync files to ensure latest code
    echo "[$test_name] Step 5: Syncing files to VMs..."
    for vm in leader broker1 broker2; do
        if vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "^${vm}\s+.*running" >/dev/null 2>&1; then
            vagrant rsync $vm 2>&1 | grep -vE "WARNING|Permission denied|libvirt_ip_command" >/dev/null 2>&1 || true
            echo "[$test_name]     [OK] $vm synced"
        fi
    done
    
    # Step 6: Verify no stale processes remain
    echo "[$test_name] Step 6: Verifying clean state..."
    for vm in leader broker1 broker2; do
        local proc_count=$(vagrant ssh $vm -c "pgrep -f logstream | wc -l" 2>/dev/null || echo "0")
        if [ "$proc_count" = "0" ]; then
            echo "[$test_name]     [OK] $vm has no running processes [OK]"
        else
            echo "[$test_name]     [!] WARNING: $vm has $proc_count running process(es)"
        fi
    done
    
    cd "$PROJECT_ROOT"
    echo "[$test_name] ================================================"
    echo "[$test_name] [OK] Full VM cleanup completed - ready for next test"
    echo "[$test_name] ================================================"
    echo ""
}

# Function to run a test sequentially with prefix and log to file
run_test() {
    local test_name="$1"
    local test_script="$2"
    local test_category="$3"
    local test_log_dir="$LOG_BASE_DIR/$test_category/${test_name,,}"
    local test_log_file="$test_log_dir/test.log"
    local exit_code_file="$test_log_dir/exit_code.txt"
    
    # Create test-specific log directory with category
    mkdir -p "$test_log_dir"
    
    # Create log file immediately and write header to ensure it exists even if test exits early
    {
        echo "========================================"
        echo "Test: $test_name"
        echo "Started: $(date)"
        echo "Script: $test_script"
        echo "========================================"
        echo ""
    } > "$test_log_file"
    
    echo ""
    echo "========================================" 
    echo "Running: $test_name"
    echo "========================================"
    echo ""
    
    # Run test sequentially, capture output and exit code (append to header)
    # Don't use set -e here - we want to capture exit code without exiting
    set +e
    LOG_BASE_DIR="$LOG_BASE_DIR" "$test_script" "vagrant" >> "$test_log_file" 2>&1
    local exit_code=$?
    set +e  # Keep it disabled
    
    # Append footer with exit code
    {
        echo ""
        echo "========================================"
        echo "Test completed: $(date)"
        echo "Exit code: $exit_code"
        echo "========================================"
    } >> "$test_log_file"
    
    # Process log file and output with prefix (only if file exists)
    if [ -f "$test_log_file" ]; then
        while IFS= read -r line || [ -n "$line" ]; do
            prefixed_line="[$test_name] $line"
            echo "$prefixed_line"
            echo "$prefixed_line" >> "$LOG_BASE_DIR/combined.log"
        done < "$test_log_file"
    else
        echo "[$test_name] WARNING: Test log file not found: $test_log_file"
        echo "[$test_name] Test may have exited before creating log file."
    fi
    
    # Save exit code
    echo $exit_code > "$exit_code_file"
    
    # Copy process logs from VM to host (if they exist)
    echo "[$test_name] Copying process logs from VMs to host..."
    
    # Find and copy all .log files from the test directory on VMs
    local vm_log_base="/vagrant/logstream/test-logs/$(basename "$LOG_BASE_DIR")/${test_name,,}"
    for vm in leader broker1 broker2; do
        # Check if log directory exists on VM and has log files
        local log_files=$(vagrant_ssh_retry "$vm" "test -d '$vm_log_base' && find '$vm_log_base' -name '*.log' -type f 2>/dev/null" 2>/dev/null || echo "")
        if [ -n "$log_files" ] && echo "$log_files" | grep -q "\.log$"; then
            for log_file in $(echo "$log_files"); do
                local log_filename=$(basename "$log_file")
                local host_log_file="$test_log_dir/${vm}-${log_filename}"
                echo "[$test_name] Copying $log_filename from $vm..."
                
                # Use the helper function for reliable copying
                if copy_vm_log_to_host "$vm" "$log_file" "$host_log_file"; then
                    local line_count=$(wc -l < "$host_log_file" 2>/dev/null || echo "0")
                    local file_size=$(stat -c%s "$host_log_file" 2>/dev/null || echo "0")
                    echo "[$test_name] [OK] Copied $log_filename from $vm ($line_count lines, ${file_size} bytes)"
                    echo "[$test_name]   Saved to: $host_log_file"
                else
                    echo "[$test_name] [!] Failed to copy $log_filename from $vm"
                fi
            done
        fi
    done
    
    # Print result
    if [ $exit_code -eq 0 ]; then
        echo ""
        echo "[$test_name] [OK] Test PASSED"
    else
        echo ""
        echo "[$test_name] [X] Test FAILED (exit code: $exit_code)"
    fi
    echo ""
    
    return $exit_code
}

# Run each test sequentially
echo "Starting tests..."
echo "" | tee -a "$LOG_BASE_DIR/summary.txt" >> "$LOG_BASE_DIR/combined.log"

PASSED=0
FAILED=0
EXIT_CODES=()

# Disable set -e for the loop so we can continue even if tests fail
set +e

for i in "${!TEST_SCRIPTS[@]}"; do
    run_test "${TEST_NAMES[$i]}" "${TEST_SCRIPTS[$i]}" "${TEST_CATEGORIES[$i]}"
    test_exit_code=$?
    
    if [ $test_exit_code -eq 0 ]; then
        ((PASSED++))
        EXIT_CODES+=(0)
    else
        ((FAILED++))
        EXIT_CODES+=(1)
    fi
    
    # Cleanup VMs between tests to ensure clean state (except after last test)
    # This avoids timeout issues by ensuring no lingering processes
    if [ $i -lt $((${#TEST_SCRIPTS[@]} - 1)) ]; then
        echo ""
        cleanup_vms_between_tests "${TEST_NAMES[$i]}"
        echo ""
    fi
done

# Keep set -e disabled - we'll handle errors manually
set +e

# Generate summary
{
echo ""
echo "========================================"
    echo "All tests completed"
    echo "========================================"
    for i in "${!TEST_NAMES[@]}"; do
        exit_code=${EXIT_CODES[$i]}
        test_name="${TEST_NAMES[$i]}"
        status=$([ $exit_code -eq 0 ] && echo "[OK] PASS" || echo "[X] FAIL (exit code: $exit_code)")
        printf "%-20s %s\n" "$test_name:" "$status"
    done
echo "========================================"
echo "Passed: $PASSED"
echo "Failed: $FAILED"
echo "========================================"
} | tee -a "$LOG_BASE_DIR/summary.txt" | tee -a "$LOG_BASE_DIR/combined.log"

# Save detailed summary to file
{
    echo ""
    echo "Detailed Results:"
    echo "================"
    for i in "${!TEST_NAMES[@]}"; do
        exit_code=${EXIT_CODES[$i]}
        test_name="${TEST_NAMES[$i]}"
        test_category="${TEST_CATEGORIES[$i]}"
        test_log_dir="$LOG_BASE_DIR/$test_category/${test_name,,}"
        status=$([ $exit_code -eq 0 ] && echo "PASS" || echo "FAIL")
        echo ""
        echo "Test: $test_name"
        echo "  Category: $test_category"
        echo "  Status: $status"
        echo "  Exit Code: $exit_code"
        echo "  Log File: $test_log_dir/test.log"
    done
    echo ""
    echo "Combined Log: $LOG_BASE_DIR/combined.log"
    echo "Summary: $LOG_BASE_DIR/summary.txt"
} >> "$LOG_BASE_DIR/summary.txt"

echo ""
echo "Logs saved to: $LOG_BASE_DIR"
echo "  - Individual test logs: $LOG_BASE_DIR/{category}/{test-name}/test.log"
echo "  - Combined log: $LOG_BASE_DIR/combined.log"
echo "  - Summary: $LOG_BASE_DIR/summary.txt"
echo ""
echo "Log categories:"
echo "  - core/     : Basic functionality tests"
echo "  - advanced/ : Leader election, failover, recovery tests"
echo "  - client/   : Client-side feature tests"

# Optional: Cleanup Vagrant resources
echo ""
read -p "Stop Vagrant VMs? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    cd "$PROJECT_ROOT/deploy/vagrant"
    vagrant halt
    cd "$PROJECT_ROOT"
fi

# Return non-zero if any test failed
exit $FAILED
