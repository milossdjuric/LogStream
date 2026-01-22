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

# Create logs directory structure
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
LOG_BASE_DIR="$PROJECT_ROOT/test-logs/vagrant-all-${TIMESTAMP}"
mkdir -p "$LOG_BASE_DIR"

# Note: Manual election test removed - automatic election test covers the functionality
# and manual SIGUSR1 triggering has reliability issues in VM environments
TEST_NAMES=("SINGLE" "TRIO" "SEQUENCE" "ELECTION-AUTOMATIC" "PRODUCER-CONSUMER")
TEST_SCRIPTS=(
    "./tests/linux/test-single.sh"
    "./tests/linux/test-trio.sh"
    "./tests/linux/test-sequence.sh"
    "./tests/linux/test-election-automatic.sh"
    "./tests/linux/test-producer-consumer.sh"
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
echo "This will run 5 tests sequentially using Vagrant VMs:"
echo "  1. Single broker"
echo "  2. Trio (3 brokers)"
echo "  3. Sequence Demo"
echo "  4. Election (Automatic failure detection)"
echo "  5. Producer-Consumer"
echo ""
echo "All output will be prefixed with test name."
echo "Logs will be saved to: $LOG_BASE_DIR"
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

# Function to restart VMs between tests (ensures completely clean state)
cleanup_vms_between_tests() {
    local test_name="$1"
    echo "[$test_name] Restarting VMs for next test (ensuring clean state)..."
    
    cd "$PROJECT_ROOT/deploy/vagrant"
    
    # Step 1: Halt all VMs
    echo "[$test_name]   Halting VMs..."
    for vm in leader broker1 broker2; do
        if vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "^${vm}\s+.*running" >/dev/null 2>&1; then
            echo "[$test_name]     Halting $vm..."
            vagrant halt $vm 2>&1 | grep -vE "WARNING|Permission denied|libvirt_ip_command" >/dev/null 2>&1 || true
        fi
    done
    
    # Wait for VMs to fully shut down
    echo "[$test_name]   Waiting for VMs to fully shut down..."
    sleep 3
    
    # Step 2: Bring VMs back up
    echo "[$test_name]   Starting VMs..."
    for vm in leader broker1 broker2; do
        echo "[$test_name]     Starting $vm..."
        vagrant up $vm 2>&1 | grep -vE "WARNING|Permission denied|libvirt_ip_command" >/dev/null 2>&1 || true
        # Verify it's actually running
        sleep 2
        if vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "^${vm}\s+.*running" >/dev/null 2>&1; then
            echo "[$test_name]     [OK] $vm is running"
        else
            echo "[$test_name]     [!] $vm may not be running, checking again..."
            sleep 2
            if vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "^${vm}\s+.*running" >/dev/null 2>&1; then
                echo "[$test_name]     [OK] $vm is running"
            else
                echo "[$test_name]     [X] Failed to start $vm"
                # Don't exit - continue with other VMs
            fi
        fi
    done
    
    # Wait for VMs to be ready
    echo "[$test_name]   Waiting for VMs to be ready..."
    sleep 5
    
    # Step 3: Sync files to ensure latest code
    echo "[$test_name]   Syncing files to VMs..."
    for vm in leader broker1 broker2; do
        if vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "^${vm}\s+.*running" >/dev/null 2>&1; then
            vagrant rsync $vm 2>&1 | grep -vE "WARNING|Permission denied|libvirt_ip_command" >/dev/null 2>&1 || true
        fi
    done
    
    cd "$PROJECT_ROOT"
    echo "[$test_name] [OK] VM restart completed"
}

# Function to run a test sequentially with prefix and log to file
run_test() {
    local test_name="$1"
    local test_script="$2"
    local test_log_dir="$LOG_BASE_DIR/${test_name,,}"
    local test_log_file="$test_log_dir/test.log"
    local exit_code_file="$test_log_dir/exit_code.txt"
    
    # Create test-specific log directory
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
    run_test "${TEST_NAMES[$i]}" "${TEST_SCRIPTS[$i]}"
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
        test_log_dir="$LOG_BASE_DIR/${test_name,,}"
        status=$([ $exit_code -eq 0 ] && echo "PASS" || echo "FAIL")
        echo ""
        echo "Test: $test_name"
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
echo "  - Individual test logs: $LOG_BASE_DIR/{test-name}/test.log"
echo "  - Combined log: $LOG_BASE_DIR/combined.log"
echo "  - Summary: $LOG_BASE_DIR/summary.txt"

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
