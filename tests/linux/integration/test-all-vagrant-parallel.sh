#!/bin/bash

# =============================================================================
# Parallel Vagrant Test Runner - True Parallel Execution with Dedicated VMs
# =============================================================================
# Runs multiple Vagrant tests in parallel batches using dedicated VM sets.
# Each test in a batch gets its own isolated set of 3 VMs (leader, broker1, broker2).
#
# VM Slots:
#   - Slot 1: deploy/vagrant-parallel/slot1 (192.168.101.x)
#   - Slot 2: deploy/vagrant-parallel/slot2 (192.168.102.x)
#   - Slot 3: deploy/vagrant-parallel/slot3 (192.168.103.x)
#
# Logging:
#   - Individual test logs: $LOG_BASE_DIR/{category}/{test-name}/test.log
#   - Node logs copied from VMs: $LOG_BASE_DIR/{category}/{test-name}/{vm}-*.log
#   - Combined log: $LOG_BASE_DIR/combined.log
#   - Summary: $LOG_BASE_DIR/summary.txt
#
# Usage: ./test-all-vagrant-parallel.sh
#
# Requirements:
# - Vagrant with vagrant-libvirt plugin
# - Sufficient RAM for 9 concurrent VMs (~12GB+ recommended)
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$PROJECT_ROOT"

# Source common functions
source "$PROJECT_ROOT/tests/linux/lib/common.sh"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

log() { echo -e "${BLUE}[PARALLEL-RUNNER]${NC} $1"; }
success() { echo -e "${GREEN}[PARALLEL-RUNNER] [OK]${NC} $1"; }
error() { echo -e "${RED}[PARALLEL-RUNNER] [X]${NC} $1"; }
warn() { echo -e "${YELLOW}[PARALLEL-RUNNER] !${NC} $1"; }

# VM Slot directories
SLOT_DIRS=(
    "$PROJECT_ROOT/deploy/vagrant-parallel/slot1"
    "$PROJECT_ROOT/deploy/vagrant-parallel/slot2"
    "$PROJECT_ROOT/deploy/vagrant-parallel/slot3"
)

# Create log directory
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
LOG_BASE_DIR="$PROJECT_ROOT/test-logs/vagrant-parallel-${TIMESTAMP}"
mkdir -p "$LOG_BASE_DIR"

# Initialize combined log and summary
touch "$LOG_BASE_DIR/combined.log"
{
    echo "Vagrant Parallel Test Run Summary"
    echo "=================================="
    echo "Mode: vagrant (parallel)"
    echo "Provider: libvirt (KVM)"
    echo "Timestamp: $TIMESTAMP"
    echo "Log Directory: $LOG_BASE_DIR"
    echo ""
} > "$LOG_BASE_DIR/summary.txt"

log "============================================="
log "PARALLEL VAGRANT TEST RUNNER (True Parallel)"
log "============================================="
log "Log directory: $LOG_BASE_DIR"
log ""

# =============================================================================
# PREREQUISITE CHECKS
# =============================================================================
log "Checking prerequisites..."

if ! command -v vagrant &> /dev/null; then
    error "Vagrant is not installed"
    exit 1
fi
success "Vagrant found"

if ! vagrant plugin list 2>/dev/null | grep -q "vagrant-libvirt"; then
    error "vagrant-libvirt plugin is not installed"
    exit 1
fi
success "vagrant-libvirt plugin found"

# Verify slot directories exist
for slot_dir in "${SLOT_DIRS[@]}"; do
    if [ ! -f "$slot_dir/Vagrantfile" ]; then
        error "Slot directory missing or invalid: $slot_dir"
        error "Run the setup script first or create Vagrantfiles for parallel slots"
        exit 1
    fi
done
success "All 3 VM slots configured"

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

# Helper to run vagrant ssh with specific slot (with retries)
slot_vagrant_ssh() {
    local slot_dir="$1"
    local vm="$2"
    shift 2
    local cmd="$@"
    local max_attempts=3
    local attempt=1

    while [ $attempt -le $max_attempts ]; do
        if (cd "$slot_dir" && vagrant ssh "$vm" -c "$cmd" 2>/dev/null); then
            return 0
        fi
        if [ $attempt -lt $max_attempts ]; then
            sleep 1
        fi
        attempt=$((attempt + 1))
    done
    return 1
}

# Copy log file from VM to host (slot-aware)
copy_slot_vm_log() {
    local slot_dir="$1"
    local vm="$2"
    local vm_log_file="$3"
    local host_log_file="$4"

    # Ensure host directory exists
    mkdir -p "$(dirname "$host_log_file")" 2>/dev/null || true

    # Copy via SSH
    if (cd "$slot_dir" && vagrant ssh "$vm" -c "cat '$vm_log_file' 2>/dev/null" 2>/dev/null) > "$host_log_file" 2>&1; then
        if [ -f "$host_log_file" ] && [ -s "$host_log_file" ]; then
            return 0
        fi
    fi
    return 1
}

# Kill all processes on a VM in a slot
kill_slot_vm_processes() {
    local slot_dir="$1"
    local vm="$2"

    (cd "$slot_dir" && vagrant ssh "$vm" -c "
        # Kill by process name
        pkill -9 -f logstream 2>/dev/null || true
        pkill -9 -f producer 2>/dev/null || true
        pkill -9 -f consumer 2>/dev/null || true

        # Kill by port
        for port in 8001 8002 8003 8888 9999; do
            if command -v fuser >/dev/null 2>&1; then
                fuser -k -9 \${port}/tcp 2>/dev/null || true
                fuser -k -9 \${port}/udp 2>/dev/null || true
            fi
        done
    " 2>/dev/null) || true
}

# Verify clean state on a VM (no running processes)
verify_slot_vm_clean() {
    local slot_dir="$1"
    local vm="$2"

    local proc_count=$(cd "$slot_dir" && vagrant ssh "$vm" -c "pgrep -f logstream 2>/dev/null | wc -l" 2>/dev/null || echo "0")
    proc_count=$(echo "$proc_count" | tr -d '\n\r ' | grep -E '^[0-9]+$' || echo "0")

    if [ "$proc_count" = "0" ]; then
        return 0
    else
        return 1
    fi
}

# Build binaries on a VM in a slot
build_slot_vm_binaries() {
    local slot_dir="$1"
    local vm="$2"
    local build_failed=0

    log "    Building on $vm..."

    # Build logstream
    local build_output=$(cd "$slot_dir" && vagrant ssh "$vm" -c "cd /vagrant/logstream && /usr/local/go/bin/go build -o logstream . 2>&1" 2>&1 | grep -vE "WARNING|Permission denied|libvirt_ip_command" || true)

    if [ -n "$build_output" ] && echo "$build_output" | grep -qiE "error|undefined|cannot|failed"; then
        error "      logstream build failed: $build_output"
        build_failed=1
    elif (cd "$slot_dir" && vagrant ssh "$vm" -c "test -f /vagrant/logstream/logstream && test -x /vagrant/logstream/logstream" 2>/dev/null); then
        local size=$(cd "$slot_dir" && vagrant ssh "$vm" -c "stat -c%s /vagrant/logstream/logstream 2>/dev/null" 2>/dev/null || echo "0")
        log "      logstream: $size bytes"
    else
        error "      logstream: binary not found"
        build_failed=1
    fi

    # Build producer
    build_output=$(cd "$slot_dir" && vagrant ssh "$vm" -c "cd /vagrant/logstream && /usr/local/go/bin/go build -o producer cmd/producer/main.go 2>&1" 2>&1 | grep -vE "WARNING|Permission denied|libvirt_ip_command" || true)

    if [ -n "$build_output" ] && echo "$build_output" | grep -qiE "error|undefined|cannot|failed"; then
        error "      producer build failed: $build_output"
        build_failed=1
    elif (cd "$slot_dir" && vagrant ssh "$vm" -c "test -f /vagrant/logstream/producer && test -x /vagrant/logstream/producer" 2>/dev/null); then
        local size=$(cd "$slot_dir" && vagrant ssh "$vm" -c "stat -c%s /vagrant/logstream/producer 2>/dev/null" 2>/dev/null || echo "0")
        log "      producer: $size bytes"
    else
        error "      producer: binary not found"
        build_failed=1
    fi

    # Build consumer
    build_output=$(cd "$slot_dir" && vagrant ssh "$vm" -c "cd /vagrant/logstream && /usr/local/go/bin/go build -o consumer cmd/consumer/main.go 2>&1" 2>&1 | grep -vE "WARNING|Permission denied|libvirt_ip_command" || true)

    if [ -n "$build_output" ] && echo "$build_output" | grep -qiE "error|undefined|cannot|failed"; then
        error "      consumer build failed: $build_output"
        build_failed=1
    elif (cd "$slot_dir" && vagrant ssh "$vm" -c "test -f /vagrant/logstream/consumer && test -x /vagrant/logstream/consumer" 2>/dev/null); then
        local size=$(cd "$slot_dir" && vagrant ssh "$vm" -c "stat -c%s /vagrant/logstream/consumer 2>/dev/null" 2>/dev/null || echo "0")
        log "      consumer: $size bytes"
    else
        error "      consumer: binary not found"
        build_failed=1
    fi

    return $build_failed
}

# Full cleanup and restart for a slot (like cleanup_vms_between_tests)
full_slot_cleanup_and_restart() {
    local slot_num="$1"
    local slot_dir="${SLOT_DIRS[$((slot_num - 1))]}"

    log "  [Slot $slot_num] Step 0: Killing all processes..."
    for vm in leader broker1 broker2; do
        if (cd "$slot_dir" && vagrant status $vm 2>&1 | grep -E "^${vm}\s+.*running" >/dev/null 2>&1); then
            kill_slot_vm_processes "$slot_dir" "$vm"
            sleep 1
            if verify_slot_vm_clean "$slot_dir" "$vm"; then
                log "    [OK] $vm clean"
            else
                log "    [!] $vm still has processes, retrying..."
                kill_slot_vm_processes "$slot_dir" "$vm"
            fi
        fi
    done

    sleep 2

    log "  [Slot $slot_num] Step 1: Force halting VMs..."
    cd "$slot_dir"
    for vm in leader broker1 broker2; do
        if vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "^${vm}\s+.*running" >/dev/null 2>&1; then
            vagrant halt $vm --force 2>&1 | grep -vE "WARNING|Permission denied|libvirt_ip_command" >/dev/null 2>&1 || true
        fi
    done
    cd "$PROJECT_ROOT"

    log "  [Slot $slot_num] Step 2: Waiting for shutdown..."
    sleep 5

    log "  [Slot $slot_num] Step 3: Starting VMs fresh..."
    cd "$slot_dir"
    for vm in leader broker1 broker2; do
        vagrant up $vm 2>&1 | grep -vE "WARNING|Permission denied|libvirt_ip_command" >/dev/null 2>&1 || true
        sleep 2
        if vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "^${vm}\s+.*running" >/dev/null 2>&1; then
            log "    [OK] $vm running"
        else
            sleep 3
            if vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "^${vm}\s+.*running" >/dev/null 2>&1; then
                log "    [OK] $vm running (delayed)"
            else
                error "    [X] $vm failed to start!"
            fi
        fi
    done
    cd "$PROJECT_ROOT"

    log "  [Slot $slot_num] Step 4: Waiting for VMs to be ready..."
    sleep 5

    log "  [Slot $slot_num] Step 5: Syncing files..."
    cd "$slot_dir"
    for vm in leader broker1 broker2; do
        if vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "^${vm}\s+.*running" >/dev/null 2>&1; then
            vagrant rsync $vm 2>&1 | grep -vE "WARNING|Permission denied|libvirt_ip_command" >/dev/null 2>&1 || true
            log "    [OK] $vm synced"
        fi
    done
    cd "$PROJECT_ROOT"

    log "  [Slot $slot_num] Step 6: Verifying clean state..."
    local all_clean=true
    for vm in leader broker1 broker2; do
        if verify_slot_vm_clean "$slot_dir" "$vm"; then
            log "    [OK] $vm clean"
        else
            log "    [!] $vm has processes"
            all_clean=false
        fi
    done

    if [ "$all_clean" = true ]; then
        success "  [Slot $slot_num] Cleanup complete - ready for tests"
    else
        warn "  [Slot $slot_num] Some processes remain - tests may clean them"
    fi
}

# Start VMs in a specific slot (simple version)
start_slot_vms() {
    local slot_num="$1"
    local slot_dir="${SLOT_DIRS[$((slot_num - 1))]}"

    log "Starting VMs in slot $slot_num ($slot_dir)..."
    cd "$slot_dir"

    for vm in leader broker1 broker2; do
        if vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "^${vm}\s+.*running" >/dev/null 2>&1; then
            log "  $vm already running in slot $slot_num"
        else
            log "  Starting $vm in slot $slot_num..."
            vagrant up $vm 2>&1 | grep -vE "WARNING|Permission denied|libvirt_ip_command" >/dev/null 2>&1 || true
        fi
    done

    cd "$PROJECT_ROOT"
}

# Stop VMs in a specific slot with full cleanup
stop_slot_vms() {
    local slot_num="$1"
    local slot_dir="${SLOT_DIRS[$((slot_num - 1))]}"

    log "Stopping VMs in slot $slot_num..."
    cd "$slot_dir"

    # Kill processes first (thoroughly)
    for vm in leader broker1 broker2; do
        if vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "^${vm}\s+.*running" >/dev/null 2>&1; then
            kill_slot_vm_processes "$slot_dir" "$vm"
        fi
    done

    sleep 2

    # Halt VMs
    vagrant halt --force 2>&1 | grep -vE "WARNING|Permission denied|libvirt_ip_command" >/dev/null 2>&1 || true

    cd "$PROJECT_ROOT"
}

# Sync files to VMs in a slot
sync_slot_vms() {
    local slot_num="$1"
    local slot_dir="${SLOT_DIRS[$((slot_num - 1))]}"

    cd "$slot_dir"

    for vm in leader broker1 broker2; do
        if vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "^${vm}\s+.*running" >/dev/null 2>&1; then
            vagrant rsync $vm 2>&1 | grep -vE "WARNING|Permission denied|libvirt_ip_command" >/dev/null 2>&1 || true
        fi
    done

    cd "$PROJECT_ROOT"
}

# Stop ALL VMs in ALL slots (complete cleanup between batches)
stop_all_slots() {
    log ""
    log "============================================="
    log "FULL VM CLEANUP - Stopping ALL VMs in ALL slots"
    log "============================================="

    for slot_num in 1 2 3; do
        stop_slot_vms "$slot_num"
    done

    # Wait for VMs to fully stop
    sleep 5

    success "All VMs stopped"
}

# Start fresh VMs in ALL slots with full cleanup and verification
start_all_slots() {
    log ""
    log "============================================="
    log "STARTING FRESH VMs in ALL slots (Full Cleanup)"
    log "============================================="

    # Run full cleanup and restart for all slots in parallel
    for slot_num in 1 2 3; do
        (
            full_slot_cleanup_and_restart "$slot_num"
        ) &
    done

    # Wait for all to complete
    wait

    # Verify all VMs are running and clean
    log ""
    log "Verifying all VMs are running..."
    local all_running=true
    for slot_num in 1 2 3; do
        local slot_dir="${SLOT_DIRS[$((slot_num - 1))]}"
        cd "$slot_dir"
        for vm in leader broker1 broker2; do
            if ! vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "^${vm}\s+.*running" >/dev/null 2>&1; then
                error "  VM $vm in slot $slot_num is not running!"
                all_running=false
            fi
        done
        cd "$PROJECT_ROOT"
    done

    if [ "$all_running" = true ]; then
        success "All 9 VMs are running (3 slots x 3 VMs)"
    else
        error "Some VMs failed to start"
        return 1
    fi
}

# Get test category from test name
get_test_category() {
    local test_name="$1"
    case "$test_name" in
        SINGLE|TRIO|SEQUENCE|PRODUCER-CONSUMER)
            echo "core"
            ;;
        ELECTION-AUTOMATIC|STREAM-FAILOVER|VIEW-SYNC|STATE-EXCHANGE)
            echo "advanced"
            ;;
        CLIENT-CLEANUP|FIFO-ORDERING|ONE-TO-ONE-MAPPING)
            echo "client"
            ;;
        *)
            echo "other"
            ;;
    esac
}

# =============================================================================
# TEST BATCHES
# =============================================================================

# BATCH 1: Producer-Consumer Tests (Light tests)
declare -a BATCH1_NAMES=("PRODUCER-CONSUMER" "CLIENT-CLEANUP" "FIFO-ORDERING")
declare -a BATCH1_SCRIPTS=(
    "./tests/linux/test-producer-consumer.sh"
    "./tests/linux/test-client-cleanup.sh"
    "./tests/linux/test-fifo-ordering.sh"
)

# BATCH 2: Mapping and State Tests (Medium tests)
declare -a BATCH2_NAMES=("ONE-TO-ONE-MAPPING" "STATE-EXCHANGE")
declare -a BATCH2_SCRIPTS=(
    "./tests/linux/test-one-to-one-mapping.sh"
    "./tests/linux/test-state-exchange.sh"
)

# BATCH 3: Cluster Tests (Heavy tests)
declare -a BATCH3_NAMES=("ELECTION-AUTOMATIC" "VIEW-SYNC" "STREAM-FAILOVER")
declare -a BATCH3_SCRIPTS=(
    "./tests/linux/test-election-automatic.sh"
    "./tests/linux/test-view-sync.sh"
    "./tests/linux/test-stream-failover.sh"
)

# Track results
declare -A TEST_RESULTS
declare -A TEST_DURATIONS
declare -A TEST_CATEGORIES
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# Function to run a single test (called as background job)
# This function is designed to be run directly with & (not in command substitution)
_run_test_impl() {
    local test_name="$1"
    local test_script="$2"
    local slot_num="$3"
    local test_category="$4"
    local log_base_dir="$5"
    local slot_dir="$6"

    # Create category-based log directory
    local test_log_dir="$log_base_dir/$test_category/${test_name,,}"
    local test_log_file="$test_log_dir/test.log"
    local exit_code_file="$test_log_dir/exit_code.txt"

    mkdir -p "$test_log_dir"

    local start_time=$(date +%s)

    # Write test header
    {
        echo "========================================"
        echo "Test: $test_name"
        echo "Category: $test_category"
        echo "Slot: $slot_num ($slot_dir)"
        echo "Started: $(date)"
        echo "Script: $test_script"
        echo "========================================"
        echo ""
    } > "$test_log_file"

    # Export VAGRANT_DIR for this test
    export VAGRANT_DIR="$slot_dir"
    export LOG_BASE_DIR="$log_base_dir"

    # Run the test
    set +e
    "$test_script" vagrant >> "$test_log_file" 2>&1
    local exit_code=$?
    set -e

    local end_time=$(date +%s)
    local duration=$((end_time - start_time))

    # Append test footer
    {
        echo ""
        echo "========================================"
        echo "Test completed: $(date)"
        echo "Exit code: $exit_code"
        echo "Duration: ${duration}s"
        echo "========================================"
    } >> "$test_log_file"

    # Save exit code
    echo "$exit_code" > "$exit_code_file"

    # Copy process logs from VMs to host
    echo "" >> "$test_log_file"
    echo "[$test_name] Copying process logs from VMs to host..." >> "$test_log_file"

    local vm_log_base="/vagrant/logstream/test-logs/$(basename "$log_base_dir")/$test_category/${test_name,,}"

    for vm in leader broker1 broker2; do
        # Check if VM is still running
        if (cd "$slot_dir" && vagrant status $vm 2>&1 | grep -E "^${vm}\s+.*running" >/dev/null 2>&1); then
            # Find log files on VM
            local log_files=$(cd "$slot_dir" && vagrant ssh "$vm" -c "test -d '$vm_log_base' && find '$vm_log_base' -name '*.log' -type f 2>/dev/null" 2>/dev/null || echo "")

            if [ -n "$log_files" ]; then
                for vm_log_file in $log_files; do
                    if echo "$vm_log_file" | grep -q "\.log$"; then
                        local log_filename=$(basename "$vm_log_file")
                        local host_log_file="$test_log_dir/${vm}-${log_filename}"

                        echo "[$test_name] Copying $log_filename from $vm..." >> "$test_log_file"

                        if (cd "$slot_dir" && vagrant ssh "$vm" -c "cat '$vm_log_file' 2>/dev/null" 2>/dev/null) > "$host_log_file" 2>&1; then
                            if [ -f "$host_log_file" ] && [ -s "$host_log_file" ]; then
                                local line_count=$(wc -l < "$host_log_file" 2>/dev/null || echo "0")
                                local file_size=$(stat -c%s "$host_log_file" 2>/dev/null || echo "0")
                                echo "[$test_name] [OK] Copied $log_filename from $vm ($line_count lines, ${file_size} bytes)" >> "$test_log_file"
                            else
                                rm -f "$host_log_file" 2>/dev/null || true
                                echo "[$test_name] [!] Empty log file from $vm" >> "$test_log_file"
                            fi
                        else
                            echo "[$test_name] [!] Failed to copy $log_filename from $vm" >> "$test_log_file"
                        fi
                    fi
                done
            fi

            # Also try to copy common log locations
            for log_name in leader.log broker1.log broker2.log producer.log consumer.log; do
                local vm_log="/vagrant/logstream/test-logs/$(basename "$log_base_dir")/$test_category/${test_name,,}/$log_name"
                local host_log="$test_log_dir/${vm}-${log_name}"

                if [ ! -f "$host_log" ]; then
                    if (cd "$slot_dir" && vagrant ssh "$vm" -c "test -f '$vm_log' && cat '$vm_log'" 2>/dev/null) > "$host_log" 2>&1; then
                        if [ -f "$host_log" ] && [ -s "$host_log" ]; then
                            echo "[$test_name] [OK] Copied $log_name from $vm" >> "$test_log_file"
                        else
                            rm -f "$host_log" 2>/dev/null || true
                        fi
                    fi
                fi
            done
        else
            echo "[$test_name] [!] VM $vm not running, skipping log copy" >> "$test_log_file"
        fi
    done

    # Final result in log
    echo "" >> "$test_log_file"
    if [ $exit_code -eq 0 ]; then
        echo "[$test_name] [OK] Test PASSED" >> "$test_log_file"
    else
        echo "[$test_name] [X] Test FAILED (exit code: $exit_code)" >> "$test_log_file"
    fi

    return $exit_code
}

# Function to run a batch of tests in parallel
run_batch() {
    local batch_name="$1"
    shift
    local -n names_ref=$1
    shift
    local -n scripts_ref=$1

    local batch_size=${#names_ref[@]}

    log ""
    log "============================================="
    log "BATCH: $batch_name ($batch_size tests)"
    log "============================================="

    # Log to summary
    {
        echo ""
        echo "Batch: $batch_name"
        echo "Tests: ${names_ref[*]}"
        echo "Started: $(date)"
    } >> "$LOG_BASE_DIR/summary.txt"

    log "Starting VMs fresh for this batch..."
    log ""

    # Start all VMs fresh
    start_all_slots || {
        error "Failed to start VMs for batch $batch_name"
        return 1
    }

    log ""
    log "Running tests in parallel with dedicated VMs..."
    log ""

    local pids=()
    local test_names_arr=()
    local slot_nums_arr=()
    local categories_arr=()
    local batch_start=$(date +%s)

    # Start all tests in parallel, each with its own VM slot
    for i in "${!names_ref[@]}"; do
        local test_name="${names_ref[$i]}"
        local test_script="${scripts_ref[$i]}"
        local slot_num=$((i + 1))
        local test_category=$(get_test_category "$test_name")
        local slot_dir="${SLOT_DIRS[$((slot_num - 1))]}"

        TEST_CATEGORIES["$test_name"]="$test_category"

        log "  Starting: $test_name (Slot $slot_num, Category: $test_category)"

        # Start test directly as background job (not in command substitution)
        _run_test_impl "$test_name" "$test_script" "$slot_num" "$test_category" "$LOG_BASE_DIR" "$slot_dir" &
        local pid=$!

        pids+=("$pid")
        test_names_arr+=("$test_name")
        slot_nums_arr+=("$slot_num")
        categories_arr+=("$test_category")
    done

    log ""
    log "Waiting for batch to complete..."
    log ""

    # Wait for all tests and collect results
    for i in "${!pids[@]}"; do
        local pid="${pids[$i]}"
        local test_name="${test_names_arr[$i]}"
        local slot_num="${slot_nums_arr[$i]}"
        local test_category="${categories_arr[$i]}"

        local test_log_dir="$LOG_BASE_DIR/$test_category/${test_name,,}"
        local test_log_file="$test_log_dir/test.log"

        if wait "$pid"; then
            success "  $test_name (Slot $slot_num): PASSED"
            TEST_RESULTS["$test_name"]="PASSED"
            ((PASSED_TESTS++))
        else
            error "  $test_name (Slot $slot_num): FAILED"
            TEST_RESULTS["$test_name"]="FAILED"
            ((FAILED_TESTS++))
        fi
        ((TOTAL_TESTS++))

        # Extract duration from log
        local duration=$(grep "Duration:" "$test_log_file" 2>/dev/null | tail -1 | sed 's/.*Duration: \([0-9]*\)s.*/\1/' || echo "0")
        TEST_DURATIONS["$test_name"]="$duration"

        # Append test log to combined log with prefix
        if [ -f "$test_log_file" ]; then
            while IFS= read -r line || [ -n "$line" ]; do
                echo "[$test_name] $line" >> "$LOG_BASE_DIR/combined.log"
            done < "$test_log_file"
        fi
    done

    local batch_end=$(date +%s)
    local batch_duration=$((batch_end - batch_start))

    log ""
    log "Batch completed in ${batch_duration}s"

    # Log to summary
    echo "Completed: $(date) (${batch_duration}s)" >> "$LOG_BASE_DIR/summary.txt"

    # Full cleanup after batch - stop ALL VMs
    stop_all_slots
}

# =============================================================================
# MAIN EXECUTION
# =============================================================================

# Record overall start time
OVERALL_START=$(date +%s)

log ""
log "============================================="
log "INITIAL VM SETUP AND BINARY BUILD (PARALLEL)"
log "============================================="
log "Setting up all 3 VM slots in parallel..."
log ""

# Build binaries on host first (if rebuild.sh exists) - this is fast
if [ -f "$PROJECT_ROOT/rebuild.sh" ]; then
    log "Building binaries on host first..."
    if ! "$PROJECT_ROOT/rebuild.sh" > "$LOG_BASE_DIR/build.log" 2>&1; then
        warn "Host build failed, will build on VMs"
    else
        success "Host binaries built"
    fi
fi

# Function to setup a single slot (used for parallel execution)
setup_single_slot() {
    local slot_num="$1"
    local slot_dir="${SLOT_DIRS[$((slot_num - 1))]}"
    local slot_log="$LOG_BASE_DIR/slot${slot_num}-setup.log"

    {
        echo "========================================"
        echo "[Slot $slot_num] Setup started: $(date)"
        echo "========================================"
        echo ""

        echo "[Slot $slot_num] Starting VMs..."
        cd "$slot_dir"
        for vm in leader broker1 broker2; do
            echo "[Slot $slot_num]   Starting $vm..."
            if ! vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "^${vm}\s+.*running" >/dev/null 2>&1; then
                vagrant up $vm 2>&1 | grep -vE "WARNING|Permission denied|libvirt_ip_command" || true
            else
                echo "[Slot $slot_num]   $vm already running"
            fi
        done
        cd "$PROJECT_ROOT"

        echo "[Slot $slot_num] Waiting for VMs to be ready..."
        sleep 5

        echo "[Slot $slot_num] Syncing files..."
        cd "$slot_dir"
        for vm in leader broker1 broker2; do
            if vagrant status $vm 2>&1 | grep -v "Permission denied" | grep -E "^${vm}\s+.*running" >/dev/null 2>&1; then
                vagrant rsync $vm 2>&1 | grep -vE "WARNING|Permission denied|libvirt_ip_command" || true
                echo "[Slot $slot_num]   $vm synced"
            fi
        done
        cd "$PROJECT_ROOT"

        echo "[Slot $slot_num] Building binaries..."
        local build_failed=0

        # Build logstream
        echo "[Slot $slot_num]   Building logstream..."
        local build_output=$(cd "$slot_dir" && vagrant ssh leader -c "cd /vagrant/logstream && /usr/local/go/bin/go build -o logstream . 2>&1" 2>&1 || true)
        if echo "$build_output" | grep -qiE "error|undefined|cannot|failed"; then
            echo "[Slot $slot_num]   ERROR: logstream build failed"
            echo "$build_output"
            build_failed=1
        elif (cd "$slot_dir" && vagrant ssh leader -c "test -f /vagrant/logstream/logstream && test -x /vagrant/logstream/logstream" 2>/dev/null); then
            local size=$(cd "$slot_dir" && vagrant ssh leader -c "stat -c%s /vagrant/logstream/logstream 2>/dev/null" 2>/dev/null || echo "0")
            echo "[Slot $slot_num]   logstream: $size bytes [OK]"
        else
            echo "[Slot $slot_num]   ERROR: logstream binary not found"
            build_failed=1
        fi

        # Build producer
        echo "[Slot $slot_num]   Building producer..."
        build_output=$(cd "$slot_dir" && vagrant ssh leader -c "cd /vagrant/logstream && /usr/local/go/bin/go build -o producer cmd/producer/main.go 2>&1" 2>&1 || true)
        if echo "$build_output" | grep -qiE "error|undefined|cannot|failed"; then
            echo "[Slot $slot_num]   ERROR: producer build failed"
            build_failed=1
        elif (cd "$slot_dir" && vagrant ssh leader -c "test -f /vagrant/logstream/producer && test -x /vagrant/logstream/producer" 2>/dev/null); then
            local size=$(cd "$slot_dir" && vagrant ssh leader -c "stat -c%s /vagrant/logstream/producer 2>/dev/null" 2>/dev/null || echo "0")
            echo "[Slot $slot_num]   producer: $size bytes [OK]"
        else
            echo "[Slot $slot_num]   ERROR: producer binary not found"
            build_failed=1
        fi

        # Build consumer
        echo "[Slot $slot_num]   Building consumer..."
        build_output=$(cd "$slot_dir" && vagrant ssh leader -c "cd /vagrant/logstream && /usr/local/go/bin/go build -o consumer cmd/consumer/main.go 2>&1" 2>&1 || true)
        if echo "$build_output" | grep -qiE "error|undefined|cannot|failed"; then
            echo "[Slot $slot_num]   ERROR: consumer build failed"
            build_failed=1
        elif (cd "$slot_dir" && vagrant ssh leader -c "test -f /vagrant/logstream/consumer && test -x /vagrant/logstream/consumer" 2>/dev/null); then
            local size=$(cd "$slot_dir" && vagrant ssh leader -c "stat -c%s /vagrant/logstream/consumer 2>/dev/null" 2>/dev/null || echo "0")
            echo "[Slot $slot_num]   consumer: $size bytes [OK]"
        else
            echo "[Slot $slot_num]   ERROR: consumer binary not found"
            build_failed=1
        fi

        # Verify clean state
        echo "[Slot $slot_num] Verifying clean state..."
        for vm in leader broker1 broker2; do
            local proc_count=$(cd "$slot_dir" && vagrant ssh $vm -c "pgrep -f logstream 2>/dev/null | wc -l" 2>/dev/null || echo "0")
            proc_count=$(echo "$proc_count" | tr -d '\n\r ')
            if [ "$proc_count" = "0" ]; then
                echo "[Slot $slot_num]   $vm: clean [OK]"
            else
                echo "[Slot $slot_num]   $vm: has $proc_count process(es), cleaning..."
                (cd "$slot_dir" && vagrant ssh $vm -c "pkill -9 -f logstream; pkill -9 -f producer; pkill -9 -f consumer" 2>/dev/null) || true
            fi
        done

        echo ""
        echo "========================================"
        if [ "$build_failed" -eq 0 ]; then
            echo "[Slot $slot_num] Setup completed successfully: $(date)"
            echo "========================================"
            exit 0
        else
            echo "[Slot $slot_num] Setup FAILED: $(date)"
            echo "========================================"
            exit 1
        fi
    } > "$slot_log" 2>&1

    return $?
}

# Export functions and variables for subshells
export -f setup_single_slot
export PROJECT_ROOT LOG_BASE_DIR
export SLOT_DIRS

# Run all 3 slot setups in parallel
log "Starting parallel setup of all 3 slots..."
log "  (This may take a few minutes - starting 9 VMs and building binaries)"
log ""

SLOT1_PID=""
SLOT2_PID=""
SLOT3_PID=""

# Start slot 1 setup in background
(setup_single_slot 1) &
SLOT1_PID=$!
log "  Slot 1 setup started (PID: $SLOT1_PID)"

# Start slot 2 setup in background
(setup_single_slot 2) &
SLOT2_PID=$!
log "  Slot 2 setup started (PID: $SLOT2_PID)"

# Start slot 3 setup in background
(setup_single_slot 3) &
SLOT3_PID=$!
log "  Slot 3 setup started (PID: $SLOT3_PID)"

log ""
log "Waiting for all slots to complete setup..."
log ""

# Wait for all and collect results
SETUP_FAILED=0

if wait $SLOT1_PID; then
    success "Slot 1 setup complete"
else
    error "Slot 1 setup FAILED (see $LOG_BASE_DIR/slot1-setup.log)"
    SETUP_FAILED=1
fi

if wait $SLOT2_PID; then
    success "Slot 2 setup complete"
else
    error "Slot 2 setup FAILED (see $LOG_BASE_DIR/slot2-setup.log)"
    SETUP_FAILED=1
fi

if wait $SLOT3_PID; then
    success "Slot 3 setup complete"
else
    error "Slot 3 setup FAILED (see $LOG_BASE_DIR/slot3-setup.log)"
    SETUP_FAILED=1
fi

if [ "$SETUP_FAILED" -ne 0 ]; then
    error "One or more slots failed to setup"
    log "Check logs for details:"
    log "  $LOG_BASE_DIR/slot1-setup.log"
    log "  $LOG_BASE_DIR/slot2-setup.log"
    log "  $LOG_BASE_DIR/slot3-setup.log"
    exit 1
fi

log ""
success "All 3 slots ready with 9 VMs running and binaries built"

log ""
log "============================================="
log "STARTING TEST EXECUTION"
log "============================================="
log ""
warn "Note: SINGLE, TRIO, SEQUENCE tests are skipped"
warn "      (uncomment in script to include them)"
log ""
warn "Each batch uses 9 VMs (3 slots x 3 VMs each)"
log ""

# Add test list to summary
{
    echo ""
    echo "Tests to run:"
    echo "  Batch 1 (Light): ${BATCH1_NAMES[*]}"
    echo "  Batch 2 (Medium): ${BATCH2_NAMES[*]}"
    echo "  Batch 3 (Heavy): ${BATCH3_NAMES[*]}"
    echo ""
    echo "Starting tests..."
} >> "$LOG_BASE_DIR/summary.txt"

# Run Batch 1
run_batch "Light Tests" BATCH1_NAMES BATCH1_SCRIPTS

# Run Batch 2
run_batch "Medium Tests" BATCH2_NAMES BATCH2_SCRIPTS

# Run Batch 3
run_batch "Heavy Tests" BATCH3_NAMES BATCH3_SCRIPTS

# Calculate overall duration
OVERALL_END=$(date +%s)
OVERALL_DURATION=$((OVERALL_END - OVERALL_START))

# =============================================================================
# FINAL SUMMARY
# =============================================================================
log ""
log "============================================="
log "FINAL TEST SUMMARY"
log "============================================="
log ""

# Print results table
printf "${CYAN}%-25s %-10s %-10s %10s${NC}\n" "TEST" "CATEGORY" "RESULT" "DURATION"
printf "%-25s %-10s %-10s %10s\n" "------------------------" "----------" "----------" "----------"

for test_name in "${!TEST_RESULTS[@]}"; do
    result="${TEST_RESULTS[$test_name]}"
    duration="${TEST_DURATIONS[$test_name]}"
    category="${TEST_CATEGORIES[$test_name]}"

    if [ "$result" = "PASSED" ]; then
        printf "${GREEN}%-25s %-10s %-10s %10ss${NC}\n" "$test_name" "$category" "$result" "$duration"
    else
        printf "${RED}%-25s %-10s %-10s %10ss${NC}\n" "$test_name" "$category" "$result" "$duration"
    fi
done

printf "%-25s %-10s %-10s %10s\n" "------------------------" "----------" "----------" "----------"

log ""
log "Total tests: $TOTAL_TESTS"
success "Passed: $PASSED_TESTS"
if [ "$FAILED_TESTS" -gt 0 ]; then
    error "Failed: $FAILED_TESTS"
fi
log ""
log "Total execution time: ${OVERALL_DURATION}s ($(( OVERALL_DURATION / 60 ))m $(( OVERALL_DURATION % 60 ))s)"
log ""
log "Log directory: $LOG_BASE_DIR"
log "  - Individual test logs: $LOG_BASE_DIR/{category}/{test-name}/test.log"
log "  - Node logs: $LOG_BASE_DIR/{category}/{test-name}/{vm}-*.log"
log "  - Combined log: $LOG_BASE_DIR/combined.log"
log "  - Summary: $LOG_BASE_DIR/summary.txt"
log ""

# Generate final summary
{
    echo ""
    echo "========================================"
    echo "FINAL RESULTS"
    echo "========================================"
    for test_name in "${!TEST_RESULTS[@]}"; do
        result="${TEST_RESULTS[$test_name]}"
        duration="${TEST_DURATIONS[$test_name]}"
        category="${TEST_CATEGORIES[$test_name]}"
        status=$([ "$result" = "PASSED" ] && echo "[OK] PASS" || echo "[X] FAIL")
        printf "%-25s %-10s %s (%ss)\n" "$test_name" "$category" "$status" "$duration"
    done
    echo "========================================"
    echo "Total: $TOTAL_TESTS | Passed: $PASSED_TESTS | Failed: $FAILED_TESTS"
    echo "Duration: ${OVERALL_DURATION}s ($(( OVERALL_DURATION / 60 ))m $(( OVERALL_DURATION % 60 ))s)"
    echo "========================================"
    echo ""
    echo "Log categories:"
    echo "  - core/     : Basic functionality tests"
    echo "  - advanced/ : Leader election, failover, recovery tests"
    echo "  - client/   : Client-side feature tests"
} >> "$LOG_BASE_DIR/summary.txt"

# Exit with appropriate code
if [ "$FAILED_TESTS" -gt 0 ]; then
    error "Some tests failed!"
    exit 1
else
    success "All tests passed!"
    exit 0
fi
