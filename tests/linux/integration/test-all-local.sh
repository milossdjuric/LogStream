#!/bin/bash

# Local Sequential Test Runner - All 5 Tests
# Usage: ./test-all-local.sh

# Don't use set -e - we want to continue even if individual tests fail
# set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$PROJECT_ROOT"

# Check binaries exist
if [ ! -f logstream ]; then
    echo "ERROR: Binary not found!"
    echo "Run: ./scripts/rebuild.sh"
    exit 1
fi

echo "[OK] Binary ready"
echo ""

# Create logs directory structure
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
LOG_BASE_DIR="$PROJECT_ROOT/test-logs/local-all-${TIMESTAMP}"
mkdir -p "$LOG_BASE_DIR"

# Note: Manual election test removed - automatic election test covers the functionality
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
    pkill -f logstream 2>/dev/null || true
    pkill -f producer 2>/dev/null || true
    pkill -f consumer 2>/dev/null || true
    
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
echo "Running All Tests (Local Mode)"
echo "Sequential Execution"
echo "========================================"
echo ""
echo "This will run 5 tests sequentially on the local machine:"
echo "  1. Single broker"
echo "  2. Trio (3 brokers)"
echo "  3. Sequence Demo"
echo "  4. Election (Automatic failure detection)"
echo "  5. Producer-Consumer"
echo ""
echo "Logs will be saved to: $LOG_BASE_DIR"
echo "Press Ctrl+C to stop tests and cleanup."
echo ""

# Save initial info to summary
{
    echo "Local Sequential Test Run Summary"
    echo "=================================="
    echo "Mode: local"
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

sleep 2

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
    LOG_BASE_DIR="$LOG_BASE_DIR" "$test_script" "local" >> "$test_log_file" 2>&1
    local exit_code=$?
    
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
    
    # Small delay between tests to ensure cleanup completes
sleep 2
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

# Return non-zero if any test failed
exit $FAILED
