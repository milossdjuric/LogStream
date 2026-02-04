#!/bin/bash
# Run All Tests Across All Modes - Local, Docker, and Vagrant
# Comprehensive test suite for LogStream across all deployment environments

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() { echo -e "${BLUE}[MULTI-MODE] $@${NC}"; }
success() { echo -e "${GREEN}[OK] $@${NC}"; }
error() { echo -e "${RED}[X] $@${NC}"; }
warn() { echo -e "${YELLOW}[!] $@${NC}"; }

# Results tracking - write to test-logs directory (gitignored)
RESULTS_DIR="$PROJECT_ROOT/test-logs/all-modes-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RESULTS_DIR"/{local,docker,vagrant}

LOCAL_PASS=0
LOCAL_FAIL=0
DOCKER_PASS=0
DOCKER_FAIL=0
VAGRANT_PASS=0
VAGRANT_FAIL=0

log "========================================"
log "LogStream Multi-Mode Test Suite"
log "========================================"
log "Results: $RESULTS_DIR"
log ""

# ====================
# Tests That Work in ALL Modes
# ====================
MULTI_MODE_TESTS=(
    "test-single.sh"
    "test-trio.sh"
    "test-election-automatic.sh"
    "test-producer-consumer.sh"
    "test-sequence.sh"
    "test-fifo-ordering.sh"
    "test-one-to-one-mapping.sh"
    "test-stream-failover.sh"
    "test-client-cleanup.sh"
    "test-state-exchange.sh"
    "test-view-sync.sh"
    "test-cluster-state.sh"
)

# Tests that work in specific modes only
NETWORK_TESTS=(
    "test-protocol-compliance.sh"  # local, docker, vagrant
    "test-network-analysis.sh"     # local, docker, vagrant
)

# ====================
# Run Tests in Local Mode
# ====================
run_local_tests() {
    log "========================================"
    log "Running Tests in LOCAL Mode"
    log "========================================"
    
    cd "$SCRIPT_DIR/linux"
    
    for test in "${MULTI_MODE_TESTS[@]}"; do
        log "Running $test (local)..."
        if bash "$test" local > "$RESULTS_DIR/local/${test%.sh}.log" 2>&1; then
            success "$test (local) - PASSED"
            LOCAL_PASS=$((LOCAL_PASS + 1))
        else
            error "$test (local) - FAILED"
            LOCAL_FAIL=$((LOCAL_FAIL + 1))
        fi
    done
    
    # Network tests (require sudo)
    if [ "$EUID" -eq 0 ] || sudo -n true 2>/dev/null; then
        for test in "${NETWORK_TESTS[@]}"; do
            log "Running $test (local)..."
            if sudo bash "$test" local > "$RESULTS_DIR/local/${test%.sh}.log" 2>&1; then
                success "$test (local) - PASSED"
                LOCAL_PASS=$((LOCAL_PASS + 1))
            else
                error "$test (local) - FAILED"
                LOCAL_FAIL=$((LOCAL_FAIL + 1))
            fi
        done
    else
        warn "Skipping network tests in local mode (no sudo)"
    fi
    
    log ""
    log "Local Mode Summary: $LOCAL_PASS passed, $LOCAL_FAIL failed"
    log ""
}

# ====================
# Run Tests in Docker Mode
# ====================
run_docker_tests() {
    log "========================================"
    log "Running Tests in DOCKER Mode"
    log "========================================"
    
    # Check if Docker is available
    if ! command -v docker &> /dev/null; then
        warn "Docker not found, skipping Docker tests"
        return
    fi
    
    if ! docker ps &> /dev/null; then
        warn "Docker daemon not running, skipping Docker tests"
        return
    fi
    
    cd "$SCRIPT_DIR/linux"
    
    for test in "${MULTI_MODE_TESTS[@]}"; do
        log "Running $test (docker)..."
        if bash "$test" docker > "$RESULTS_DIR/docker/${test%.sh}.log" 2>&1; then
            success "$test (docker) - PASSED"
            DOCKER_PASS=$((DOCKER_PASS + 1))
        else
            error "$test (docker) - FAILED"
            DOCKER_FAIL=$((DOCKER_FAIL + 1))
        fi
    done
    
    # Network tests
    if [ "$EUID" -eq 0 ] || sudo -n true 2>/dev/null; then
        for test in "${NETWORK_TESTS[@]}"; do
            log "Running $test (docker)..."
            if sudo bash "$test" docker > "$RESULTS_DIR/docker/${test%.sh}.log" 2>&1; then
                success "$test (docker) - PASSED"
                DOCKER_PASS=$((DOCKER_PASS + 1))
            else
                error "$test (docker) - FAILED"
                DOCKER_FAIL=$((DOCKER_FAIL + 1))
            fi
        done
    else
        warn "Skipping network tests in docker mode (no sudo)"
    fi
    
    log ""
    log "Docker Mode Summary: $DOCKER_PASS passed, $DOCKER_FAIL failed"
    log ""
}

# ====================
# Run Tests in Vagrant Mode
# ====================
run_vagrant_tests() {
    log "========================================"
    log "Running Tests in VAGRANT Mode"
    log "========================================"
    
    # Check if Vagrant is available
    if ! command -v vagrant &> /dev/null; then
        warn "Vagrant not found, skipping Vagrant tests"
        return
    fi
    
    # Check if VMs are running
    cd "$PROJECT_ROOT/deploy/vagrant"
    if ! vagrant status | grep -q "running"; then
        warn "Vagrant VMs not running, attempting to start..."
        if ! vagrant up; then
            error "Failed to start Vagrant VMs, skipping Vagrant tests"
            return
        fi
    fi
    
    cd "$SCRIPT_DIR/linux"
    
    for test in "${MULTI_MODE_TESTS[@]}"; do
        log "Running $test (vagrant)..."
        if bash "$test" vagrant > "$RESULTS_DIR/vagrant/${test%.sh}.log" 2>&1; then
            success "$test (vagrant) - PASSED"
            VAGRANT_PASS=$((VAGRANT_PASS + 1))
        else
            error "$test (vagrant) - FAILED"
            VAGRANT_FAIL=$((VAGRANT_FAIL + 1))
        fi
    done
    
    # Network tests
    if [ "$EUID" -eq 0 ] || sudo -n true 2>/dev/null; then
        for test in "${NETWORK_TESTS[@]}"; do
            log "Running $test (vagrant)..."
            if sudo bash "$test" vagrant > "$RESULTS_DIR/vagrant/${test%.sh}.log" 2>&1; then
                success "$test (vagrant) - PASSED"
                VAGRANT_PASS=$((VAGRANT_PASS + 1))
            else
                error "$test (vagrant) - FAILED"
                VAGRANT_FAIL=$((VAGRANT_FAIL + 1))
            fi
        done
    else
        warn "Skipping network tests in vagrant mode (no sudo)"
    fi
    
    log ""
    log "Vagrant Mode Summary: $VAGRANT_PASS passed, $VAGRANT_FAIL failed"
    log ""
}

# ====================
# Generate Summary Report
# ====================
generate_summary() {
    local summary_file="$RESULTS_DIR/MULTI_MODE_SUMMARY.txt"
    
    cat > "$summary_file" << EOF
LogStream Multi-Mode Test Summary
==================================
Generated: $(date)
Results Directory: $RESULTS_DIR

Test Execution Summary:
-----------------------

LOCAL Mode:
  Tests Run:    $(($LOCAL_PASS + $LOCAL_FAIL))
  Passed:       $LOCAL_PASS
  Failed:       $LOCAL_FAIL
  Status:       $([ $LOCAL_FAIL -eq 0 ] && echo "[OK] ALL PASSED" || echo "[X] SOME FAILED")

DOCKER Mode:
  Tests Run:    $(($DOCKER_PASS + $DOCKER_FAIL))
  Passed:       $DOCKER_PASS
  Failed:       $DOCKER_FAIL
  Status:       $([ $DOCKER_FAIL -eq 0 ] && echo "[OK] ALL PASSED" || echo "[X] SOME FAILED")

VAGRANT Mode:
  Tests Run:    $(($VAGRANT_PASS + $VAGRANT_FAIL))
  Passed:       $VAGRANT_PASS
  Failed:       $VAGRANT_FAIL
  Status:       $([ $VAGRANT_FAIL -eq 0 ] && echo "[OK] ALL PASSED" || echo "[X] SOME FAILED")

Overall:
  Total Tests:  $(($LOCAL_PASS + $LOCAL_FAIL + $DOCKER_PASS + $DOCKER_FAIL + $VAGRANT_PASS + $VAGRANT_FAIL))
  Total Passed: $(($LOCAL_PASS + $DOCKER_PASS + $VAGRANT_PASS))
  Total Failed: $(($LOCAL_FAIL + $DOCKER_FAIL + $VAGRANT_FAIL))

Tests Executed (all modes):
----------------------------
EOF

    for test in "${MULTI_MODE_TESTS[@]}"; do
        echo "  - $test" >> "$summary_file"
    done
    
    cat >> "$summary_file" << EOF

Network Tests (all modes):
--------------------------
EOF

    for test in "${NETWORK_TESTS[@]}"; do
        echo "  - $test" >> "$summary_file"
    done
    
    cat >> "$summary_file" << EOF

Detailed Logs:
--------------
  Local:   $RESULTS_DIR/local/*.log
  Docker:  $RESULTS_DIR/docker/*.log
  Vagrant: $RESULTS_DIR/vagrant/*.log

Mode Comparison:
----------------
$(compare_modes)

EOF

    # Display summary
    cat "$summary_file"
}

# Compare results across modes
compare_modes() {
    local comparison=""
    
    for test in "${MULTI_MODE_TESTS[@]}"; do
        local test_name="${test%.sh}"
        local local_status="⊘"
        local docker_status="⊘"
        local vagrant_status="⊘"
        
        [ -f "$RESULTS_DIR/local/${test_name}.log" ] && grep -q "SUCCESS\|PASS" "$RESULTS_DIR/local/${test_name}.log" 2>/dev/null && local_status="[OK]" || local_status="[X]"
        [ -f "$RESULTS_DIR/docker/${test_name}.log" ] && grep -q "SUCCESS\|PASS" "$RESULTS_DIR/docker/${test_name}.log" 2>/dev/null && docker_status="[OK]" || docker_status="[X]"
        [ -f "$RESULTS_DIR/vagrant/${test_name}.log" ] && grep -q "SUCCESS\|PASS" "$RESULTS_DIR/vagrant/${test_name}.log" 2>/dev/null && vagrant_status="[OK]" || vagrant_status="[X]"
        
        printf "%-30s | Local: %-1s | Docker: %-1s | Vagrant: %-1s\n" "$test_name" "$local_status" "$docker_status" "$vagrant_status"
    done
}

# ====================
# Main Execution
# ====================
main() {
    log "Starting multi-mode test suite..."
    log "This will run all tests in local, docker, and vagrant modes"
    log ""
    
    # Determine which modes to run
    RUN_LOCAL=true
    RUN_DOCKER=true
    RUN_VAGRANT=true
    
    # Parse arguments
    if [ "$1" = "--local-only" ]; then
        RUN_DOCKER=false
        RUN_VAGRANT=false
    elif [ "$1" = "--docker-only" ]; then
        RUN_LOCAL=false
        RUN_VAGRANT=false
    elif [ "$1" = "--vagrant-only" ]; then
        RUN_LOCAL=false
        RUN_DOCKER=false
    fi
    
    # Run tests
    [ "$RUN_LOCAL" = true ] && run_local_tests
    [ "$RUN_DOCKER" = true ] && run_docker_tests
    [ "$RUN_VAGRANT" = true ] && run_vagrant_tests
    
    # Generate summary
    generate_summary
    
    # Exit code
    TOTAL_FAILURES=$(($LOCAL_FAIL + $DOCKER_FAIL + $VAGRANT_FAIL))
    
    if [ $TOTAL_FAILURES -eq 0 ]; then
        success "========================================"
        success "All Tests Passed Across All Modes!"
        success "========================================"
        exit 0
    else
        error "========================================"
        error "Some Tests Failed ($TOTAL_FAILURES failures)"
        error "========================================"
        exit 1
    fi
}

# Show usage
if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    cat << EOF
Usage: $0 [OPTIONS]

Run all LogStream tests across all deployment modes (local, docker, vagrant).

Options:
  (none)           Run tests in all modes (default)
  --local-only     Run tests only in local mode
  --docker-only    Run tests only in docker mode
  --vagrant-only   Run tests only in vagrant mode
  -h, --help       Show this help message

Examples:
  $0                    # Run all tests in all modes
  $0 --local-only       # Run only local mode tests
  $0 --docker-only      # Run only docker mode tests

Results are saved to: test-logs/all-modes-YYYYMMDD-HHMMSS/

Tests that run in all modes:
  - test-single.sh
  - test-trio.sh
  - test-election-automatic.sh
  - test-producer-consumer.sh
  - test-sequence.sh
  - test-fifo-ordering.sh
  - test-one-to-one-mapping.sh
  - test-stream-failover.sh
  - test-client-cleanup.sh
  - test-state-exchange.sh
  - test-view-sync.sh
  - test-cluster-state.sh
  - test-protocol-compliance.sh (requires sudo)
  - test-network-analysis.sh (requires sudo)

EOF
    exit 0
fi

# Run main
main "$@"
