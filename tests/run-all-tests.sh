#!/bin/bash
# Master Test Automation Script for LogStream
# Runs all tests: unit, integration, network analysis
# Supports multiple deployment modes: local, docker, vagrant
# Usage: ./run-all-tests.sh [local|docker|vagrant|all] [--skip-unit] [--skip-integration] [--skip-network]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TEST_PREFIX="[TEST-RUNNER]"

MODE="${1:-local}"
SKIP_UNIT=false
SKIP_INTEGRATION=false
SKIP_NETWORK=false

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() {
    echo -e "${BLUE}$@${NC}"
}

success() {
    echo -e "${GREEN}[OK] $@${NC}"
}

error() {
    echo -e "${RED}[X] $@${NC}"
}

warn() {
    echo -e "${YELLOW}[!] $@${NC}"
}

# Parse arguments
shift || true
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-unit)
            SKIP_UNIT=true
            shift
            ;;
        --skip-integration)
            SKIP_INTEGRATION=true
            shift
            ;;
        --skip-network)
            SKIP_NETWORK=true
            shift
            ;;
        *)
            shift
            ;;
    esac
done

# Check if running as root (needed for network tests)
check_sudo() {
    if [ "$EUID" -ne 0 ] && [ "$SKIP_NETWORK" = false ]; then
        warn "Network tests require sudo. Run with: sudo ./run-all-tests.sh"
        warn "Or skip network tests with: ./run-all-tests.sh --skip-network"
        exit 1
    fi
}

# Create results directory - write to test-logs directory (gitignored)
RESULTS_DIR="$PROJECT_ROOT/test-logs/tests-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RESULTS_DIR"

log "========================================"
log "LogStream Automated Test Suite"
log "========================================"
log "Mode: $MODE"
log "Results: $RESULTS_DIR"
log ""

# Track results
UNIT_PASS=0
UNIT_FAIL=0
INTEGRATION_PASS=0
INTEGRATION_FAIL=0
NETWORK_PASS=0
NETWORK_FAIL=0

# ====================
# 1. UNIT TESTS
# ====================
run_unit_tests() {
    if [ "$SKIP_UNIT" = true ]; then
        warn "Skipping unit tests"
        return
    fi

    log "========================================"
    log "Running Unit Tests"
    log "========================================"
    
    cd "$PROJECT_ROOT"
    
    log "Running tests with race detection and coverage..."
    if go test -v -race -coverprofile="$RESULTS_DIR/coverage.out" ./tests/unit/ 2>&1 | tee "$RESULTS_DIR/unit_tests.log"; then
        success "Unit tests passed"
        UNIT_PASS=1
        
        # Generate coverage report
        go tool cover -func="$RESULTS_DIR/coverage.out" > "$RESULTS_DIR/coverage_summary.txt"
        COVERAGE=$(grep "total:" "$RESULTS_DIR/coverage_summary.txt" | awk '{print $3}')
        log "Coverage: $COVERAGE"
        
        # Generate HTML coverage (optional, won't open automatically)
        go tool cover -html="$RESULTS_DIR/coverage.out" -o "$RESULTS_DIR/coverage.html"
    else
        error "Unit tests failed"
        UNIT_FAIL=1
    fi
    
    echo ""
}

# ====================
# 2. INTEGRATION TESTS
# ====================
run_integration_tests() {
    if [ "$SKIP_INTEGRATION" = true ]; then
        warn "Skipping integration tests"
        return
    fi

    log "========================================"
    log "Running Integration Tests ($MODE mode)"
    log "========================================"
    
    local integration_script=""
    case "$MODE" in
        local)
            integration_script="$SCRIPT_DIR/linux/integration/test-all-local.sh"
            ;;
        docker)
            integration_script="$SCRIPT_DIR/linux/integration/test-all-docker.sh"
            ;;
        vagrant)
            integration_script="$SCRIPT_DIR/linux/integration/test-all-vagrant.sh"
            ;;
        all)
            integration_script="$SCRIPT_DIR/linux/integration/test-all-environments.sh"
            ;;
        *)
            error "Unknown mode: $MODE"
            return 1
            ;;
    esac
    
    if [ ! -f "$integration_script" ]; then
        error "Integration script not found: $integration_script"
        INTEGRATION_FAIL=1
        return
    fi
    
    log "Running: $integration_script"
    if bash "$integration_script" 2>&1 | tee "$RESULTS_DIR/integration_tests.log"; then
        success "Integration tests passed"
        INTEGRATION_PASS=1
    else
        error "Integration tests failed"
        INTEGRATION_FAIL=1
    fi
    
    echo ""
}

# ====================
# 3. NETWORK TESTS
# ====================
run_network_tests() {
    if [ "$SKIP_NETWORK" = true ]; then
        warn "Skipping network tests"
        return
    fi

    log "========================================"
    log "Running Network Analysis Tests"
    log "========================================"
    
    # Check dependencies
    if ! command -v tcpdump &> /dev/null; then
        error "tcpdump not found. Install: sudo apt-get install tcpdump"
        NETWORK_FAIL=1
        return
    fi
    
    if ! command -v tshark &> /dev/null; then
        warn "tshark not found. Install for detailed analysis: sudo apt-get install tshark"
    fi
    
    # Run protocol compliance test
    log "Running protocol compliance test..."
    local protocol_test="$SCRIPT_DIR/linux/test-protocol-compliance.sh"
    
    if [ -f "$protocol_test" ]; then
        if bash "$protocol_test" "$MODE" 2>&1 | tee "$RESULTS_DIR/protocol_compliance.log"; then
            success "Protocol compliance test passed"
            NETWORK_PASS=$((NETWORK_PASS + 1))
        else
            error "Protocol compliance test failed"
            NETWORK_FAIL=$((NETWORK_FAIL + 1))
        fi
    else
        warn "Protocol compliance test not found: $protocol_test"
    fi
    
    # Run network analysis
    log "Running comprehensive network analysis..."
    local network_test="$SCRIPT_DIR/linux/test-network-analysis.sh"
    
    if [ -f "$network_test" ]; then
        if bash "$network_test" "$MODE" 2>&1 | tee "$RESULTS_DIR/network_analysis.log"; then
            success "Network analysis passed"
            NETWORK_PASS=$((NETWORK_PASS + 1))
        else
            error "Network analysis failed"
            NETWORK_FAIL=$((NETWORK_FAIL + 1))
        fi
    else
        warn "Network analysis test not found: $network_test"
    fi
    
    # Copy capture files if they exist
    if [ -d "/tmp/protocol_compliance_"* ]; then
        cp -r /tmp/protocol_compliance_* "$RESULTS_DIR/" 2>/dev/null || true
    fi
    
    echo ""
}

# ====================
# 4. BENCHMARKS
# ====================
run_benchmarks() {
    log "========================================"
    log "Running Benchmarks"
    log "========================================"
    
    cd "$PROJECT_ROOT"
    
    log "Running performance benchmarks..."
    if go test -bench=. -benchmem -benchtime=5s ./tests/unit/ 2>&1 | tee "$RESULTS_DIR/benchmarks.log"; then
        success "Benchmarks completed"
        
        # Extract key metrics
        grep "Benchmark" "$RESULTS_DIR/benchmarks.log" > "$RESULTS_DIR/benchmark_summary.txt" || true
    else
        warn "Benchmark run had issues"
    fi
    
    echo ""
}

# ====================
# GENERATE SUMMARY
# ====================
generate_summary() {
    log "========================================"
    log "Generating Test Summary"
    log "========================================"
    
    local summary_file="$RESULTS_DIR/TEST_SUMMARY.txt"
    
    cat > "$summary_file" << EOF
LogStream Test Execution Summary
=================================
Generated: $(date)
Mode: $MODE
Results Directory: $RESULTS_DIR

Test Results:
-------------
Unit Tests:        $([ $UNIT_PASS -eq 1 ] && echo "[OK] PASS" || echo "[X] FAIL")
Integration Tests: $([ $INTEGRATION_PASS -eq 1 ] && echo "[OK] PASS" || ([ $INTEGRATION_FAIL -eq 0 ] && echo "[SKIP] SKIPPED" || echo "[X] FAIL"))
Network Tests:     $([ $NETWORK_PASS -gt 0 ] && echo "[OK] PASS ($NETWORK_PASS)" || ([ $NETWORK_FAIL -eq 0 ] && echo "[SKIP] SKIPPED" || echo "[X] FAIL ($NETWORK_FAIL)"))

Files Generated:
----------------
EOF

    # List all files in results directory
    find "$RESULTS_DIR" -type f | while read file; do
        echo "- $(basename $file)" >> "$summary_file"
    done
    
    cat >> "$summary_file" << EOF

Coverage:
---------
EOF
    
    if [ -f "$RESULTS_DIR/coverage_summary.txt" ]; then
        grep "total:" "$RESULTS_DIR/coverage_summary.txt" >> "$summary_file" || echo "No coverage data" >> "$summary_file"
    else
        echo "No coverage data collected" >> "$summary_file"
    fi
    
    cat >> "$summary_file" << EOF

Benchmark Highlights:
---------------------
EOF
    
    if [ -f "$RESULTS_DIR/benchmark_summary.txt" ]; then
        head -n 10 "$RESULTS_DIR/benchmark_summary.txt" >> "$summary_file"
    else
        echo "No benchmark data collected" >> "$summary_file"
    fi
    
    cat >> "$summary_file" << EOF

Network Analysis:
-----------------
EOF
    
    if [ -f "$RESULTS_DIR/protocol_compliance.log" ]; then
        grep -E "(PASS|FAIL|packets)" "$RESULTS_DIR/protocol_compliance.log" | head -n 20 >> "$summary_file" || echo "See protocol_compliance.log" >> "$summary_file"
    else
        echo "No network analysis performed" >> "$summary_file"
    fi
    
    # Display summary
    cat "$summary_file"
    
    # Calculate overall status
    local overall_status="PASS"
    if [ $UNIT_FAIL -eq 1 ] || [ $INTEGRATION_FAIL -eq 1 ] || [ $NETWORK_FAIL -gt 0 ]; then
        overall_status="FAIL"
    fi
    
    echo ""
    if [ "$overall_status" = "PASS" ]; then
        success "========================================"
        success "All Tests Passed!"
        success "========================================"
    else
        error "========================================"
        error "Some Tests Failed - Check Logs"
        error "========================================"
    fi
    
    log "Results saved to: $RESULTS_DIR"
}

# ====================
# MAIN EXECUTION
# ====================
main() {
    # Check sudo if needed
    if [ "$SKIP_NETWORK" = false ]; then
        check_sudo
    fi
    
    # Run all test suites
    run_unit_tests
    run_integration_tests
    run_network_tests
    run_benchmarks
    
    # Generate summary
    generate_summary
    
    # Exit with appropriate code
    if [ $UNIT_FAIL -eq 1 ] || [ $INTEGRATION_FAIL -eq 1 ] || [ $NETWORK_FAIL -gt 0 ]; then
        exit 1
    fi
    
    exit 0
}

# Run main
main
