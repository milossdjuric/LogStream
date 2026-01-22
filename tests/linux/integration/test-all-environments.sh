#!/bin/bash

set -e

echo "========================================"
echo "Testing All Environments"
echo "========================================"
echo ""

PASSED=0
FAILED=0
TOTAL=0

# Test Local
echo "-> Testing LOCAL environment..."
if ../linux/test-single.sh local > /dev/null 2>&1; then
    echo "  [OK] Local works"
    ((PASSED++))
else
    echo "  [X] Local failed"
    ((FAILED++))
fi
((TOTAL++))

# Test Docker
echo "-> Testing DOCKER environment..."
if ../linux/test-single.sh docker > /dev/null 2>&1; then
    echo "  [OK] Docker works"
    ((PASSED++))
else
    echo "  [X] Docker failed"
    ((FAILED++))
fi
((TOTAL++))

# Test Vagrant (if available)
if command -v vagrant &> /dev/null; then
    echo "-> Testing VAGRANT environment..."
    cd ../../deploy/vagrant
    if vagrant status | grep -q "running"; then
        cd ../../tests/linux
        if ./test-single.sh vagrant > /dev/null 2>&1; then
            echo "  [OK] Vagrant works"
            ((PASSED++))
        else
            echo "  [X] Vagrant failed"
            ((FAILED++))
        fi
        ((TOTAL++))
    else
        echo "  ⊘ Vagrant VMs not running (skipped)"
    fi
else
    echo "  ⊘ Vagrant not installed (skipped)"
fi

echo ""
echo "========================================"
echo "Summary: $PASSED/$TOTAL environments work"
echo "========================================"

[ $FAILED -eq 0 ]