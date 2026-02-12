#!/bin/bash
# Run ALL integration tests (Go + Bash)

set -e

echo "🧪 Running ALL Integration Tests (Go + Bash)"
echo "============================================="
echo ""

# 1. Build binary
echo "🔨 Step 1: Building logstream binary..."
go build -o logstream main.go
echo "✅ Binary built"
echo ""

# 2. Run Go integration tests
echo "🧪 Step 2: Running Go integration tests..."
if go test -v -timeout 15m ./tests/integration 2>&1 | tee go-integration-tests.log; then
    echo "✅ Go integration tests passed"
    GO_RESULT="✅ PASS"
else
    echo "⚠️ Some Go integration tests failed"
    GO_RESULT="⚠️ FAIL"
fi
echo ""

# 3. Run bash integration tests
echo "🧪 Step 3: Running bash integration tests (local mode)..."
if sudo ./tests/run-all-tests.sh local --skip-unit 2>&1 | tee bash-integration-tests.log; then
    echo "✅ Bash integration tests passed"
    BASH_RESULT="✅ PASS"
else
    echo "⚠️ Some bash integration tests failed"
    BASH_RESULT="⚠️ FAIL"
fi
echo ""

# Summary
echo "📊 Test Summary"
echo "==============="
echo "Go Integration Tests:   $GO_RESULT"
echo "Bash Integration Tests: $BASH_RESULT"
echo ""
echo "📁 Logs saved:"
echo "  - go-integration-tests.log"
echo "  - bash-integration-tests.log"
