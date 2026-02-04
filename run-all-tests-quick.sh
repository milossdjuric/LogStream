#!/bin/bash
# Quick test runner for LogStream

echo "🧪 LogStream Quick Test Suite"
echo "=============================="
echo ""

# Unit tests
echo "1️⃣ Running unit tests..."
go test -v -cover ./tests/unit || echo "⚠️ Some unit tests failed"

echo ""
echo "2️⃣ Running integration tests (Go)..."
go test -v -timeout 10m ./tests/integration || echo "⚠️ Some integration tests failed"

echo ""
echo "3️⃣ Running bash integration tests (local mode)..."
echo "   (This requires sudo - run manually if needed)"
echo "   Command: sudo ./tests/run-all-tests.sh local"

echo ""
echo "✅ Quick test suite complete!"
echo ""
echo "📊 For full test suite:"
echo "  sudo ./tests/run-all-modes.sh"
