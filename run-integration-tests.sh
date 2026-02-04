#!/bin/bash
# Run Go integration tests (builds binary first)

set -e

echo "🔨 Building logstream binary..."
go build -o logstream main.go

echo ""
echo "🧪 Running integration tests..."
go test -v -timeout 15m ./tests/integration

echo ""
echo "✅ Integration tests complete!"
