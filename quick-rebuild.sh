#!/bin/bash
# Quick rebuild script for LogStream

set -e  # Exit on error

echo "🔨 LogStream Quick Rebuild"
echo "=========================="
echo ""

# Clean old binaries
echo "Cleaning old binaries..."
rm -f logstream producer consumer

# Clean caches
echo "Cleaning Go caches..."
go clean -cache -testcache

# Build main executable
echo ""
echo "Building logstream..."
go build -v -o logstream main.go

# Build producer
echo ""
echo "Building producer..."
go build -v -o producer cmd/producer/main.go

# Build consumer
echo ""
echo "Building consumer..."
go build -v -o consumer cmd/consumer/main.go

# Verify
echo ""
echo "✅ Build Complete!"
echo "=================="
ls -lh logstream producer consumer

echo ""
echo "🎯 Next steps:"
echo "  ./logstream --address 127.0.0.1:8001"
echo "  ./producer -leader 127.0.0.1:8001 -topic logs"
echo "  ./consumer -leader 127.0.0.1:8001 -topic logs"
