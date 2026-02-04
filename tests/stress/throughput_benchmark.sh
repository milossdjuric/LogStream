#!/bin/bash
# Throughput Benchmark - Measure maximum message throughput
# Tests various configurations to find optimal throughput

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RESULTS_DIR="/tmp/throughput-benchmark-$(date +%s)"

mkdir -p "$RESULTS_DIR"

echo "==============================================="
echo "LogStream Throughput Benchmark"
echo "==============================================="
echo "Results will be saved to: $RESULTS_DIR"
echo ""

# Build binary
cd "$PROJECT_ROOT"
echo "Building LogStream..."
go build -o logstream main.go || { echo "Build failed"; exit 1; }

# Function to measure throughput
measure_throughput() {
    local num_producers=$1
    local num_brokers=$2
    local duration=$3
    local msg_size=$4
    
    echo ""
    echo "--- Configuration: $num_brokers brokers, $num_producers producers, ${duration}s, ${msg_size}B msgs ---"
    
    # Start cluster
    for i in $(seq 1 $num_brokers); do
        local port=$((8000 + i))
        local is_leader="false"
        [ $i -eq 1 ] && is_leader="true"
        
        NODE_ADDRESS="127.0.0.1:$port" IS_LEADER=$is_leader \
            MULTICAST_GROUP="239.0.0.1:9999" \
            ./logstream > "$RESULTS_DIR/broker-$i.log" 2>&1 &
        
        echo $! >> "$RESULTS_DIR/pids.txt"
    done
    
    sleep 3
    echo "Cluster started with $num_brokers brokers"
    
    # Start producers and count messages
    local total_sent=0
    for i in $(seq 1 $num_producers); do
        # TODO: Start producer and send messages at max rate
        # This is a template - actual implementation depends on your client API
        # producer --topic "bench-$i" --rate max --duration $duration --msg-size $msg_size &
        echo $! >> "$RESULTS_DIR/pids.txt"
    done
    
    echo "Started $num_producers producers"
    
    # Wait for test duration
    sleep $duration
    
    # Stop producers
    # pkill -f "producer"
    
    # Collect metrics
    sleep 2
    
    # TODO: Query broker for actual message count
    # For now, estimate based on logs
    total_sent=$(grep -r "Message sent" "$RESULTS_DIR"/*.log | wc -l || echo "0")
    
    # Calculate throughput
    local throughput=$(echo "scale=2; $total_sent / $duration" | bc)
    local throughput_per_broker=$(echo "scale=2; $throughput / $num_brokers" | bc)
    
    echo "Results:"
    echo "  Total messages: $total_sent"
    echo "  Duration: ${duration}s"
    echo "  Throughput: $throughput msgs/sec"
    echo "  Per broker: $throughput_per_broker msgs/sec"
    
    # Save results
    echo "$num_brokers,$num_producers,$duration,$msg_size,$total_sent,$throughput" >> "$RESULTS_DIR/results.csv"
    
    # Stop cluster
    if [ -f "$RESULTS_DIR/pids.txt" ]; then
        while read pid; do
            kill $pid 2>/dev/null || true
        done < "$RESULTS_DIR/pids.txt"
        rm "$RESULTS_DIR/pids.txt"
    fi
    
    sleep 2
}

# Initialize results file
echo "brokers,producers,duration,msg_size,total_msgs,throughput" > "$RESULTS_DIR/results.csv"

# Test configurations
echo ""
echo "Running throughput benchmarks..."
echo ""

# Baseline: 1 broker, varying producers
measure_throughput 1 1 30 1024
measure_throughput 5 1 30 1024
measure_throughput 10 1 30 1024
measure_throughput 20 1 30 1024

# Scaling brokers: 10 producers, varying brokers
measure_throughput 10 1 30 1024
measure_throughput 10 3 30 1024
measure_throughput 10 5 30 1024

# Message size impact: 3 brokers, 10 producers
measure_throughput 10 3 30 128
measure_throughput 10 3 30 1024
measure_throughput 10 3 30 10240

# High load test: max configuration
measure_throughput 50 5 60 1024

# Generate report
cat > "$RESULTS_DIR/benchmark_report.txt" << EOF
LogStream Throughput Benchmark Report
======================================
Generated: $(date)

Configuration Tests:
--------------------
$(cat "$RESULTS_DIR/results.csv" | column -t -s,)

Analysis:
---------
EOF

# Find best configuration
best_throughput=$(awk -F',' 'NR>1 {print $6}' "$RESULTS_DIR/results.csv" | sort -n | tail -1)
best_config=$(awk -F',' -v bt="$best_throughput" '$6 == bt {print "Brokers: "$1", Producers: "$2", Throughput: "$6" msgs/sec"}' "$RESULTS_DIR/results.csv")

echo "Best Configuration: $best_config" >> "$RESULTS_DIR/benchmark_report.txt"
echo "" >> "$RESULTS_DIR/benchmark_report.txt"

# Scaling analysis
echo "Broker Scaling:" >> "$RESULTS_DIR/benchmark_report.txt"
grep "^.,10," "$RESULTS_DIR/results.csv" | awk -F',' '{printf "  %d brokers: %.2f msgs/sec (%.2f per broker)\n", $1, $6, $6/$1}' >> "$RESULTS_DIR/benchmark_report.txt"
echo "" >> "$RESULTS_DIR/benchmark_report.txt"

echo "Producer Scaling:" >> "$RESULTS_DIR/benchmark_report.txt"
grep "^1,." "$RESULTS_DIR/results.csv" | awk -F',' '{printf "  %d producers: %.2f msgs/sec (%.2f per producer)\n", $2, $6, $6/$2}' >> "$RESULTS_DIR/benchmark_report.txt"
echo "" >> "$RESULTS_DIR/benchmark_report.txt"

# Display report
cat "$RESULTS_DIR/benchmark_report.txt"

echo ""
echo "==============================================="
echo "Benchmark complete!"
echo "Results saved to: $RESULTS_DIR"
echo "  - results.csv: Raw data"
echo "  - benchmark_report.txt: Analysis"
echo "  - broker-*.log: Broker logs"
echo "==============================================="
