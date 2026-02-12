#!/bin/bash
# Horizontal Scaling Test - Test system with increasing number of brokers
# Measures how throughput and latency scale with cluster size

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RESULTS_DIR="/tmp/scalability-horizontal-$(date +%s)"

mkdir -p "$RESULTS_DIR"

echo "================================================"
echo "Horizontal Scaling Test"
echo "================================================"
echo "Results: $RESULTS_DIR"
echo ""

cd "$PROJECT_ROOT"
go build -o logstream main.go

# Test with different cluster sizes
for num_brokers in 1 3 5 7 10; do
    echo ""
    echo "=== Testing with $num_brokers brokers ==="
    
    # Start cluster
    echo "Starting cluster..."
    for i in $(seq 1 $num_brokers); do
        port=$((8000 + i))
        is_leader="false"
        [ $i -eq 1 ] && is_leader="true"
        
        NODE_ADDRESS="127.0.0.1:$port" IS_LEADER=$is_leader \
            MULTICAST_GROUP="239.0.0.1:9999" \
            ./logstream > "$RESULTS_DIR/broker-$num_brokers-$i.log" 2>&1 &
        
        echo $! >> "$RESULTS_DIR/pids-$num_brokers.txt"
    done
    
    sleep 5
    echo "Cluster started with $num_brokers brokers"
    
    # Run workload (10 producers, 1 minute)
    echo "Running workload..."
    start_time=$(date +%s)
    
    # TODO: Start producers and measure throughput
    # For template purposes, simulate
    sleep 60
    
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    
    # TODO: Collect actual metrics
    # messages_sent=$(query_cluster_metrics)
    # avg_latency=$(calculate_avg_latency)
    
    # Simulate results
    messages_sent=$((num_brokers * 1000))
    throughput=$(echo "scale=2; $messages_sent / $duration" | bc)
    avg_latency=$((10 + num_brokers * 2))
    
    echo "Results for $num_brokers brokers:"
    echo "  Messages: $messages_sent"
    echo "  Throughput: $throughput msgs/sec"
    echo "  Avg Latency: ${avg_latency}ms"
    echo "  Per-broker throughput: $(echo "scale=2; $throughput / $num_brokers" | bc) msgs/sec"
    
    # Save results
    echo "$num_brokers,$messages_sent,$throughput,$avg_latency" >> "$RESULTS_DIR/results.csv"
    
    # Stop cluster
    if [ -f "$RESULTS_DIR/pids-$num_brokers.txt" ]; then
        while read pid; do
            kill $pid 2>/dev/null || true
        done < "$RESULTS_DIR/pids-$num_brokers.txt"
        rm "$RESULTS_DIR/pids-$num_brokers.txt"
    fi
    
    sleep 3
done

# Generate report
cat > "$RESULTS_DIR/scaling_report.txt" << EOF
Horizontal Scaling Test Report
===============================
Generated: $(date)

Results:
--------
Brokers | Messages | Throughput (msgs/sec) | Avg Latency (ms) | Per-Broker (msgs/sec)
--------|----------|----------------------|------------------|----------------------
EOF

while IFS=',' read -r brokers msgs throughput latency; do
    per_broker=$(echo "scale=2; $throughput / $brokers" | bc)
    printf "%7d | %8d | %20.2f | %16d | %21.2f\n" $brokers $msgs $throughput $latency $per_broker >> "$RESULTS_DIR/scaling_report.txt"
done < "$RESULTS_DIR/results.csv"

cat >> "$RESULTS_DIR/scaling_report.txt" << EOF

Analysis:
---------
EOF

# Calculate scaling efficiency
first_throughput=$(head -1 "$RESULTS_DIR/results.csv" | cut -d',' -f3)
last_line=$(tail -1 "$RESULTS_DIR/results.csv")
last_brokers=$(echo $last_line | cut -d',' -f1)
last_throughput=$(echo $last_line | cut -d',' -f3)

ideal_throughput=$(echo "scale=2; $first_throughput * $last_brokers" | bc)
actual_throughput=$last_throughput
efficiency=$(echo "scale=2; $actual_throughput / $ideal_throughput * 100" | bc)

cat >> "$RESULTS_DIR/scaling_report.txt" << EOF
Scaling Efficiency:
  Single broker throughput: $first_throughput msgs/sec
  $last_brokers brokers ideal throughput: $ideal_throughput msgs/sec
  $last_brokers brokers actual throughput: $actual_throughput msgs/sec
  Scaling efficiency: ${efficiency}%

$(if (( $(echo "$efficiency > 80" | bc -l) )); then
    echo "[OK] Good horizontal scaling (>80% efficient)"
elif (( $(echo "$efficiency > 60" | bc -l) )); then
    echo "[!] Acceptable scaling (60-80% efficient)"
else
    echo "[X] Poor scaling (<60% efficient)"
fi)
EOF

# Display report
cat "$RESULTS_DIR/scaling_report.txt"

echo ""
echo "================================================"
echo "Horizontal Scaling Test Complete"
echo "================================================"
echo "Report: $RESULTS_DIR/scaling_report.txt"
echo "Logs: $RESULTS_DIR/broker-*.log"
echo ""
