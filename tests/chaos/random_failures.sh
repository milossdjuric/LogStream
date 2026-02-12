#!/bin/bash
# Random Failure Injection - Continuously inject random failures
# Simulates real-world chaos in distributed system

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

DURATION="${1:-300}" # Default 5 minutes
NUM_BROKERS=5

echo "================================================"
echo "Random Failure Injection Test"
echo "================================================"
echo "Duration: ${DURATION}s"
echo "Brokers: $NUM_BROKERS"
echo ""

cd "$PROJECT_ROOT"
go build -o logstream main.go

# Start cluster
echo "Starting $NUM_BROKERS-node cluster..."
for i in $(seq 0 $((NUM_BROKERS-1))); do
    port=$((8001 + i))
    is_leader="false"
    [ $i -eq 0 ] && is_leader="true"
    
    NODE_ADDRESS="127.0.0.1:$port" IS_LEADER=$is_leader \
        MULTICAST_GROUP="239.0.0.1:9999" \
        ./logstream > /tmp/random-chaos-$i.log 2>&1 &
    
    echo $! >> /tmp/random-chaos-pids.txt
done

sleep 5
echo "Cluster started"
echo ""

# Failure injection function
inject_failure() {
    local failure_type=$1
    
    case $failure_type in
        "kill_broker")
            local broker_id=$((RANDOM % NUM_BROKERS))
            echo "[CHAOS] Killing broker $broker_id"
            # Find PID from line number in pids file
            local pid=$(sed -n "$((broker_id+1))p" /tmp/random-chaos-pids.txt)
            kill $pid 2>/dev/null || echo "  (broker already dead)"
            
            # Restart after random delay
            local restart_delay=$((5 + RANDOM % 20))
            sleep $restart_delay
            
            echo "[CHAOS] Restarting broker $broker_id after ${restart_delay}s"
            port=$((8001 + broker_id))
            is_leader="false"
            [ $broker_id -eq 0 ] && is_leader="true"
            
            NODE_ADDRESS="127.0.0.1:$port" IS_LEADER=$is_leader \
                MULTICAST_GROUP="239.0.0.1:9999" \
                ./logstream >> /tmp/random-chaos-$broker_id.log 2>&1 &
            
            new_pid=$!
            # Update PID file
            sed -i "$((broker_id+1))s/.*/$new_pid/" /tmp/random-chaos-pids.txt
            ;;
            
        "kill_leader")
            echo "[CHAOS] Killing current leader"
            # TODO: Identify and kill leader
            # For now, kill broker 0 (initial leader)
            local pid=$(sed -n "1p" /tmp/random-chaos-pids.txt)
            kill $pid 2>/dev/null || echo "  (leader already dead)"
            
            # Don't restart immediately - force election
            sleep 15
            
            echo "[CHAOS] Restarting ex-leader"
            NODE_ADDRESS="127.0.0.1:8001" IS_LEADER=false \
                MULTICAST_GROUP="239.0.0.1:9999" \
                ./logstream >> /tmp/random-chaos-0.log 2>&1 &
            
            new_pid=$!
            sed -i "1s/.*/$new_pid/" /tmp/random-chaos-pids.txt
            ;;
            
        "slow_gc")
            echo "[CHAOS] Triggering GC pause on random broker"
            local broker_id=$((RANDOM % NUM_BROKERS))
            # Simulate GC pause by sending SIGSTOP then SIGCONT
            local pid=$(sed -n "$((broker_id+1))p" /tmp/random-chaos-pids.txt)
            if kill -0 $pid 2>/dev/null; then
                kill -STOP $pid
                sleep $((1 + RANDOM % 5))
                kill -CONT $pid
                echo "  (GC pause simulated on broker $broker_id)"
            fi
            ;;
            
        "memory_pressure")
            echo "[CHAOS] Creating memory pressure"
            # Use stress-ng if available, otherwise simulate
            if command -v stress-ng &> /dev/null; then
                stress-ng --vm 1 --vm-bytes 75% --timeout 10s &
            else
                echo "  (stress-ng not available, skipping)"
            fi
            ;;
            
        "cpu_pressure")
            echo "[CHAOS] Creating CPU pressure"
            if command -v stress-ng &> /dev/null; then
                stress-ng --cpu 4 --timeout 15s &
            else
                echo "  (stress-ng not available, skipping)"
            fi
            ;;
            
        "multiple_kills")
            echo "[CHAOS] Killing multiple brokers (minority)"
            local kill_count=$((NUM_BROKERS / 2))
            for i in $(seq 1 $kill_count); do
                local broker_id=$((RANDOM % NUM_BROKERS))
                local pid=$(sed -n "$((broker_id+1))p" /tmp/random-chaos-pids.txt)
                kill $pid 2>/dev/null || echo "  (broker $broker_id already dead)"
            done
            
            # Restart all after delay
            sleep 20
            echo "[CHAOS] Restarting killed brokers"
            for i in $(seq 0 $((NUM_BROKERS-1))); do
                port=$((8001 + i))
                # Check if broker is running
                local pid=$(sed -n "$((i+1))p" /tmp/random-chaos-pids.txt)
                if ! kill -0 $pid 2>/dev/null; then
                    is_leader="false"
                    NODE_ADDRESS="127.0.0.1:$port" IS_LEADER=$is_leader \
                        MULTICAST_GROUP="239.0.0.1:9999" \
                        ./logstream >> /tmp/random-chaos-$i.log 2>&1 &
                    
                    new_pid=$!
                    sed -i "$((i+1))s/.*/$new_pid/" /tmp/random-chaos-pids.txt
                fi
            done
            ;;
    esac
}

# Failure types with probabilities
declare -A FAILURES
FAILURES["kill_broker"]=30      # 30% chance
FAILURES["kill_leader"]=10      # 10% chance
FAILURES["slow_gc"]=20          # 20% chance
FAILURES["memory_pressure"]=10  # 10% chance
FAILURES["cpu_pressure"]=10     # 10% chance
FAILURES["multiple_kills"]=5    # 5% chance

# Run chaos for specified duration
echo "Starting chaos injection for ${DURATION}s..."
echo "Press Ctrl+C to stop"
echo ""

start_time=$(date +%s)
failure_count=0

while [ $(($(date +%s) - start_time)) -lt $DURATION ]; do
    # Random wait between failures (10-30 seconds)
    sleep $((10 + RANDOM % 20))
    
    # Pick random failure
    rand=$((RANDOM % 100))
    cumulative=0
    
    for failure_type in "${!FAILURES[@]}"; do
        cumulative=$((cumulative + FAILURES[$failure_type]))
        if [ $rand -lt $cumulative ]; then
            inject_failure "$failure_type"
            failure_count=$((failure_count + 1))
            break
        fi
    done
    
    # Show progress
    elapsed=$(($(date +%s) - start_time))
    echo ""
    echo "[Progress] ${elapsed}s / ${DURATION}s - Total failures: $failure_count"
    echo ""
done

# Cleanup
echo ""
echo "Chaos test completed!"
echo "Total failures injected: $failure_count"
echo ""

echo "Stopping cluster..."
if [ -f /tmp/random-chaos-pids.txt ]; then
    while read pid; do
        kill $pid 2>/dev/null || true
    done < /tmp/random-chaos-pids.txt
    rm /tmp/random-chaos-pids.txt
fi

echo ""
echo "Logs saved to /tmp/random-chaos-*.log"
echo ""
echo "================================================"
echo "Random Failure Test Complete"
echo "================================================"
