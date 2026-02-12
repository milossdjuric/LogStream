#!/bin/bash
# Network Analysis Test - Comprehensive network testing with packet capture
# Tests network protocols, message patterns, and generates Wireshark-compatible captures
# Usage: ./test-network-analysis.sh [local|docker|vagrant]

set -e

MODE="${1:-local}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR/../.."
TEST_PREFIX="[NETWORK-TEST]"

source "$SCRIPT_DIR/lib/common.sh"

CAPTURE_DIR="/tmp/logstream_captures_$(date +%s)"
CAPTURE_FILE="$CAPTURE_DIR/logstream_network.pcap"
ANALYSIS_DIR="$CAPTURE_DIR/analysis"

log "$TEST_PREFIX Starting Network Analysis Test (Mode: $MODE)"
log "$TEST_PREFIX Capture directory: $CAPTURE_DIR"

mkdir -p "$CAPTURE_DIR"
mkdir -p "$ANALYSIS_DIR"

check_dependencies() {
    log "$TEST_PREFIX Checking dependencies..."
    
    if ! command -v tcpdump &> /dev/null; then
        error_msg "tcpdump not found. Install with: sudo apt-get install tcpdump"
        exit 1
    fi
    
    if ! command -v tshark &> /dev/null; then
        warn_msg "tshark not found. Install for advanced analysis: sudo apt-get install tshark"
    fi
    
    if ! command -v wireshark &> /dev/null; then
        warn_msg "wireshark not found. Install for GUI analysis: sudo apt-get install wireshark"
    fi
    
    success_test "Dependencies checked"
}

start_packet_capture() {
    local interface="$1"
    local filter="$2"
    
    log "$TEST_PREFIX Starting packet capture on $interface"
    log "$TEST_PREFIX Filter: $filter"
    
    sudo tcpdump -i "$interface" -w "$CAPTURE_FILE" "$filter" &
    TCPDUMP_PID=$!
    
    sleep 2
    
    if ! ps -p $TCPDUMP_PID > /dev/null; then
        error_msg "Failed to start tcpdump"
        exit 1
    fi
    
    success_test "Packet capture started (PID: $TCPDUMP_PID)"
}

stop_packet_capture() {
    log "$TEST_PREFIX Stopping packet capture..."
    
    if [ ! -z "$TCPDUMP_PID" ]; then
        sudo kill -SIGINT $TCPDUMP_PID 2>/dev/null || true
        wait $TCPDUMP_PID 2>/dev/null || true
    fi
    
    if [ -f "$CAPTURE_FILE" ]; then
        local size=$(du -h "$CAPTURE_FILE" | cut -f1)
        success_test "Packet capture stopped (Size: $size)"
    else
        warn_msg "No capture file generated"
    fi
}

analyze_capture() {
    log "$TEST_PREFIX Analyzing captured packets..."
    
    if [ ! -f "$CAPTURE_FILE" ]; then
        warn_msg "No capture file to analyze"
        return
    fi
    
    local packet_count=$(sudo tcpdump -r "$CAPTURE_FILE" 2>/dev/null | wc -l)
    log "$TEST_PREFIX Total packets captured: $packet_count"
    
    if command -v tshark &> /dev/null; then
        log "$TEST_PREFIX Generating TShark analysis..."
        
        log "$TEST_PREFIX === Protocol Hierarchy ==="
        sudo tshark -r "$CAPTURE_FILE" -q -z io,phs | tee "$ANALYSIS_DIR/protocol_hierarchy.txt"
        
        log "$TEST_PREFIX === IO Statistics (1 second intervals) ==="
        sudo tshark -r "$CAPTURE_FILE" -q -z io,stat,1 | tee "$ANALYSIS_DIR/io_stats.txt"
        
        log "$TEST_PREFIX === UDP Conversations ==="
        sudo tshark -r "$CAPTURE_FILE" -q -z conv,udp | tee "$ANALYSIS_DIR/udp_conversations.txt"
        
        log "$TEST_PREFIX === TCP Conversations ==="
        sudo tshark -r "$CAPTURE_FILE" -q -z conv,tcp | tee "$ANALYSIS_DIR/tcp_conversations.txt"
        
        log "$TEST_PREFIX === UDP Endpoints ==="
        sudo tshark -r "$CAPTURE_FILE" -q -z endpoints,udp | tee "$ANALYSIS_DIR/udp_endpoints.txt"
        
        log "$TEST_PREFIX === TCP Endpoints ==="
        sudo tshark -r "$CAPTURE_FILE" -q -z endpoints,tcp | tee "$ANALYSIS_DIR/tcp_endpoints.txt"
        
        log "$TEST_PREFIX Extracting LogStream messages..."
        sudo tshark -r "$CAPTURE_FILE" -Y "udp.port == 9999" -T fields -e frame.number -e frame.time_relative -e ip.src -e ip.dst -e data.text 2>/dev/null | head -20 | tee "$ANALYSIS_DIR/multicast_messages.txt"
        
        log "$TEST_PREFIX Exporting to CSV..."
        sudo tshark -r "$CAPTURE_FILE" -T fields \
            -e frame.number \
            -e frame.time_relative \
            -e ip.src \
            -e ip.dst \
            -e udp.srcport \
            -e udp.dstport \
            -e tcp.srcport \
            -e tcp.dstport \
            -e frame.len \
            -E header=y \
            -E separator=, \
            2>/dev/null > "$ANALYSIS_DIR/packets.csv"
        
        success_test "TShark analysis complete"
    fi
}

run_local_test() {
    log "$TEST_PREFIX Running LOCAL mode test"
    
    cd "$PROJECT_ROOT"
    go build -o logstream main.go || { error_msg "Build failed"; exit 1; }
    
    local MULTICAST_GROUP="239.0.0.1:9999"
    local BROADCAST_PORT="8888"
    
    start_packet_capture "lo" "(udp port 9999 or tcp port 8001 or udp port 8888)"
    
    # Use unique log file names to avoid permission issues
    LEADER_LOG="$CAPTURE_DIR/leader.log"
    FOLLOWER_LOG="$CAPTURE_DIR/follower.log"
    
    log "$TEST_PREFIX Starting leader node..."
    NODE_ADDRESS="127.0.0.1:8001" IS_LEADER=true MULTICAST_GROUP="$MULTICAST_GROUP" BROADCAST_PORT="$BROADCAST_PORT" ./logstream > "$LEADER_LOG" 2>&1 &
    LEADER_PID=$!
    sleep 3
    
    log "$TEST_PREFIX Starting follower node..."
    NODE_ADDRESS="127.0.0.1:8002" MULTICAST_GROUP="$MULTICAST_GROUP" BROADCAST_PORT="$BROADCAST_PORT" ./logstream > "$FOLLOWER_LOG" 2>&1 &
    FOLLOWER_PID=$!
    sleep 3
    
    log "$TEST_PREFIX Letting cluster run for 30 seconds..."
    sleep 30
    
    log "$TEST_PREFIX Stopping nodes..."
    kill $LEADER_PID $FOLLOWER_PID 2>/dev/null || true
    wait $LEADER_PID $FOLLOWER_PID 2>/dev/null || true
    
    stop_packet_capture
}

run_docker_test() {
    log "$TEST_PREFIX Running DOCKER mode test"
    
    cd "$PROJECT_ROOT/deploy/docker"
    
    local interface=$(ip route | grep default | awk '{print $5}')
    start_packet_capture "$interface" "(udp port 9999 or tcp port 8001 or udp port 8888)"
    
    log "$TEST_PREFIX Starting Docker cluster..."
    ./start-cluster.sh || { error_msg "Docker start failed"; exit 1; }
    
    log "$TEST_PREFIX Letting cluster run for 30 seconds..."
    sleep 30
    
    log "$TEST_PREFIX Stopping Docker cluster..."
    ./stop-cluster.sh || true
    
    stop_packet_capture
}

run_vagrant_test() {
    log "$TEST_PREFIX Running VAGRANT mode test"
    
    cd "$PROJECT_ROOT/deploy/vagrant"
    
    local interface=$(ip route | grep default | awk '{print $5}')
    start_packet_capture "$interface" "(udp port 9999 or tcp port 8001 or udp port 8888)"
    
    log "$TEST_PREFIX Starting Vagrant cluster..."
    ./start-cluster.sh || { error_msg "Vagrant start failed"; exit 1; }
    
    log "$TEST_PREFIX Letting cluster run for 30 seconds..."
    sleep 30
    
    log "$TEST_PREFIX Stopping Vagrant cluster..."
    ./stop-cluster.sh || true
    
    stop_packet_capture
}

main() {
    check_dependencies
    
    case "$MODE" in
        local)
            run_local_test
            ;;
        docker)
            run_docker_test
            ;;
        vagrant)
            run_vagrant_test
            ;;
        *)
            error_msg "Invalid mode: $MODE (use 'local', 'docker', or 'vagrant')"
            exit 1
            ;;
    esac
    
    analyze_capture

    log "$TEST_PREFIX =========================================="
    log "$TEST_PREFIX Network Analysis Test Complete!"
    log "$TEST_PREFIX =========================================="
    log "$TEST_PREFIX Capture file: $CAPTURE_FILE"
    log "$TEST_PREFIX Analysis directory: $ANALYSIS_DIR"
    log "$TEST_PREFIX"
    log "$TEST_PREFIX To open in Wireshark:"
    log "$TEST_PREFIX   wireshark $CAPTURE_FILE"
}

trap 'stop_packet_capture; kill $LEADER_PID $FOLLOWER_PID 2>/dev/null || true' EXIT

main
