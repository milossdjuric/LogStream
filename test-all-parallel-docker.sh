#!/bin/bash

# Docker Parallel Test Runner - All 8 Tests
# Usage: ./test-all-parallel-docker.sh

PROJECT_DIR="/home/milossdjuric/Documents/LogStream/Milos_LogStream/LogStream"

cd "$PROJECT_DIR"

echo "================================================"
echo "Docker Parallel Test Runner"
echo "Starting ALL 8 tests in Docker mode..."
echo "================================================"
echo ""
echo "This will start 8 separate Docker Compose stacks:"
echo "  1. Single broker"
echo "  2. Duo (2 brokers)"
echo "  3. Trio (3 brokers)"
echo "  4. Late joiner"
echo "  5. Producer-Consumer"
echo "  6. Multi-Producer (3 producers)"
echo "  7. End-to-End"
echo "  8. Sequence Demo"
echo ""
echo "Each test runs in its own Docker network to avoid conflicts."
echo ""
sleep 3

# Create tmux session
tmux new-session -d -s dockertests

# Create 3x3 grid (8 panes)
tmux split-window -h
tmux split-window -h
tmux select-layout even-horizontal
tmux select-pane -t 0
tmux split-window -v
tmux select-pane -t 2
tmux split-window -v
tmux select-pane -t 4
tmux split-window -v
tmux select-pane -t 1
tmux split-window -v
tmux select-pane -t 4
tmux split-window -v
tmux select-layout tiled

# Run Docker tests in each pane
tmux select-pane -t 0
tmux send-keys "cd $PROJECT_DIR && clear && echo '=== SINGLE (Docker) ===' && ./tests/linux/test-single.sh docker" C-m

tmux select-pane -t 1
tmux send-keys "cd $PROJECT_DIR && clear && echo '=== DUO (Docker) ===' && ./tests/linux/test-duo.sh docker" C-m

tmux select-pane -t 2
tmux send-keys "cd $PROJECT_DIR && clear && echo '=== TRIO (Docker) ===' && ./tests/linux/test-trio.sh docker" C-m

tmux select-pane -t 3
tmux send-keys "cd $PROJECT_DIR && clear && echo '=== LATE JOINER (Docker) ===' && ./tests/linux/test-late-joiner.sh docker" C-m

tmux select-pane -t 4
tmux send-keys "cd $PROJECT_DIR && clear && echo '=== PRODUCER-CONSUMER (Docker) ===' && ./tests/linux/test-producer-consumer.sh docker" C-m

tmux select-pane -t 5
tmux send-keys "cd $PROJECT_DIR && clear && echo '=== MULTI-PRODUCER (Docker) ===' && ./tests/linux/test-multi-producer.sh docker" C-m

tmux select-pane -t 6
tmux send-keys "cd $PROJECT_DIR && clear && echo '=== E2E (Docker) ===' && ./tests/linux/test-e2e.sh docker" C-m

tmux select-pane -t 7
tmux send-keys "cd $PROJECT_DIR && clear && echo '=== SEQUENCE (Docker) ===' && ./tests/linux/test-sequence.sh docker" C-m

echo "Attaching to tmux session..."
sleep 2
tmux attach-session -t dockertests
