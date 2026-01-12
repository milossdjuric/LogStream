#!/bin/bash

# Get absolute path (no tilde)
PROJECT_DIR="/home/milossdjuric/Documents/LogStream/Milos_LogStream/LogStream"

cd "$PROJECT_DIR"

# Verify binaries exist
if [ ! -f logstream ] || [ ! -f producer ] || [ ! -f consumer ]; then
    echo "ERROR: Binaries not found!"
    echo "Run: ./rebuild.sh"
    exit 1
fi

echo " All binaries ready"
echo "Starting ALL 8 tests in parallel..."
sleep 2

# Create tmux session
tmux new-session -d -s alltests

# Create grid
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

# Run tests
tmux select-pane -t 0
tmux send-keys "cd $PROJECT_DIR && clear && echo '=== SINGLE ===' && ./tests/linux/test-single.sh local" C-m

tmux select-pane -t 1
tmux send-keys "cd $PROJECT_DIR && clear && echo '=== DUO ===' && ./tests/linux/test-duo.sh local" C-m

tmux select-pane -t 2
tmux send-keys "cd $PROJECT_DIR && clear && echo '=== TRIO ===' && ./tests/linux/test-trio.sh local" C-m

tmux select-pane -t 3
tmux send-keys "cd $PROJECT_DIR && clear && echo '=== LATE JOINER ===' && ./tests/linux/test-late-joiner.sh local" C-m

tmux select-pane -t 4
tmux send-keys "cd $PROJECT_DIR && clear && echo '=== PRODUCER-CONSUMER ===' && ./tests/linux/test-producer-consumer.sh local" C-m

tmux select-pane -t 5
tmux send-keys "cd $PROJECT_DIR && clear && echo '=== MULTI-PRODUCER ===' && ./tests/linux/test-multi-producer.sh local" C-m

tmux select-pane -t 6
tmux send-keys "cd $PROJECT_DIR && clear && echo '=== E2E ===' && ./tests/linux/test-e2e.sh local" C-m

tmux select-pane -t 7
tmux send-keys "cd $PROJECT_DIR && clear && echo '=== SEQUENCE ===' && ./tests/linux/test-sequence.sh local" C-m

tmux attach-session -t alltests
