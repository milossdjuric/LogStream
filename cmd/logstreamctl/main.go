package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"text/tabwriter"
	"time"
)

const (
	stateDir     = ".logstream"
	pidsDir      = "pids"
	logsDir      = "logs"
	stateFile    = "processes.json"
	version      = "1.0.0"
)

// ProcessInfo stores metadata about a managed process
type ProcessInfo struct {
	Name      string    `json:"name"`
	Type      string    `json:"type"` // broker, producer, consumer
	PID       int       `json:"pid"`
	Address   string    `json:"address,omitempty"`   // For brokers
	Leader    string    `json:"leader,omitempty"`    // For clients
	Topic     string    `json:"topic,omitempty"`     // For clients
	StartedAt time.Time `json:"started_at"`
	Args      []string  `json:"args"` // Original arguments
}

// State holds all managed processes
type State struct {
	Processes map[string]*ProcessInfo `json:"processes"`
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]

	switch cmd {
	case "start":
		if len(os.Args) < 3 {
			fmt.Println("Usage: logstreamctl start <broker|producer|consumer> [options]")
			os.Exit(1)
		}
		handleStart(os.Args[2], os.Args[3:])

	case "stop":
		handleStop(os.Args[2:])

	case "list":
		handleList()

	case "logs":
		handleLogs(os.Args[2:])

	case "status":
		handleStatus(os.Args[2:])

	case "election":
		handleElection(os.Args[2:])

	case "config":
		if len(os.Args) < 3 {
			fmt.Println("Usage: logstreamctl config <init|show>")
			os.Exit(1)
		}
		handleConfig(os.Args[2])

	case "help", "--help", "-h":
		printUsage()

	case "version", "--version", "-v":
		fmt.Printf("logstreamctl version %s\n", version)

	default:
		fmt.Printf("Unknown command: %s\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("logstreamctl - LogStream Cluster Management Tool")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  logstreamctl <command> [options]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  start broker   Start a broker node")
	fmt.Println("  start producer Start a producer client")
	fmt.Println("  start consumer Start a consumer client")
	fmt.Println("  stop <name>    Stop a process by name")
	fmt.Println("  stop --all     Stop all managed processes")
	fmt.Println("  list           List all managed processes")
	fmt.Println("  logs <name>    View logs for a process")
	fmt.Println("  status         Show status of local processes")
	fmt.Println("  election <name> Trigger election on a broker")
	fmt.Println("  config init    Initialize state directory")
	fmt.Println("  config show    Show state directory path")
	fmt.Println("  help           Show this help")
	fmt.Println("  version        Show version")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  # Initialize (first time)")
	fmt.Println("  logstreamctl config init")
	fmt.Println()
	fmt.Println("  # Start a broker")
	fmt.Println("  logstreamctl start broker --name node1 --address 192.168.1.10:8001")
	fmt.Println()
	fmt.Println("  # Start a producer")
	fmt.Println("  logstreamctl start producer --name prod1 --leader 192.168.1.10:8001 --topic logs")
	fmt.Println()
	fmt.Println("  # Start a consumer")
	fmt.Println("  logstreamctl start consumer --name cons1 --leader 192.168.1.10:8001 --topic logs")
	fmt.Println()
	fmt.Println("  # List running processes")
	fmt.Println("  logstreamctl list")
	fmt.Println()
	fmt.Println("  # Stop a process")
	fmt.Println("  logstreamctl stop node1")
	fmt.Println()
	fmt.Println("  # View logs")
	fmt.Println("  logstreamctl logs node1 --follow")
}

// =============================================================================
// START COMMAND
// =============================================================================

func handleStart(processType string, args []string) {
	switch processType {
	case "broker":
		startBroker(args)
	case "producer":
		startProducer(args)
	case "consumer":
		startConsumer(args)
	default:
		fmt.Printf("Unknown process type: %s\n", processType)
		fmt.Println("Valid types: broker, producer, consumer")
		os.Exit(1)
	}
}

func startBroker(args []string) {
	// Parse flags
	name := ""
	address := ""
	multicast := "239.0.0.1:9999"
	broadcastPort := "8888"
	background := false

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--name", "-n":
			if i+1 < len(args) {
				name = args[i+1]
				i++
			}
		case "--address", "-a":
			if i+1 < len(args) {
				address = args[i+1]
				i++
			}
		case "--multicast", "-m":
			if i+1 < len(args) {
				multicast = args[i+1]
				i++
			}
		case "--broadcast-port", "-b":
			if i+1 < len(args) {
				broadcastPort = args[i+1]
				i++
			}
		case "--background", "-bg":
			background = true
		case "--help", "-h":
			fmt.Println("Start a broker node")
			fmt.Println()
			fmt.Println("Usage:")
			fmt.Println("  logstreamctl start broker [options]")
			fmt.Println()
			fmt.Println("Options:")
			fmt.Println("  --name, -n           Process name (required)")
			fmt.Println("  --address, -a        Node address IP:PORT (required)")
			fmt.Println("  --multicast, -m      Multicast group (default: 239.0.0.1:9999)")
			fmt.Println("  --broadcast-port, -b Broadcast port (default: 8888)")
			fmt.Println("  --background, -bg    Run in background (default: foreground)")
			os.Exit(0)
		}
	}

	if name == "" {
		fmt.Println("Error: --name is required")
		os.Exit(1)
	}
	if address == "" {
		fmt.Println("Error: --address is required")
		os.Exit(1)
	}

	// Check if name already exists
	state := loadState()
	if _, exists := state.Processes[name]; exists {
		if isProcessRunning(state.Processes[name].PID) {
			fmt.Printf("Error: process '%s' is already running (PID %d)\n", name, state.Processes[name].PID)
			os.Exit(1)
		}
		// Process exists but not running, clean it up
		delete(state.Processes, name)
	}

	// Find the logstream binary
	binary := findBinary("logstream")
	if binary == "" {
		fmt.Println("Error: 'logstream' binary not found")
		fmt.Println("Build it with: go build -o logstream main.go")
		os.Exit(1)
	}

	// Prepare environment
	env := os.Environ()
	env = append(env, fmt.Sprintf("NODE_ADDRESS=%s", address))
	env = append(env, fmt.Sprintf("MULTICAST_GROUP=%s", multicast))
	env = append(env, fmt.Sprintf("BROADCAST_PORT=%s", broadcastPort))

	if background {
		// Run in background
		logFile := getLogPath(name)

		lf, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			fmt.Printf("Error creating log file: %v\n", err)
			os.Exit(1)
		}

		cmd := exec.Command(binary)
		cmd.Env = env
		cmd.Stdout = lf
		cmd.Stderr = lf
		cmd.Stdin = nil
		// Set process group so it survives parent exit
		cmd.SysProcAttr = &syscall.SysProcAttr{
			Setpgid: true,
		}

		if err := cmd.Start(); err != nil {
			lf.Close()
			fmt.Printf("Error starting broker: %v\n", err)
			os.Exit(1)
		}

		lf.Close()

		// Save process info
		info := &ProcessInfo{
			Name:      name,
			Type:      "broker",
			PID:       cmd.Process.Pid,
			Address:   address,
			StartedAt: time.Now(),
			Args:      args,
		}
		state.Processes[name] = info
		saveState(state)

		// Write PID file
		writePidFile(name, cmd.Process.Pid)

		fmt.Printf("Started broker '%s' (PID %d)\n", name, cmd.Process.Pid)
		fmt.Printf("  Address:   %s\n", address)
		fmt.Printf("  Multicast: %s\n", multicast)
		fmt.Printf("  Logs:      %s\n", logFile)
	} else {
		// Run in foreground (default)
		cmd := exec.Command(binary)
		cmd.Env = env
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Stdin = os.Stdin

		// Still save process info for tracking
		info := &ProcessInfo{
			Name:      name,
			Type:      "broker",
			PID:       0, // Will update after start
			Address:   address,
			StartedAt: time.Now(),
			Args:      args,
		}

		fmt.Printf("Starting broker '%s' at %s\n", name, address)
		fmt.Printf("  Multicast: %s\n", multicast)
		fmt.Println("Press Ctrl+C to stop")
		fmt.Println()

		if err := cmd.Start(); err != nil {
			fmt.Printf("Error starting broker: %v\n", err)
			os.Exit(1)
		}

		info.PID = cmd.Process.Pid
		state.Processes[name] = info
		saveState(state)
		writePidFile(name, cmd.Process.Pid)

		// Wait for process to finish
		cmd.Wait()

		// Clean up when done
		delete(state.Processes, name)
		saveState(state)
		removePidFile(name)
	}
}

func startProducer(args []string) {
	// Parse flags
	name := ""
	leader := ""
	topic := "logs"
	rate := 0
	count := 0
	message := "log message"
	background := false

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--name", "-n":
			if i+1 < len(args) {
				name = args[i+1]
				i++
			}
		case "--leader", "-l":
			if i+1 < len(args) {
				leader = args[i+1]
				i++
			}
		case "--topic", "-t":
			if i+1 < len(args) {
				topic = args[i+1]
				i++
			}
		case "--rate", "-r":
			if i+1 < len(args) {
				rate, _ = strconv.Atoi(args[i+1])
				i++
			}
		case "--count", "-c":
			if i+1 < len(args) {
				count, _ = strconv.Atoi(args[i+1])
				i++
			}
		case "--message", "-m":
			if i+1 < len(args) {
				message = args[i+1]
				i++
			}
		case "--background", "-bg":
			background = true
		case "--help", "-h":
			fmt.Println("Start a producer client")
			fmt.Println()
			fmt.Println("Usage:")
			fmt.Println("  logstreamctl start producer [options]")
			fmt.Println()
			fmt.Println("Options:")
			fmt.Println("  --name, -n        Process name (required)")
			fmt.Println("  --leader, -l      Leader address IP:PORT (required)")
			fmt.Println("  --topic, -t       Topic name (default: logs)")
			fmt.Println("  --rate, -r        Messages per second (default: 0 = interactive)")
			fmt.Println("  --count, -c       Total messages to send (default: 0 = unlimited)")
			fmt.Println("  --message, -m     Message template (default: log message)")
			fmt.Println("  --background, -bg Run in background (requires --rate > 0)")
			os.Exit(0)
		}
	}

	if name == "" {
		fmt.Println("Error: --name is required")
		os.Exit(1)
	}
	if leader == "" {
		fmt.Println("Error: --leader is required")
		os.Exit(1)
	}

	// Check if name already exists
	state := loadState()
	if _, exists := state.Processes[name]; exists {
		if isProcessRunning(state.Processes[name].PID) {
			fmt.Printf("Error: process '%s' is already running (PID %d)\n", name, state.Processes[name].PID)
			os.Exit(1)
		}
		delete(state.Processes, name)
	}

	// Find binary
	binary := findBinary("producer")
	if binary == "" {
		fmt.Println("Error: 'producer' binary not found")
		fmt.Println("Build it with: go build -o producer cmd/producer/main.go")
		os.Exit(1)
	}

	// Build command args
	cmdArgs := []string{
		"-leader", leader,
		"-topic", topic,
	}
	if rate > 0 {
		cmdArgs = append(cmdArgs, "-rate", strconv.Itoa(rate))
	}
	if count > 0 {
		cmdArgs = append(cmdArgs, "-count", strconv.Itoa(count))
	}
	if message != "log message" {
		cmdArgs = append(cmdArgs, "-message", message)
	}

	if background {
		// Background mode - need rate > 0 for non-interactive
		if rate == 0 {
			fmt.Println("Warning: producer in background mode requires --rate > 0")
			fmt.Println("Setting --rate 1 (1 message/sec)")
			rate = 1
			cmdArgs = append(cmdArgs, "-rate", "1")
		}

		logFile := getLogPath(name)

		lf, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			fmt.Printf("Error creating log file: %v\n", err)
			os.Exit(1)
		}

		cmd := exec.Command(binary, cmdArgs...)
		cmd.Stdout = lf
		cmd.Stderr = lf
		cmd.Stdin = nil
		cmd.SysProcAttr = &syscall.SysProcAttr{
			Setpgid: true,
		}

		if err := cmd.Start(); err != nil {
			lf.Close()
			fmt.Printf("Error starting producer: %v\n", err)
			os.Exit(1)
		}

		lf.Close()

		info := &ProcessInfo{
			Name:      name,
			Type:      "producer",
			PID:       cmd.Process.Pid,
			Leader:    leader,
			Topic:     topic,
			StartedAt: time.Now(),
			Args:      args,
		}
		state.Processes[name] = info
		saveState(state)
		writePidFile(name, cmd.Process.Pid)

		fmt.Printf("Started producer '%s' (PID %d)\n", name, cmd.Process.Pid)
		fmt.Printf("  Leader: %s\n", leader)
		fmt.Printf("  Topic:  %s\n", topic)
		fmt.Printf("  Rate:   %d msg/sec\n", rate)
		fmt.Printf("  Logs:   %s\n", logFile)
	} else {
		// Foreground mode (default)
		cmd := exec.Command(binary, cmdArgs...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Stdin = os.Stdin

		info := &ProcessInfo{
			Name:      name,
			Type:      "producer",
			PID:       0,
			Leader:    leader,
			Topic:     topic,
			StartedAt: time.Now(),
			Args:      args,
		}

		fmt.Printf("Starting producer '%s' -> %s\n", name, leader)
		fmt.Printf("  Topic: %s\n", topic)
		fmt.Println("Press Ctrl+C to stop")
		fmt.Println()

		if err := cmd.Start(); err != nil {
			fmt.Printf("Error starting producer: %v\n", err)
			os.Exit(1)
		}

		info.PID = cmd.Process.Pid
		state.Processes[name] = info
		saveState(state)
		writePidFile(name, cmd.Process.Pid)

		cmd.Wait()

		delete(state.Processes, name)
		saveState(state)
		removePidFile(name)
	}
}

func startConsumer(args []string) {
	// Parse flags
	name := ""
	leader := ""
	topic := "logs"
	analytics := true
	background := false

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--name", "-n":
			if i+1 < len(args) {
				name = args[i+1]
				i++
			}
		case "--leader", "-l":
			if i+1 < len(args) {
				leader = args[i+1]
				i++
			}
		case "--topic", "-t":
			if i+1 < len(args) {
				topic = args[i+1]
				i++
			}
		case "--analytics":
			if i+1 < len(args) {
				analytics = args[i+1] == "true"
				i++
			}
		case "--no-analytics":
			analytics = false
		case "--background", "-bg":
			background = true
		case "--help", "-h":
			fmt.Println("Start a consumer client")
			fmt.Println()
			fmt.Println("Usage:")
			fmt.Println("  logstreamctl start consumer [options]")
			fmt.Println()
			fmt.Println("Options:")
			fmt.Println("  --name, -n        Process name (required)")
			fmt.Println("  --leader, -l      Leader address IP:PORT (required)")
			fmt.Println("  --topic, -t       Topic name (default: logs)")
			fmt.Println("  --no-analytics    Disable analytics processing")
			fmt.Println("  --background, -bg Run in background")
			os.Exit(0)
		}
	}

	if name == "" {
		fmt.Println("Error: --name is required")
		os.Exit(1)
	}
	if leader == "" {
		fmt.Println("Error: --leader is required")
		os.Exit(1)
	}

	// Check if name already exists
	state := loadState()
	if _, exists := state.Processes[name]; exists {
		if isProcessRunning(state.Processes[name].PID) {
			fmt.Printf("Error: process '%s' is already running (PID %d)\n", name, state.Processes[name].PID)
			os.Exit(1)
		}
		delete(state.Processes, name)
	}

	// Find binary
	binary := findBinary("consumer")
	if binary == "" {
		fmt.Println("Error: 'consumer' binary not found")
		fmt.Println("Build it with: go build -o consumer cmd/consumer/main.go")
		os.Exit(1)
	}

	// Build command args
	cmdArgs := []string{
		"-leader", leader,
		"-topic", topic,
	}
	if !analytics {
		cmdArgs = append(cmdArgs, "-analytics=false")
	}

	if background {
		logFile := getLogPath(name)

		lf, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			fmt.Printf("Error creating log file: %v\n", err)
			os.Exit(1)
		}

		cmd := exec.Command(binary, cmdArgs...)
		cmd.Stdout = lf
		cmd.Stderr = lf
		cmd.Stdin = nil
		cmd.SysProcAttr = &syscall.SysProcAttr{
			Setpgid: true,
		}

		if err := cmd.Start(); err != nil {
			lf.Close()
			fmt.Printf("Error starting consumer: %v\n", err)
			os.Exit(1)
		}

		lf.Close()

		info := &ProcessInfo{
			Name:      name,
			Type:      "consumer",
			PID:       cmd.Process.Pid,
			Leader:    leader,
			Topic:     topic,
			StartedAt: time.Now(),
			Args:      args,
		}
		state.Processes[name] = info
		saveState(state)
		writePidFile(name, cmd.Process.Pid)

		fmt.Printf("Started consumer '%s' (PID %d)\n", name, cmd.Process.Pid)
		fmt.Printf("  Leader:    %s\n", leader)
		fmt.Printf("  Topic:     %s\n", topic)
		fmt.Printf("  Analytics: %v\n", analytics)
		fmt.Printf("  Logs:      %s\n", logFile)
	} else {
		// Foreground mode (default)
		cmd := exec.Command(binary, cmdArgs...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Stdin = os.Stdin

		info := &ProcessInfo{
			Name:      name,
			Type:      "consumer",
			PID:       0,
			Leader:    leader,
			Topic:     topic,
			StartedAt: time.Now(),
			Args:      args,
		}

		fmt.Printf("Starting consumer '%s' -> %s\n", name, leader)
		fmt.Printf("  Topic:     %s\n", topic)
		fmt.Printf("  Analytics: %v\n", analytics)
		fmt.Println("Press Ctrl+C to stop")
		fmt.Println()

		if err := cmd.Start(); err != nil {
			fmt.Printf("Error starting consumer: %v\n", err)
			os.Exit(1)
		}

		info.PID = cmd.Process.Pid
		state.Processes[name] = info
		saveState(state)
		writePidFile(name, cmd.Process.Pid)

		cmd.Wait()

		delete(state.Processes, name)
		saveState(state)
		removePidFile(name)
	}
}

// =============================================================================
// STOP COMMAND
// =============================================================================

func handleStop(args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: logstreamctl stop <name> | --all")
		os.Exit(1)
	}

	if args[0] == "--all" || args[0] == "-a" {
		stopAll()
	} else {
		stopProcess(args[0])
	}
}

func stopProcess(name string) {
	state := loadState()

	info, exists := state.Processes[name]
	if !exists {
		fmt.Printf("Process '%s' not found\n", name)
		os.Exit(1)
	}

	if !isProcessRunning(info.PID) {
		fmt.Printf("Process '%s' is not running (stale entry)\n", name)
		delete(state.Processes, name)
		saveState(state)
		removePidFile(name)
		return
	}

	// Send SIGTERM
	process, err := os.FindProcess(info.PID)
	if err != nil {
		fmt.Printf("Error finding process: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Stopping '%s' (PID %d)...\n", name, info.PID)

	if err := process.Signal(syscall.SIGTERM); err != nil {
		fmt.Printf("Error sending SIGTERM: %v\n", err)
		// Try SIGKILL
		if err := process.Signal(syscall.SIGKILL); err != nil {
			fmt.Printf("Error sending SIGKILL: %v\n", err)
		}
	}

	// Wait for process to exit (with timeout)
	done := make(chan bool, 1)
	go func() {
		for i := 0; i < 50; i++ { // 5 seconds
			if !isProcessRunning(info.PID) {
				done <- true
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		done <- false
	}()

	if <-done {
		fmt.Printf("Stopped '%s'\n", name)
	} else {
		fmt.Printf("Process '%s' did not stop gracefully, sending SIGKILL\n", name)
		process.Signal(syscall.SIGKILL)
		time.Sleep(500 * time.Millisecond)
	}

	delete(state.Processes, name)
	saveState(state)
	removePidFile(name)
}

func stopAll() {
	state := loadState()

	if len(state.Processes) == 0 {
		fmt.Println("No processes to stop")
		return
	}

	fmt.Printf("Stopping %d processes...\n", len(state.Processes))

	for name := range state.Processes {
		stopProcess(name)
	}

	fmt.Println("All processes stopped")
}

// =============================================================================
// LIST COMMAND
// =============================================================================

func handleList() {
	state := loadState()

	if len(state.Processes) == 0 {
		fmt.Println("No managed processes")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "NAME\tTYPE\tSTATUS\tPID\tADDRESS/LEADER\tTOPIC\tUPTIME")

	for _, info := range state.Processes {
		status := "stopped"
		uptime := "-"
		if isProcessRunning(info.PID) {
			status = "running"
			uptime = formatDuration(time.Since(info.StartedAt))
		}

		addrOrLeader := info.Address
		if addrOrLeader == "" {
			addrOrLeader = info.Leader
		}

		topic := info.Topic
		if topic == "" {
			topic = "-"
		}

		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\t%s\t%s\n",
			info.Name, info.Type, status, info.PID, addrOrLeader, topic, uptime)
	}

	w.Flush()
}

// =============================================================================
// LOGS COMMAND
// =============================================================================

func handleLogs(args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: logstreamctl logs <name> [--follow]")
		os.Exit(1)
	}

	name := args[0]
	follow := false
	lines := 50

	for i := 1; i < len(args); i++ {
		switch args[i] {
		case "--follow", "-f":
			follow = true
		case "--lines", "-n":
			if i+1 < len(args) {
				lines, _ = strconv.Atoi(args[i+1])
				i++
			}
		}
	}

	logFile := getLogPath(name)

	if _, err := os.Stat(logFile); os.IsNotExist(err) {
		fmt.Printf("No logs found for '%s'\n", name)
		fmt.Printf("Expected log file: %s\n", logFile)
		os.Exit(1)
	}

	if follow {
		// Use tail -f
		cmd := exec.Command("tail", "-f", "-n", strconv.Itoa(lines), logFile)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		// Handle Ctrl+C gracefully
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt)

		go func() {
			<-sigChan
			cmd.Process.Kill()
		}()

		fmt.Printf("Following logs for '%s' (Ctrl+C to stop)...\n\n", name)
		cmd.Run()
	} else {
		// Just cat the last N lines
		cmd := exec.Command("tail", "-n", strconv.Itoa(lines), logFile)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Run()
	}
}

// =============================================================================
// STATUS COMMAND
// =============================================================================

func handleStatus(args []string) {
	showCluster := false
	leaderAddr := ""

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--cluster", "-c":
			showCluster = true
		case "--leader", "-l":
			if i+1 < len(args) {
				leaderAddr = args[i+1]
				i++
			}
		}
	}

	// Local process status
	state := loadState()

	fmt.Println("LOCAL PROCESSES")
	fmt.Println("===============")

	if len(state.Processes) == 0 {
		fmt.Println("No managed processes")
	} else {
		running := 0
		stopped := 0

		for _, info := range state.Processes {
			if isProcessRunning(info.PID) {
				running++
			} else {
				stopped++
			}
		}

		fmt.Printf("Total:   %d\n", len(state.Processes))
		fmt.Printf("Running: %d\n", running)
		fmt.Printf("Stopped: %d\n", stopped)
		fmt.Println()

		handleList()
	}

	if showCluster {
		fmt.Println()
		fmt.Println("CLUSTER STATUS")
		fmt.Println("==============")

		if leaderAddr == "" {
			// Try to find a running broker
			for _, info := range state.Processes {
				if info.Type == "broker" && isProcessRunning(info.PID) {
					leaderAddr = info.Address
					break
				}
			}
		}

		if leaderAddr == "" {
			fmt.Println("No leader address specified and no local brokers running")
			fmt.Println("Use: logstreamctl status --cluster --leader <ip:port>")
		} else {
			fmt.Printf("Querying cluster at %s...\n", leaderAddr)
			fmt.Println("(Cluster status query not implemented yet)")
			// TODO: Connect to broker and query cluster state
		}
	}
}

// =============================================================================
// ELECTION COMMAND
// =============================================================================

func handleElection(args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: logstreamctl election <broker-name>")
		os.Exit(1)
	}

	name := args[0]
	state := loadState()

	info, exists := state.Processes[name]
	if !exists {
		fmt.Printf("Process '%s' not found\n", name)
		os.Exit(1)
	}

	if info.Type != "broker" {
		fmt.Printf("Process '%s' is a %s, not a broker\n", name, info.Type)
		os.Exit(1)
	}

	if !isProcessRunning(info.PID) {
		fmt.Printf("Broker '%s' is not running\n", name)
		os.Exit(1)
	}

	// Send SIGUSR1 to trigger election
	process, err := os.FindProcess(info.PID)
	if err != nil {
		fmt.Printf("Error finding process: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Triggering election on '%s' (PID %d)...\n", name, info.PID)

	if err := process.Signal(syscall.SIGUSR1); err != nil {
		fmt.Printf("Error sending SIGUSR1: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Election signal sent")
	fmt.Printf("Check logs with: logstreamctl logs %s --follow\n", name)
}

// =============================================================================
// CONFIG COMMAND
// =============================================================================

func handleConfig(subCmd string) {
	switch subCmd {
	case "init":
		initStateDir()
	case "show":
		fmt.Printf("State directory: %s\n", getStateDir())
		fmt.Printf("PID directory:   %s\n", filepath.Join(getStateDir(), pidsDir))
		fmt.Printf("Log directory:   %s\n", filepath.Join(getStateDir(), logsDir))
		fmt.Printf("State file:      %s\n", filepath.Join(getStateDir(), stateFile))
	default:
		fmt.Printf("Unknown config subcommand: %s\n", subCmd)
		fmt.Println("Valid subcommands: init, show")
	}
}

func initStateDir() {
	dir := getStateDir()

	dirs := []string{
		dir,
		filepath.Join(dir, pidsDir),
		filepath.Join(dir, logsDir),
	}

	for _, d := range dirs {
		if err := os.MkdirAll(d, 0755); err != nil {
			fmt.Printf("Error creating directory %s: %v\n", d, err)
			os.Exit(1)
		}
	}

	// Create empty state file if it doesn't exist
	stateFilePath := filepath.Join(dir, stateFile)
	if _, err := os.Stat(stateFilePath); os.IsNotExist(err) {
		state := &State{Processes: make(map[string]*ProcessInfo)}
		saveState(state)
	}

	fmt.Printf("Initialized state directory: %s\n", dir)
	fmt.Println()
	fmt.Println("Directory structure:")
	fmt.Printf("  %s/\n", dir)
	fmt.Printf("  |-- %s/     (PID files)\n", pidsDir)
	fmt.Printf("  |-- %s/    (log files)\n", logsDir)
	fmt.Printf("  +-- %s (process metadata)\n", stateFile)
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

func getStateDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return stateDir
	}
	return filepath.Join(home, stateDir)
}

func getLogPath(name string) string {
	return filepath.Join(getStateDir(), logsDir, name+".log")
}

func getPidPath(name string) string {
	return filepath.Join(getStateDir(), pidsDir, name+".pid")
}

func loadState() *State {
	stateFilePath := filepath.Join(getStateDir(), stateFile)

	data, err := os.ReadFile(stateFilePath)
	if err != nil {
		// Return empty state
		return &State{Processes: make(map[string]*ProcessInfo)}
	}

	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return &State{Processes: make(map[string]*ProcessInfo)}
	}

	if state.Processes == nil {
		state.Processes = make(map[string]*ProcessInfo)
	}

	return &state
}

func saveState(state *State) {
	stateFilePath := filepath.Join(getStateDir(), stateFile)

	// Ensure directory exists
	os.MkdirAll(filepath.Dir(stateFilePath), 0755)

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		fmt.Printf("Error marshaling state: %v\n", err)
		return
	}

	if err := os.WriteFile(stateFilePath, data, 0644); err != nil {
		fmt.Printf("Error writing state file: %v\n", err)
	}
}

func writePidFile(name string, pid int) {
	pidPath := getPidPath(name)
	os.MkdirAll(filepath.Dir(pidPath), 0755)
	os.WriteFile(pidPath, []byte(strconv.Itoa(pid)), 0644)
}

func removePidFile(name string) {
	os.Remove(getPidPath(name))
}

func isProcessRunning(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}

	// On Unix, FindProcess always succeeds. Use Signal(0) to check if process exists
	err = process.Signal(syscall.Signal(0))
	return err == nil
}

func findBinary(name string) string {
	// First check current directory
	if _, err := os.Stat(name); err == nil {
		abs, _ := filepath.Abs(name)
		return abs
	}

	// Check in PATH
	path, err := exec.LookPath(name)
	if err == nil {
		return path
	}

	// Check in common locations
	locations := []string{
		filepath.Join(".", name),
		filepath.Join("..", name),
		filepath.Join("bin", name),
	}

	for _, loc := range locations {
		if _, err := os.Stat(loc); err == nil {
			abs, _ := filepath.Abs(loc)
			return abs
		}
	}

	return ""
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
	}
	days := int(d.Hours()) / 24
	hours := int(d.Hours()) % 24
	return fmt.Sprintf("%dd%dh", days, hours)
}

// copyOutput copies from reader to writer until EOF
func copyOutput(dst io.Writer, src io.Reader) {
	io.Copy(dst, src)
}
