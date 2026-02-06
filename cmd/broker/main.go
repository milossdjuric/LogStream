package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/milossdjuric/logstream/internal/config"
	"github.com/milossdjuric/logstream/internal/node"
)

func main() {
	// Simple CLI flags
	addr := flag.String("addr", "", "Node address (IP:PORT, e.g. 192.168.1.10:8001)")
	multicast := flag.String("multicast", "239.0.0.1:9999", "Multicast group address")
	broadcast := flag.Int("broadcast", 8888, "Broadcast port for discovery")
	maxRecords := flag.Int("max-records", 0, "Max records per topic log (0 = use default 10000)")
	help := flag.Bool("help", false, "Show help")
	
	flag.Parse()

	if *help {
		fmt.Println("LogStream Broker Node")
		fmt.Println("=====================")
		fmt.Println()
		fmt.Println("Usage:")
		fmt.Println("  broker [options]")
		fmt.Println()
		fmt.Println("Options:")
		fmt.Println("  -addr string")
		fmt.Println("        Node address in IP:PORT format (default: auto-detect)")
		fmt.Println("        Example: -addr 192.168.1.10:8001")
		fmt.Println()
		fmt.Println("  -multicast string")
		fmt.Println("        Multicast group address (default: 239.0.0.1:9999)")
		fmt.Println()
		fmt.Println("  -broadcast int")
		fmt.Println("        Broadcast port for discovery (default: 8888)")
		fmt.Println()
		fmt.Println("  -max-records int")
		fmt.Println("        Max records per topic log (default: 10000, 0 = unlimited)")
		fmt.Println()
		fmt.Println("Examples:")
		fmt.Println("  # Start with auto-detected IP")
		fmt.Println("  ./broker")
		fmt.Println()
		fmt.Println("  # Start on specific address")
		fmt.Println("  ./broker -addr 192.168.1.10:8001")
		fmt.Println()
		fmt.Println("  # Start with custom multicast group")
		fmt.Println("  ./broker -addr 192.168.1.10:8001 -multicast 239.0.0.5:9999")
		fmt.Println()
		fmt.Println("Interactive Commands:")
		fmt.Println("  election - Trigger leader election")
		fmt.Println("  status   - Show node status")
		fmt.Println("  Ctrl+C   - Shutdown gracefully")
		os.Exit(0)
	}

	// Set environment variables from flags
	if *addr != "" {
		os.Setenv("NODE_ADDRESS", *addr)
	}
	if *multicast != "" {
		os.Setenv("MULTICAST_GROUP", *multicast)
	}
	if *broadcast != 0 {
		os.Setenv("BROADCAST_PORT", fmt.Sprintf("%d", *broadcast))
	}
	if *maxRecords > 0 {
		os.Setenv("MAX_RECORDS_PER_TOPIC", fmt.Sprintf("%d", *maxRecords))
	}

	fmt.Println("===========================================")
	fmt.Println("       LogStream Broker Node")
	fmt.Println("===========================================")
	fmt.Println()

	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Configuration error: %v\n", err)
	}

	if err := cfg.Validate(); err != nil {
		log.Fatalf("Invalid configuration: %v\n", err)
	}

	fmt.Println("Configuration:")
	fmt.Printf("  Address:    %s\n", cfg.NodeAddress)
	fmt.Printf("  Multicast:  %s\n", cfg.MulticastGroup)
	fmt.Printf("  Broadcast:  %d\n", cfg.BroadcastPort)
	fmt.Println()

	// Create and start node
	clusterNode := node.NewNode(cfg)
	
	fmt.Println("Starting broker node...")
	if err := clusterNode.Start(); err != nil {
		log.Fatalf("Failed to start node: %v\n", err)
	}
	fmt.Println("Broker node started successfully")
	fmt.Println()
	fmt.Println("Press Ctrl+C to shutdown")
	fmt.Println()

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Wait for shutdown signal
	<-sigChan
	fmt.Println("\nShutting down...")
	clusterNode.Shutdown()
	fmt.Println("Shutdown complete")
}
