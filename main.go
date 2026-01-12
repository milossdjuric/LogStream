package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/milossdjuric/logstream/internal/config"
	"github.com/milossdjuric/logstream/internal/node"
	"github.com/milossdjuric/logstream/internal/protocol"
)

func main() {
	// Show network info if NODE_ADDRESS not set
	if os.Getenv("NODE_ADDRESS") == "" {
		protocol.ShowNetworkInfo()
	}

	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Configuration error: %v\n", err)
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		log.Fatalf("Invalid configuration: %v\n", err)
	}

	// Print configuration
	cfg.Print()

	// Create and start node
	clusterNode := node.NewNode(cfg)
	if err := clusterNode.Start(); err != nil {
		log.Fatalf("Failed to start node: %v\n", err)
	}

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	fmt.Println("\n[System] Press Ctrl+C to shutdown")
	fmt.Println("==================================\n")

	<-sigChan

	// Shutdown gracefully
	clusterNode.Shutdown()
	fmt.Println("[System] Shutdown complete")
}
