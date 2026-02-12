package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"

	"github.com/milossdjuric/logstream/internal/config"
	"github.com/milossdjuric/logstream/internal/node"
)

func main() {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackLen := runtime.Stack(buf, false)
			log.Printf("\n[System] ========================================\n")
			log.Printf("[System] FATAL PANIC RECOVERED:\n")
			log.Printf("[System] Error: %v\n", r)
			log.Printf("[System] Stack trace:\n%s\n", buf[:stackLen])
			log.Printf("[System] ========================================\n")
			os.Exit(1)
		}
	}()

	fmt.Printf("[System] ========================================\n")
	fmt.Printf("[System] PROCESS STARTING\n")
	fmt.Printf("[System] ========================================\n\n")

	if os.Getenv("NODE_ADDRESS") == "" {
		fmt.Printf("[System] NODE_ADDRESS not set, will auto-detect\n")
	}

	fmt.Printf("[System] Loading configuration...\n")
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("[System] Configuration error: %v\n", err)
	}
	fmt.Printf("[System] Configuration loaded successfully\n")

	fmt.Printf("[System] Validating configuration...\n")
	if err := cfg.Validate(); err != nil {
		log.Fatalf("[System] Invalid configuration: %v\n", err)
	}
	fmt.Printf("[System] Configuration validated successfully\n")

	fmt.Printf("[System] Configuration:\n")
	cfg.Print()

	fmt.Printf("[System] Creating node instance...\n")
	clusterNode := node.NewNode(cfg)
	fmt.Printf("[System] Node instance created successfully\n")

	fmt.Printf("[System] Starting node...\n")
	if err := clusterNode.Start(); err != nil {
		log.Fatalf("[System] Failed to start node: %v\n", err)
	}
	fmt.Printf("[System] Node started successfully\n")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGUSR1)

	// Only start command input handler if stdin is a terminal (not /dev/null or pipe)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				buf := make([]byte, 4096)
				stackLen := runtime.Stack(buf, false)
				log.Printf("[System] [Command-Input] PANIC: %v\nStack:\n%s\n", r, buf[:stackLen])
			}
		}()

		stat, err := os.Stdin.Stat()
		if err != nil || (stat.Mode()&os.ModeCharDevice) == 0 {
			fmt.Printf("[System] Stdin is not a terminal, skipping command input handler\n")
			return
		}

		fmt.Printf("[System] Starting command input handler...\n")
		scanner := bufio.NewScanner(os.Stdin)
		fmt.Println("\n[System] Commands:")
		fmt.Println("  'election' - Trigger leader election")
		fmt.Println("  'status'   - Show node status")
		fmt.Println("  Ctrl+C     - Shutdown")
		fmt.Println("==================================\n")

		for scanner.Scan() {
			cmd := strings.TrimSpace(scanner.Text())

			switch cmd {
			case "election":
				fmt.Println("\n[System] Triggering election manually...")
				if err := clusterNode.StartElection(); err != nil {
					log.Printf("[System] Election failed: %v\n", err)
				}

			case "status":
				clusterNode.PrintStatus()

			case "":

			default:
				fmt.Printf("[System] Unknown command: %s\n", cmd)
			}
		}

		if err := scanner.Err(); err != nil {
			// Don't log EOF errors - they're normal when stdin is closed
			if err.Error() != "EOF" {
				log.Printf("[System] [Command-Input] Scanner error: %v\n", err)
			}
		} else {
			fmt.Printf("[System] [Command-Input] Stdin closed, command input handler exiting\n")
		}
	}()

	// Signal handling loop (NON-BLOCKING for SIGUSR1)
	for {
		sig := <-sigChan

		switch sig {
		case syscall.SIGUSR1:
			// Trigger election but DON'T exit
			fmt.Printf("\n[System] ========================================\n")
			fmt.Printf("[System] Received SIGUSR1 - triggering election...\n")
			fmt.Printf("[System] ========================================\n\n")
			if err := clusterNode.StartElection(); err != nil {
				log.Printf("[System] Election failed: %v\n", err)
			} else {
				fmt.Printf("[System] Election triggered successfully\n")
			}
			// Continue running - DON'T block waiting for another signal

		case os.Interrupt, syscall.SIGTERM:
			fmt.Println("\n[System] Received shutdown signal...")
			clusterNode.Shutdown()
			fmt.Println("[System] Shutdown complete")
			return

		default:
			log.Printf("[System] Received unexpected signal: %v\n", sig)
		}
	}
}
