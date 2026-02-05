package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/milossdjuric/logstream/internal/client"
)

func main() {
	// CLI flags with env var fallback
	leader := flag.String("leader", os.Getenv("LEADER_ADDRESS"), "Leader address (IP:PORT, e.g. 192.168.1.10:8001)")
	topic := flag.String("topic", getEnvOrDefault("TOPIC", "logs"), "Topic to consume from")
	analytics := flag.Bool("analytics", true, "Request analytics/processing from broker")
	help := flag.Bool("help", false, "Show help")

	flag.Parse()

	if *help {
		fmt.Println("LogStream Consumer")
		fmt.Println("==================")
		fmt.Println()
		fmt.Println("Usage:")
		fmt.Println("  consumer [options]")
		fmt.Println()
		fmt.Println("Options:")
		fmt.Println("  -leader string")
		fmt.Println("        Leader broker address (optional - auto-discovers via broadcast if not set)")
		fmt.Println("        Example: -leader 192.168.1.10:8001")
		fmt.Println()
		fmt.Println("  -topic string")
		fmt.Println("        Topic name (default: logs)")
		fmt.Println()
		fmt.Println("  -analytics bool")
		fmt.Println("        Request analytics/processing from broker (default: true)")
		fmt.Println()
		fmt.Println("Examples:")
		fmt.Println("  # Auto-discover cluster, consume with analytics")
		fmt.Println("  ./consumer -topic logs")
		fmt.Println()
		fmt.Println("  # Explicit leader address")
		fmt.Println("  ./consumer -leader 192.168.1.10:8001 -topic logs")
		fmt.Println()
		fmt.Println("  # Consume raw data only (no analytics)")
		fmt.Println("  ./consumer -topic logs -analytics=false")
		os.Exit(0)
	}

	fmt.Println("===========================================")
	fmt.Println("       LogStream Consumer")
	fmt.Println("===========================================")
	fmt.Println()
	if *leader != "" {
		fmt.Printf("Leader:    %s\n", *leader)
	} else {
		fmt.Println("Leader:    (auto-discover via broadcast)")
	}
	fmt.Printf("Topic:     %s\n", *topic)
	fmt.Printf("Analytics: %v\n", *analytics)
	fmt.Println()

	// Create consumer
	consumer := client.NewConsumerWithOptions(*topic, *leader, *analytics)

	// Connect to cluster
	fmt.Println("Connecting to cluster...")
	if err := consumer.Connect(); err != nil {
		log.Fatalf("Failed to connect: %v\n", err)
	}
	fmt.Println("[OK] Connected successfully")
	fmt.Println()
	fmt.Println("Receiving messages (Press Ctrl+C to stop)...")
	fmt.Println()

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Display received messages (receiveResults() already started in Connect())
	received := 0
	for {
		select {
		case result := <-consumer.Results():
			received++
			fmt.Printf("-----------------------------------------\n")
			fmt.Printf("Message #%d\n", received)
			fmt.Printf("Topic:  %s\n", result.Topic)
			fmt.Printf("Offset: %d\n", result.Offset)
			
			if len(result.Data) > 0 {
				fmt.Printf("Data:   %s\n", string(result.Data))
			}

		case err := <-consumer.Errors():
			log.Printf("Error: %v\n", err)

		case <-sigChan:
			fmt.Printf("\n\n[OK] Received %d messages total\n", received)
			fmt.Println("\nDisconnecting...")
			consumer.Close()
			fmt.Println("[OK] Disconnected")
			return
		}
	}
}

// getEnvOrDefault returns environment variable value or default
func getEnvOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
