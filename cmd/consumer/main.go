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
	port := flag.Int("port", 8003, "Client TCP listener port (default: 8003)")
	analytics := flag.Bool("analytics", true, "Request analytics/processing from broker")
	windowSeconds := flag.Int("window", 0, "Analytics window in seconds (0 = broker default, typically 60)")
	intervalMs := flag.Int("interval", 0, "Analytics update interval in ms (0 = broker default, typically 1000)")
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
		fmt.Println("  -port int")
		fmt.Println("        Client TCP listener port (default: 8003)")
		fmt.Println()
		fmt.Println("  -analytics bool")
		fmt.Println("        Request analytics/processing from broker (default: true)")
		fmt.Println()
		fmt.Println("  -window int")
		fmt.Println("        Analytics window in seconds (0 = broker default, typically 60)")
		fmt.Println()
		fmt.Println("  -interval int")
		fmt.Println("        Analytics update interval in ms (0 = broker default, typically 1000)")
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
		fmt.Println()
		fmt.Println("  # Custom analytics window (30s) and update interval (2s)")
		fmt.Println("  ./consumer -topic logs -window 30 -interval 2000")
		os.Exit(0)
	}

	// Create consumer
	consumer := client.NewConsumerWithFullOptions(*topic, *leader, client.ConsumerOptions{
		EnableProcessing:       *analytics,
		AnalyticsWindowSeconds: int32(*windowSeconds),
		AnalyticsIntervalMs:    int32(*intervalMs),
		ClientPort:             *port,
	})

	// Consumer ID prefix for all logs
	cid := consumer.ID()[:8]

	fmt.Println("===========================================")
	fmt.Println("       LogStream Consumer")
	fmt.Println("===========================================")
	fmt.Println()
	fmt.Printf("ID:        %s\n", cid)
	if *leader != "" {
		fmt.Printf("Leader:    %s\n", *leader)
	} else {
		fmt.Println("Leader:    (auto-discover via broadcast)")
	}
	fmt.Printf("Topic:     %s\n", *topic)
	fmt.Printf("Port:      %d\n", *port)
	fmt.Printf("Analytics: %v\n", *analytics)
	if *windowSeconds > 0 {
		fmt.Printf("Window:    %ds\n", *windowSeconds)
	}
	if *intervalMs > 0 {
		fmt.Printf("Interval:  %dms\n", *intervalMs)
	}
	fmt.Println()

	// Connect to cluster
	fmt.Printf("[Consumer %s] Connecting to cluster...\n", cid)
	if err := consumer.Connect(); err != nil {
		log.Fatalf("[Consumer %s] Failed to connect: %v\n", cid, err)
	}
	fmt.Printf("[Consumer %s] Connected successfully\n", cid)
	fmt.Println()
	fmt.Printf("[Consumer %s] Receiving messages (Press Ctrl+C to stop)...\n", cid)
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
			fmt.Printf("[Consumer %s] Message #%d\n", cid, received)
			fmt.Printf("  Topic:  %s\n", result.Topic)
			fmt.Printf("  Offset: %d\n", result.Offset)

			if len(result.Data) > 0 {
				fmt.Printf("  Data:   %s\n", string(result.Data))
			}

		case err := <-consumer.Errors():
			log.Printf("[Consumer %s] Error: %v\n", cid, err)

		case <-sigChan:
			fmt.Printf("\n\n[Consumer %s] Received %d messages total\n", cid, received)
			fmt.Printf("[Consumer %s] Disconnecting...\n", cid)
			consumer.Close()
			fmt.Printf("[Consumer %s] Disconnected\n", cid)
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
