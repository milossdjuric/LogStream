package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/milossdjuric/logstream/internal/client"
)

func main() {
	leader := flag.String("leader", os.Getenv("LEADER_ADDRESS"), "Leader address (IP:PORT, e.g. 192.168.1.10:8001)")
	topic := flag.String("topic", getEnvOrDefault("TOPIC", "logs"), "Topic to consume from")
	port := flag.Int("port", 8003, "Client TCP listener port (default: 8003)")
	address := flag.String("address", "", "Advertised IP address (for WSL/NAT, e.g. Windows host LAN IP)")
	analytics := flag.Bool("analytics", true, "Request analytics/processing from broker")
	windowSeconds := flag.Int("window", 0, "Analytics window in seconds (0 = broker default, typically 60)")
	intervalMs := flag.Int("interval", 0, "Analytics update interval in ms (0 = broker default, typically 1000)")
	hbInterval := flag.Int("hb-interval", 2, "Heartbeat interval in seconds (default: 2)")
	reconnectAttempts := flag.Int("reconnect-attempts", 10, "Max reconnection attempts (default: 10)")
	reconnectDelay := flag.Int("reconnect-delay", 5, "Delay between reconnection attempts in seconds (default: 5)")
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
		fmt.Println()
		fmt.Println("  # Custom heartbeat timing")
		fmt.Println("  ./consumer -topic logs -hb-interval 5")
		fmt.Println()
		fmt.Println("  # More reconnection attempts with shorter delay")
		fmt.Println("  ./consumer -topic logs -reconnect-attempts 20 -reconnect-delay 2")
		os.Exit(0)
	}

	consumer := client.NewConsumerWithFullOptions(*topic, *leader, client.ConsumerOptions{
		EnableProcessing:       *analytics,
		AnalyticsWindowSeconds: int32(*windowSeconds),
		AnalyticsIntervalMs:    int32(*intervalMs),
		ClientPort:             *port,
		AdvertiseAddr:          *address,
		HeartbeatInterval:      time.Duration(*hbInterval) * time.Second,
		ReconnectAttempts:      *reconnectAttempts,
		ReconnectDelay:         time.Duration(*reconnectDelay) * time.Second,
	})

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

	fmt.Printf("[Consumer %s] Connecting to cluster...\n", cid)
	if err := consumer.Connect(); err != nil {
		log.Fatalf("[Consumer %s] Failed to connect: %v\n", cid, err)
	}
	fmt.Printf("[Consumer %s] Connected successfully\n", cid)
	fmt.Println()
	fmt.Printf("[Consumer %s] Receiving messages (Press Ctrl+C to stop)...\n", cid)
	fmt.Println()

	// Setup signal handling - first Ctrl+C graceful, second forces exit
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	type analyticsResult struct {
		Topic         string  `json:"topic"`
		TotalCount    int64   `json:"total_count"`
		WindowSeconds int     `json:"window_seconds"`
		WindowCount   int64   `json:"window_count"`
		WindowRate    float64 `json:"window_rate"`
		RawData       []byte  `json:"raw_data,omitempty"`
	}

	received := 0
	for {
		select {
		case result := <-consumer.Results():
			ts := time.Now().Format("15:04:05")

			var ar analyticsResult
			if json.Unmarshal(result.Data, &ar) == nil && ar.Topic != "" {
				if len(ar.RawData) > 0 {
					received++
					fmt.Println()
					fmt.Printf("  [%s] Message #%d\n", ts, received)
					fmt.Printf("  Topic:   %s\n", ar.Topic)
					fmt.Printf("  Offset:  %d\n", result.Offset)
					fmt.Printf("  Data:    %s\n", string(ar.RawData))
					fmt.Printf("  Stats:   %d total | %d in %ds | %.2f msg/s\n",
						ar.TotalCount, ar.WindowCount, ar.WindowSeconds, ar.WindowRate)
				} else {
					fmt.Printf("  [%s] Stats: %d total | %d in %ds | %.2f msg/s\n",
						ts, ar.TotalCount, ar.WindowCount, ar.WindowSeconds, ar.WindowRate)
				}
			} else if len(result.Data) > 0 {
				received++
				fmt.Println()
				fmt.Printf("  [%s] Message #%d\n", ts, received)
				fmt.Printf("  Topic:   %s\n", result.Topic)
				fmt.Printf("  Offset:  %d\n", result.Offset)
				fmt.Printf("  Data:    %s\n", string(result.Data))
			}

		case <-sigChan:
			fmt.Printf("\n\n[Consumer %s] Received %d messages total\n", cid, received)
			fmt.Printf("[Consumer %s] Disconnecting (press Ctrl+C again to force)...\n", cid)
			// Second Ctrl+C forces immediate exit
			go func() {
				<-sigChan
				fmt.Printf("\n[Consumer %s] Forced exit\n", cid)
				os.Exit(1)
			}()
			consumer.Close()
			fmt.Printf("[Consumer %s] Disconnected\n", cid)
			return
		}
	}
}

func getEnvOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
