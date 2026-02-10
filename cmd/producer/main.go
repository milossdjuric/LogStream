package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/milossdjuric/logstream/internal/client"
)

func main() {
	// Simple CLI flags with env var fallback
	leader := flag.String("leader", os.Getenv("LEADER_ADDRESS"), "Leader address (IP:PORT, e.g. 192.168.1.10:8001)")
	topic := flag.String("topic", getEnvOrDefault("TOPIC", "logs"), "Topic to produce to")
	port := flag.Int("port", 8002, "Client TCP listener port (default: 8002)")
	address := flag.String("address", "", "Advertised IP address (for WSL/NAT, e.g. Windows host LAN IP)")
	rate := flag.Int("rate", 0, "Messages per second (0 = interactive mode)")
	interval := flag.Int("interval", 0, "Interval between messages in milliseconds (overrides -rate)")
	count := flag.Int("count", 0, "Number of messages to send (0 = unlimited)")
	message := flag.String("message", "log message", "Message template for auto mode")
	hbInterval := flag.Int("hb-interval", 2, "Heartbeat interval in seconds (default: 2)")
	hbTimeout := flag.Int("hb-timeout", 10, "Leader timeout in seconds before reconnect (default: 10)")
	reconnectAttempts := flag.Int("reconnect-attempts", 10, "Max reconnection attempts (default: 10)")
	reconnectDelay := flag.Int("reconnect-delay", 5, "Delay between reconnection attempts in seconds (default: 5)")
	help := flag.Bool("help", false, "Show help")

	flag.Parse()

	if *help {
		fmt.Println("LogStream Producer")
		fmt.Println("==================")
		fmt.Println()
		fmt.Println("Usage:")
		fmt.Println("  producer [options]")
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
		fmt.Println("        Client TCP listener port (default: 8002)")
		fmt.Println()
		fmt.Println("  -rate int")
		fmt.Println("        Messages per second, 0 for interactive (default: 0)")
		fmt.Println()
		fmt.Println("  -interval int")
		fmt.Println("        Interval between messages in milliseconds (overrides -rate)")
		fmt.Println("        Example: -interval 2000 = 1 message every 2 seconds")
		fmt.Println()
		fmt.Println("  -count int")
		fmt.Println("        Number of messages to send, 0 for unlimited (default: 0)")
		fmt.Println()
		fmt.Println("  -message string")
		fmt.Println("        Message template for auto mode (default: log message)")
		fmt.Println()
		fmt.Println("Examples:")
		fmt.Println("  # Auto-discover cluster, interactive mode")
		fmt.Println("  ./producer -topic logs")
		fmt.Println()
		fmt.Println("  # Auto-discover cluster, auto mode - 10 messages per second")
		fmt.Println("  ./producer -topic logs -rate 10")
		fmt.Println()
		fmt.Println("  # Explicit leader address")
		fmt.Println("  ./producer -leader 192.168.1.10:8001 -topic logs -rate 10")
		fmt.Println()
		fmt.Println("  # Send 100 messages and exit")
		fmt.Println("  ./producer -topic logs -rate 10 -count 100")
		fmt.Println()
		fmt.Println("  # Slow rate - 1 message every 2 seconds")
		fmt.Println("  ./producer -topic logs -interval 2000")
		fmt.Println()
		fmt.Println("  # Slow rate - 1 message every 5 seconds")
		fmt.Println("  ./producer -topic logs -interval 5000")
		fmt.Println()
		fmt.Println("  # Custom heartbeat timing")
		fmt.Println("  ./producer -topic logs -hb-interval 5 -hb-timeout 30")
		fmt.Println()
		fmt.Println("  # More reconnection attempts with shorter delay")
		fmt.Println("  ./producer -topic logs -reconnect-attempts 20 -reconnect-delay 2")
		os.Exit(0)
	}

	fmt.Println("===========================================")
	fmt.Println("       LogStream Producer")
	fmt.Println("===========================================")
	fmt.Println()
	if *leader != "" {
		fmt.Printf("Leader:  %s\n", *leader)
	} else {
		fmt.Println("Leader:  (auto-discover via broadcast)")
	}
	fmt.Printf("Topic:   %s\n", *topic)
	fmt.Printf("Port:    %d\n", *port)
	fmt.Println()

	// Create producer
	producer := client.NewProducerWithOptions(*topic, *leader, client.ProducerOptions{
		ClientPort:        *port,
		AdvertiseAddr:     *address,
		HeartbeatInterval: time.Duration(*hbInterval) * time.Second,
		LeaderTimeout:     time.Duration(*hbTimeout) * time.Second,
		ReconnectAttempts: *reconnectAttempts,
		ReconnectDelay:    time.Duration(*reconnectDelay) * time.Second,
	})

	// Connect to cluster
	fmt.Println("Connecting to cluster...")
	if err := producer.Connect(); err != nil {
		log.Fatalf("Failed to connect: %v\n", err)
	}
	fmt.Println("[OK] Connected successfully")
	fmt.Println()

	// Setup signal handling - first Ctrl+C graceful, second forces exit
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	done := make(chan struct{})

	if *rate > 0 || *interval > 0 {
		// Auto mode - send messages at specified rate/interval
		var tickInterval time.Duration
		if *interval > 0 {
			// Use explicit interval (milliseconds)
			tickInterval = time.Duration(*interval) * time.Millisecond
			fmt.Printf("Sending messages every %dms (%.2f msg/sec)", *interval, 1000.0/float64(*interval))
		} else {
			// Use rate (messages per second)
			tickInterval = time.Second / time.Duration(*rate)
			fmt.Printf("Sending messages at %d msg/sec", *rate)
		}
		if *count > 0 {
			fmt.Printf(" (total: %d messages)", *count)
		}
		fmt.Println()
		fmt.Println("Press Ctrl+C to stop")
		fmt.Println()

		go func() {
			defer close(done)
			ticker := time.NewTicker(tickInterval)
			defer ticker.Stop()

			sent := 0
			for {
				select {
				case <-ticker.C:
				msg := fmt.Sprintf("%s #%d at %s", *message, sent+1, time.Now().Format("15:04:05"))
				if err := producer.SendData([]byte(msg)); err != nil {
					log.Printf("Send error: %v\n", err)
				} else {
						sent++
						if sent%10 == 0 || (*count > 0 && sent >= *count) {
							fmt.Printf("[OK] Sent %d messages\n", sent)
						}
					}

					if *count > 0 && sent >= *count {
						fmt.Printf("\n[OK] Sent all %d messages\n", sent)
						return
					}
				case <-sigChan:
					fmt.Printf("\n[OK] Sent %d messages total\n", sent)
					return
				}
			}
		}()

		<-done
	} else {
		// Interactive mode - read from stdin
		fmt.Println("Interactive mode - type messages and press Enter")
		fmt.Println("Press Ctrl+C to exit")
		fmt.Println()

		go func() {
			defer close(done)
			scanner := bufio.NewScanner(os.Stdin)
			sent := 0

			for scanner.Scan() {
				line := strings.TrimSpace(scanner.Text())
				if line == "" {
					continue
				}

				if err := producer.SendData([]byte(line)); err != nil {
					log.Printf("Send error: %v\n", err)
				} else {
					sent++
					fmt.Printf("[OK] Sent message #%d\n", sent)
				}
			}

			if err := scanner.Err(); err != nil {
				log.Printf("Scanner error: %v\n", err)
			}
		}()

		select {
		case <-done:
		case <-sigChan:
		}
	}

	fmt.Println("\nDisconnecting (press Ctrl+C again to force)...")
	// Second Ctrl+C forces immediate exit
	go func() {
		<-sigChan
		fmt.Println("\n[Producer] Forced exit")
		os.Exit(1)
	}()
	producer.Close()
	fmt.Println("[OK] Disconnected")
}

// getEnvOrDefault returns environment variable value or default
func getEnvOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
