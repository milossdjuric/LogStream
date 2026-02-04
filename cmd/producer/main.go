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
	rate := flag.Int("rate", 0, "Messages per second (0 = interactive mode)")
	count := flag.Int("count", 0, "Number of messages to send (0 = unlimited)")
	message := flag.String("message", "log message", "Message template for auto mode")
	help := flag.Bool("help", false, "Show help")

	flag.Parse()

	if *help || *leader == "" {
		fmt.Println("LogStream Producer")
		fmt.Println("==================")
		fmt.Println()
		fmt.Println("Usage:")
		fmt.Println("  producer -leader <address> [options]")
		fmt.Println()
		fmt.Println("Options:")
		fmt.Println("  -leader string")
		fmt.Println("        Leader broker address (REQUIRED)")
		fmt.Println("        Example: -leader 192.168.1.10:8001")
		fmt.Println()
		fmt.Println("  -topic string")
		fmt.Println("        Topic name (default: logs)")
		fmt.Println()
		fmt.Println("  -rate int")
		fmt.Println("        Messages per second, 0 for interactive (default: 0)")
		fmt.Println()
		fmt.Println("  -count int")
		fmt.Println("        Number of messages to send, 0 for unlimited (default: 0)")
		fmt.Println()
		fmt.Println("  -message string")
		fmt.Println("        Message template for auto mode (default: log message)")
		fmt.Println()
		fmt.Println("Examples:")
		fmt.Println("  # Interactive mode - type messages")
		fmt.Println("  ./producer -leader 192.168.1.10:8001 -topic logs")
		fmt.Println()
		fmt.Println("  # Auto mode - 10 messages per second")
		fmt.Println("  ./producer -leader 192.168.1.10:8001 -topic logs -rate 10")
		fmt.Println()
		fmt.Println("  # Send 100 messages and exit")
		fmt.Println("  ./producer -leader 192.168.1.10:8001 -topic logs -rate 10 -count 100")
		os.Exit(0)
	}

	fmt.Println("===========================================")
	fmt.Println("       LogStream Producer")
	fmt.Println("===========================================")
	fmt.Println()
	fmt.Printf("Leader:  %s\n", *leader)
	fmt.Printf("Topic:   %s\n", *topic)
	fmt.Println()

	// Create producer
	producer := client.NewProducer(*topic, *leader)

	// Connect to cluster
	fmt.Println("Connecting to cluster...")
	if err := producer.Connect(); err != nil {
		log.Fatalf("Failed to connect: %v\n", err)
	}
	fmt.Println("[OK] Connected successfully")
	fmt.Println()

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	done := make(chan struct{})

	if *rate > 0 {
		// Auto mode - send messages at specified rate
		fmt.Printf("Sending messages at %d msg/sec", *rate)
		if *count > 0 {
			fmt.Printf(" (total: %d messages)", *count)
		}
		fmt.Println()
		fmt.Println("Press Ctrl+C to stop")
		fmt.Println()

		go func() {
			defer close(done)
			interval := time.Second / time.Duration(*rate)
			ticker := time.NewTicker(interval)
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

	fmt.Println("\nDisconnecting...")
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
