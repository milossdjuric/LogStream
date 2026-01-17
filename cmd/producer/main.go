package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/milossdjuric/logstream/internal/client"
)

func main() {
	// Get configuration from environment
	leaderAddr := getEnv("LEADER_ADDRESS", "localhost:8001")
	topic := getEnv("TOPIC", "test-logs")

	fmt.Println("=== LogStream Producer ===")
	fmt.Printf("Leader Address: %s\n", leaderAddr)
	fmt.Printf("Topic:          %s\n", topic)
	fmt.Println("================================")
	fmt.Println()

	// Create producer using the client library
	producer := client.NewProducer(topic, leaderAddr)

	// Connect to cluster
	fmt.Println("Connecting to LogStream cluster...")
	if err := producer.Connect(); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}

	// Setup graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\n\nShutting down producer...")
		producer.Close()
		os.Exit(0)
	}()

	// Read from stdin and send data
	fmt.Println("Connected! Type messages to send (Ctrl+C to quit):")
	fmt.Println("---")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		if err := producer.SendData([]byte(line)); err != nil {
			log.Printf("Failed to send data: %v", err)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Error reading input: %v", err)
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
