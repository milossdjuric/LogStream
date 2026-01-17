package main

import (
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

	fmt.Println("=== LogStream Consumer ===")
	fmt.Printf("Leader Address: %s\n", leaderAddr)
	fmt.Printf("Topic:          %s\n", topic)
	fmt.Println("================================")
	fmt.Println()

	// Create consumer using the client library
	consumer := client.NewConsumer(topic, leaderAddr)

	// Connect and subscribe
	fmt.Println("Connecting to LogStream cluster...")
	if err := consumer.Connect(); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}

	// Setup graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\n\nShutting down consumer...")
		consumer.Close()
		os.Exit(0)
	}()

	// Process incoming results
	fmt.Println("\nSubscribed! Waiting for messages (Ctrl+C to quit):")
	fmt.Println("---")

	for {
		select {
		case result := <-consumer.Results():
			fmt.Printf("[%s] Offset %d: %s\n", result.Topic, result.Offset, string(result.Data))

		case err := <-consumer.Errors():
			log.Printf("Error: %v", err)
		}
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
