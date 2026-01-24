package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/milossdjuric/logstream/internal/protocol"
	"github.com/milossdjuric/logstream/internal/storage"
)

func sendLogEntry(senderConn *net.UDPConn, targetAddr *net.UDPAddr, senderID string, offset uint64, data []byte) error {
	// Send as a DataMsg
	msg := protocol.NewDataMsg(senderID, "memory-log-topic", data, 0) // broker handles seq
	return protocol.WriteUDPMessage(senderConn, msg, targetAddr)
}

func main() {
	target := flag.String("target", "127.0.0.1:9999", "target broker address to send to")
	id := flag.String("id", "mem-node-1", "node ID")
	flag.Parse()

	// Initialize in-memory log
	memLog := storage.NewMemoryLog()
	fmt.Printf("[Node %s] Initialized in-memory log\n", *id)

	// Resolve target address
	addr, err := net.ResolveUDPAddr("udp4", *target)
	if err != nil {
		log.Fatal("Invalid target address:", err)
	}

	// Create sender connection
	senderConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		log.Fatal("Failed to create sender conn:", err)
	}
	defer senderConn.Close()

	// Simulate generating logs
	go func() {
		counter := 0
		for {
			msg := fmt.Sprintf("log entry %d from %s", counter, *id)
			off, err := memLog.Append([]byte(msg))
			if err != nil {
				log.Printf("Error appending to memory log: %v\n", err)
			} else {
				fmt.Printf("[Node %s] Stored in memory at offset %d: %s\n", *id, off, msg)
			}
			counter++
			time.Sleep(1 * time.Second)
		}
	}()

	// Simulate sending logs from memory to cluster
	// In a real app, this might track what has been sent successfully.
	// For this demo, we'll just poll the memory log and send everything.

	// Initialize lastSentOffset to -1 (uint max) or handle 0 carefully.
	// Is easiest to keep a separate cursor.
	cursor := uint64(0)

	fmt.Printf("[Node %s] Starting sender loop to %s...\n", *id, *target)

	for {
		// Try to read at cursor
		data, err := memLog.Read(cursor)
		if err != nil {
			// Probably incomplete or end of log, wait a bit
			time.Sleep(500 * time.Millisecond)
			continue
		}

		// Send data
		err = sendLogEntry(senderConn, addr, *id, cursor, data)
		if err != nil {
			log.Printf("Error sending offset %d: %v\n", cursor, err)
			time.Sleep(1 * time.Second) // retry delay
			continue
		}

		fmt.Printf("[Node %s] Sent offset %d to cluster\n", *id, cursor)
		cursor++
	}
}
