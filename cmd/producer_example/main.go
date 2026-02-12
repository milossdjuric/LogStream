//go:build example
// +build example

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

func runListener(listenAddr, dataDir string) {
	udpAddr, err := net.ResolveUDPAddr("udp4", listenAddr)
	if err != nil {
		log.Fatal(err)
	}
	conn, err := net.ListenUDP("udp4", udpAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	fmt.Println("Listener: started on", listenAddr)

	l, err := storage.NewLog(dataDir, storage.DefaultMaxSegmentBytes)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	for {
		msg, addr, err := protocol.ReadUDPMessage(conn)
		if err != nil {
			log.Println("listener read error:", err)
			continue
		}
		if dm, ok := msg.(*protocol.DataMsg); ok {
			off, err := l.Append(dm.Data)
			if err != nil {
				log.Println("append error:", err)
			} else {
				fmt.Printf("Listener: appended offset=%d from %s topic=%s data=%s\n", off, addr, dm.Topic, string(dm.Data))
			}
		} else {
			fmt.Println("Listener: received non-data message from", addr)
		}
	}
}

func sendData(targetAddr, senderID, topic string, data []byte) error {
	addr, err := net.ResolveUDPAddr("udp4", targetAddr)
	if err != nil {
		return err
	}
	// local sender conn
	senderConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		return err
	}
	defer senderConn.Close()

	msg := protocol.NewDataMsg(senderID, topic, data, 0)
	return protocol.WriteUDPMessage(senderConn, msg, addr)
}

func main() {
	listen := flag.String("listen", "127.0.0.1:9999", "address to listen on (starts listener)")
	target := flag.String("target", "127.0.0.1:9999", "target broker address to send to")
	data := flag.String("data", "hello from producer", "message payload")
	dir := flag.String("dir", "./data_producer_test", "data dir for listener storage")
	mode := flag.String("mode", "both", "mode: listener|send|both")
	flag.Parse()

	if *mode == "listener" || *mode == "both" {
		go runListener(*listen, *dir)
		// give listener time to start
		time.Sleep(200 * time.Millisecond)
	}

	if *mode == "send" || *mode == "both" {
		if err := sendData(*target, "producer-1", "topicA", []byte(*data)); err != nil {
			log.Fatal("send error:", err)
		}
		fmt.Println("Producer: sent data to", *target)
	}

	// allow time for listener to receive and append
	time.Sleep(500 * time.Millisecond)
}
