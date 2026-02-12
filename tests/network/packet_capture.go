package network

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"github.com/google/gopacket/pcapgo"
)

type PacketCapture struct {
	handle      *pcap.Handle
	pcapWriter  *pcapgo.Writer
	outputFile  *os.File
	stopChan    chan struct{}
	packetCount int
	stats       CaptureStats
}

type CaptureStats struct {
	TotalPackets     int
	UDPPackets       int
	TCPPackets       int
	MulticastPackets int
	BroadcastPackets int
	HeartbeatPackets int
	ReplicatePackets int
	ElectionPackets  int
	DataPackets      int
}

func NewPacketCapture(interfaceName string, outputFilePath string) (*PacketCapture, error) {
	handle, err := pcap.OpenLive(interfaceName, 65536, true, pcap.BlockForever)
	if err != nil {
		return nil, fmt.Errorf("failed to open device: %w", err)
	}

	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		handle.Close()
		return nil, fmt.Errorf("failed to create output file: %w", err)
	}

	pcapWriter := pcapgo.NewWriter(outputFile)
	if err := pcapWriter.WriteFileHeader(65536, layers.LinkTypeEthernet); err != nil {
		handle.Close()
		outputFile.Close()
		return nil, fmt.Errorf("failed to write pcap header: %w", err)
	}

	return &PacketCapture{
		handle:     handle,
		pcapWriter: pcapWriter,
		outputFile: outputFile,
		stopChan:   make(chan struct{}),
		stats:      CaptureStats{},
	}, nil
}

func (pc *PacketCapture) SetFilter(filter string) error {
	return pc.handle.SetBPFFilter(filter)
}

func (pc *PacketCapture) Start() {
	packetSource := gopacket.NewPacketSource(pc.handle, pc.handle.LinkType())

	fmt.Println("[PacketCapture] Starting packet capture...")
	fmt.Println("[PacketCapture] Press Ctrl+C or call Stop() to end capture")

	go func() {
		for {
			select {
			case <-pc.stopChan:
				return
			case packet := <-packetSource.Packets():
				pc.processPacket(packet)
			}
		}
	}()
}

func (pc *PacketCapture) processPacket(packet gopacket.Packet) {
	pc.packetCount++
	pc.stats.TotalPackets++

	if err := pc.pcapWriter.WritePacket(packet.Metadata().CaptureInfo, packet.Data()); err != nil {
		log.Printf("[PacketCapture] Error writing packet: %v", err)
	}

	pc.analyzePacket(packet)

	if pc.packetCount%100 == 0 {
		fmt.Printf("[PacketCapture] Captured %d packets\n", pc.packetCount)
	}
}

func (pc *PacketCapture) analyzePacket(packet gopacket.Packet) {
	ipLayer := packet.Layer(layers.LayerTypeIPv4)
	if ipLayer == nil {
		return
	}

	ip, _ := ipLayer.(*layers.IPv4)

	if ip.DstIP.IsMulticast() {
		pc.stats.MulticastPackets++
	}

	if ip.DstIP[0] == 255 {
		pc.stats.BroadcastPackets++
	}

	if udpLayer := packet.Layer(layers.LayerTypeUDP); udpLayer != nil {
		pc.stats.UDPPackets++
		pc.analyzeUDPPayload(packet)
	}

	if tcpLayer := packet.Layer(layers.LayerTypeTCP); tcpLayer != nil {
		pc.stats.TCPPackets++
	}
}

func (pc *PacketCapture) analyzeUDPPayload(packet gopacket.Packet) {
	appLayer := packet.ApplicationLayer()
	if appLayer == nil {
		return
	}

	payload := appLayer.Payload()
	if len(payload) < 10 {
		return
	}

	payloadStr := string(payload[:min(len(payload), 100)])
	
	if contains(payloadStr, "HEARTBEAT") {
		pc.stats.HeartbeatPackets++
	} else if contains(payloadStr, "REPLICATE") {
		pc.stats.ReplicatePackets++
	} else if contains(payloadStr, "ELECTION") {
		pc.stats.ElectionPackets++
	} else if contains(payloadStr, "DATA") {
		pc.stats.DataPackets++
	}
}

func (pc *PacketCapture) Stop() {
	close(pc.stopChan)
	time.Sleep(100 * time.Millisecond)
	
	pc.handle.Close()
	pc.outputFile.Close()

	fmt.Println("\n[PacketCapture] Capture stopped")
	pc.PrintStats()
}

func (pc *PacketCapture) PrintStats() {
	fmt.Println("\n========== Capture Statistics ==========")
	fmt.Printf("Total Packets:     %d\n", pc.stats.TotalPackets)
	fmt.Printf("UDP Packets:       %d\n", pc.stats.UDPPackets)
	fmt.Printf("TCP Packets:       %d\n", pc.stats.TCPPackets)
	fmt.Printf("Multicast Packets: %d\n", pc.stats.MulticastPackets)
	fmt.Printf("Broadcast Packets: %d\n", pc.stats.BroadcastPackets)
	fmt.Println("\n--- LogStream Protocol Analysis ---")
	fmt.Printf("Heartbeat Messages:  %d\n", pc.stats.HeartbeatPackets)
	fmt.Printf("Replicate Messages:  %d\n", pc.stats.ReplicatePackets)
	fmt.Printf("Election Messages:   %d\n", pc.stats.ElectionPackets)
	fmt.Printf("Data Messages:       %d\n", pc.stats.DataPackets)
	fmt.Println("========================================")
}

func (pc *PacketCapture) GetStats() CaptureStats {
	return pc.stats
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && s[:len(substr)] == substr
}
