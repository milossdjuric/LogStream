package network

import (
	"os"
	"testing"
	"time"
)

func TestPacketCapture_Basic(t *testing.T) {
	interfaceName := "lo"
	outputFile := "/tmp/test_capture.pcap"
	
	capture, err := NewPacketCapture(interfaceName, outputFile)
	if err != nil {
		t.Skipf("Packet capture not available (may need sudo): %v", err)
	}
	defer capture.Stop()
	defer os.Remove(outputFile)
	
	err = capture.SetFilter("udp port 9999 or tcp port 8001")
	if err != nil {
		t.Logf("Failed to set filter: %v", err)
	}
	
	capture.Start()
	time.Sleep(2 * time.Second)
	capture.Stop()
	
	stats := capture.GetStats()
	t.Logf("Captured packets: total=%d, udp=%d, tcp=%d, multicast=%d",
		stats.TotalPackets, stats.UDPPackets, stats.TCPPackets, stats.MulticastPackets)
}

func TestWiresharkExport(t *testing.T) {
	pcapFile := "/tmp/test_export.pcap"
	
	exporter := NewWiresharkExporter(pcapFile)
	
	err := exporter.ExportToCSV("/tmp/test_export.csv", nil)
	if err != nil {
		t.Logf("CSV export failed (tshark may not be installed): %v", err)
	}
	
	err = exporter.GenerateProtocolStats("/tmp/test_stats.txt")
	if err != nil {
		t.Logf("Stats generation failed (tshark may not be installed): %v", err)
	}
	
	err = exporter.GenerateTextReport("/tmp/test_report.txt")
	if err != nil {
		t.Logf("Text report generation failed: %v", err)
	}
}

func ExamplePacketCapture() {
	capture, _ := NewPacketCapture("lo", "/tmp/capture.pcap")
	capture.SetFilter("udp port 9999")
	capture.Start()
	time.Sleep(10 * time.Second)
	capture.Stop()
	
	stats := capture.GetStats()
	_ = stats
}

func ExampleWiresharkExporter() {
	exporter := NewWiresharkExporter("/tmp/capture.pcap")
	
	exporter.ExportToCSV("/tmp/capture.csv", []string{"frame.time", "ip.src", "ip.dst"})
	exporter.GenerateProtocolStats("/tmp/stats.txt")
	exporter.GenerateTextReport("/tmp/report.txt")
}
