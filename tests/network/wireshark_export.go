package network

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// WiresharkExporter provides functionality to export packet captures
// and generate reports using Wireshark/TShark
type WiresharkExporter struct {
	pcapFile string
}

// NewWiresharkExporter creates a new Wireshark exporter
func NewWiresharkExporter(pcapFile string) *WiresharkExporter {
	return &WiresharkExporter{
		pcapFile: pcapFile,
	}
}

// ExportToCSV exports packet capture to CSV format using tshark
func (w *WiresharkExporter) ExportToCSV(outputFile string, fields []string) error {
	if len(fields) == 0 {
		fields = []string{"frame.number", "frame.time", "ip.src", "ip.dst", "tcp.srcport", "tcp.dstport", "udp.srcport", "udp.dstport", "data.text"}
	}

	args := []string{"-r", w.pcapFile, "-T", "fields"}
	for _, field := range fields {
		args = append(args, "-e", field)
	}
	args = append(args, "-E", "header=y", "-E", "separator=,")

	cmd := exec.Command("tshark", args...)
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("tshark export failed: %w", err)
	}

	return os.WriteFile(outputFile, output, 0644)
}

// GenerateProtocolStats generates protocol distribution statistics
func (w *WiresharkExporter) GenerateProtocolStats(outputFile string) error {
	cmd := exec.Command("tshark", "-r", w.pcapFile, "-q", "-z", "io,phs")
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("tshark stats failed: %w", err)
	}

	return os.WriteFile(outputFile, output, 0644)
}

// GenerateTextReport generates a simple text report with statistics
func (w *WiresharkExporter) GenerateTextReport(outputFile string) error {
	f, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("create file failed: %w", err)
	}
	defer f.Close()

	fmt.Fprintf(f, "LogStream Network Analysis Report\n")
	fmt.Fprintf(f, "==================================\n\n")
	fmt.Fprintf(f, "PCAP File: %s\n\n", filepath.Base(w.pcapFile))

	fmt.Fprintf(f, "Protocol Hierarchy:\n")
	fmt.Fprintf(f, "-------------------\n")
	cmd := exec.Command("tshark", "-r", w.pcapFile, "-q", "-z", "io,phs")
	output, err := cmd.Output()
	if err == nil {
		f.Write(output)
	}
	fmt.Fprintf(f, "\n")

	fmt.Fprintf(f, "Conversation Statistics (UDP):\n")
	fmt.Fprintf(f, "------------------------------\n")
	cmd = exec.Command("tshark", "-r", w.pcapFile, "-q", "-z", "conv,udp")
	output, err = cmd.Output()
	if err == nil {
		f.Write(output)
	}
	fmt.Fprintf(f, "\n")

	fmt.Fprintf(f, "Conversation Statistics (TCP):\n")
	fmt.Fprintf(f, "------------------------------\n")
	cmd = exec.Command("tshark", "-r", w.pcapFile, "-q", "-z", "conv,tcp")
	output, err = cmd.Output()
	if err == nil {
		f.Write(output)
	}
	fmt.Fprintf(f, "\n")

	fmt.Fprintf(f, "Useful Wireshark Display Filters:\n")
	fmt.Fprintf(f, "----------------------------------\n")
	fmt.Fprintf(f, "- Heartbeats:    udp.port == 9999 && data contains \"HEARTBEAT\"\n")
	fmt.Fprintf(f, "- Replication:   udp.port == 9999 && data contains \"REPLICATE\"\n")
	fmt.Fprintf(f, "- Election:      tcp.port == 8001 && data contains \"ELECTION\"\n")
	fmt.Fprintf(f, "- Multicast:     ip.dst >= 224.0.0.0 && ip.dst <= 239.255.255.255\n")
	fmt.Fprintf(f, "- Broadcast:     udp.port == 8888\n")

	return nil
}

// LaunchWireshark launches Wireshark GUI with the capture file
func (w *WiresharkExporter) LaunchWireshark() error {
	cmd := exec.Command("wireshark", w.pcapFile)
	return cmd.Start()
}
