package integration

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// NetnsConfig holds configuration for network namespace testing
type NetnsConfig struct {
	Enabled      bool
	SetupScript  string
	CleanupScript string
	Namespaces   []string
	BaseIP       string
}

// DefaultNetnsConfig returns default network namespace configuration
func DefaultNetnsConfig() *NetnsConfig {
	// Detect project root (3 levels up from tests/integration)
	projectRoot, _ := filepath.Abs("../..")
	
	return &NetnsConfig{
		Enabled:       true,
		SetupScript:   filepath.Join(projectRoot, "deploy/netns/setup-netns.sh"),
		CleanupScript: filepath.Join(projectRoot, "deploy/netns/cleanup.sh"),
		Namespaces:    []string{"logstream-a", "logstream-b", "logstream-c"},
		BaseIP:        "172.20.0",
	}
}

// IsNetnsAvailable checks if network namespaces are available on this system
func IsNetnsAvailable() bool {
	// Check if ip netns command exists
	cmd := exec.Command("ip", "netns", "list")
	if err := cmd.Run(); err != nil {
		return false
	}
	
	// Check if /proc/self/ns/net exists (kernel support)
	if _, err := os.Stat("/proc/self/ns/net"); os.IsNotExist(err) {
		return false
	}
	
	return true
}

// EnsureNetnsSetup ensures network namespaces are set up and ready
func EnsureNetnsSetup(config *NetnsConfig) error {
	if !config.Enabled {
		return nil
	}
	
	// Check if running with sudo
	if os.Geteuid() != 0 {
		return fmt.Errorf("network namespace tests require sudo: run with 'sudo -E go test'")
	}
	
	// Check if ip command exists
	if !IsNetnsAvailable() {
		return fmt.Errorf("network namespaces not available on this system (Linux only)")
	}
	
	// Check if namespaces already exist
	cmd := exec.Command("ip", "netns", "list")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to list namespaces: %w", err)
	}
	
	// If logstream-a exists, assume setup is done
	if strings.Contains(string(output), "logstream-a") {
		return nil
	}
	
	// Namespaces don't exist, run setup script
	if _, err := os.Stat(config.SetupScript); os.IsNotExist(err) {
		return fmt.Errorf("setup script not found: %s", config.SetupScript)
	}
	
	fmt.Printf("[netns] Setting up network namespaces...\n")
	cmd = exec.Command("bash", config.SetupScript)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to setup namespaces: %w", err)
	}
	
	fmt.Printf("[netns] Network namespaces ready\n")
	return nil
}

// CleanupNetnsProcesses kills all processes in network namespaces but keeps namespaces alive
func CleanupNetnsProcesses(config *NetnsConfig) error {
	if !config.Enabled {
		return nil
	}
	
	for _, ns := range config.Namespaces {
		// Get PIDs in namespace
		cmd := exec.Command("ip", "netns", "pids", ns)
		output, err := cmd.CombinedOutput()
		if err != nil {
			// Namespace might not exist, skip
			continue
		}
		
		// Kill each PID
		pids := strings.Fields(string(output))
		for _, pid := range pids {
			exec.Command("kill", "-9", pid).Run()
		}
	}
	
	return nil
}

// GetNetnsName returns the namespace name for a broker index
func GetNetnsName(index int) string {
	names := []string{"logstream-a", "logstream-b", "logstream-c"}
	if index >= len(names) {
		return fmt.Sprintf("logstream-%d", index)
	}
	return names[index]
}

// GetNetnsIP returns the IP address for a broker index
func GetNetnsIP(index int, baseIP string) string {
	// logstream-a: 172.20.0.10
	// logstream-b: 172.20.0.20
	// logstream-c: 172.20.0.30
	offset := 10 + (index * 10)
	return fmt.Sprintf("%s.%d", baseIP, offset)
}

// GetNetnsAddress returns the full address (IP:PORT) for a broker index
func GetNetnsAddress(index int, baseIP string, port int) string {
	ip := GetNetnsIP(index, baseIP)
	return fmt.Sprintf("%s:%d", ip, port)
}

// RunInNetns executes a command in a network namespace
func RunInNetns(ns string, cmd *exec.Cmd) *exec.Cmd {
	// Wrap command with ip netns exec
	args := append([]string{"ip", "netns", "exec", ns}, cmd.Path)
	args = append(args, cmd.Args[1:]...)
	
	wrappedCmd := exec.Command("sudo", args...)
	wrappedCmd.Env = cmd.Env
	wrappedCmd.Stdout = cmd.Stdout
	wrappedCmd.Stderr = cmd.Stderr
	wrappedCmd.SysProcAttr = cmd.SysProcAttr
	
	return wrappedCmd
}

// TestNetnsConnectivity tests if network namespaces can communicate
func TestNetnsConnectivity(config *NetnsConfig) error {
	if !config.Enabled || len(config.Namespaces) < 2 {
		return nil
	}
	
	// Try to ping from first namespace to second
	nsA := config.Namespaces[0]
	ipB := GetNetnsIP(1, config.BaseIP)
	
	cmd := exec.Command("ip", "netns", "exec", nsA, "ping", "-c", "1", "-W", "1", ipB)
	output, err := cmd.CombinedOutput()
	
	if err != nil {
		return fmt.Errorf("connectivity test failed: %w\nOutput: %s", err, string(output))
	}
	
	return nil
}

// PrintNetnsInfo prints information about network namespace setup (for debugging)
func PrintNetnsInfo() {
	fmt.Println("=== Network Namespace Information ===")
	
	// List namespaces
	cmd := exec.Command("ip", "netns", "list")
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Printf("Namespaces: ERROR - %v\n", err)
	} else {
		fmt.Printf("Namespaces:\n%s\n", string(output))
	}
	
	// Show bridge
	cmd = exec.Command("ip", "link", "show", "br-logstream")
	output, err = cmd.CombinedOutput()
	if err != nil {
		fmt.Printf("Bridge: ERROR - %v\n", err)
	} else {
		fmt.Printf("Bridge:\n%s\n", string(output))
	}
	
	// Show IPs in each namespace
	for _, ns := range []string{"logstream-a", "logstream-b", "logstream-c"} {
		cmd = exec.Command("ip", "netns", "exec", ns, "ip", "addr", "show", "veth-"+string(ns[10]))
		output, err = cmd.CombinedOutput()
		if err != nil {
			fmt.Printf("%s: ERROR - %v\n", ns, err)
		} else {
			fmt.Printf("%s:\n%s\n", ns, string(output))
		}
	}
	
	fmt.Println("=====================================")
}
