package config

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	NodeAddress      string
	IsLeader         bool // TODO: Replace with leader election algorithm
	MulticastGroup   string
	BroadcastPort    int
	NetworkInterface string
}

func LoadConfig() (*Config, error) {
	cfg := &Config{
		NodeAddress:    getEnv("NODE_ADDRESS", ""),
		IsLeader:       getEnv("IS_LEADER", "false") == "true",
		MulticastGroup: getEnv("MULTICAST_GROUP", "239.0.0.1:9999"),
		BroadcastPort:  getEnvInt("BROADCAST_PORT", 8888),
	}

	if cfg.NodeAddress == "" {
		autoIP, err := AutoDetectIP()
		if err != nil {
			return nil, fmt.Errorf("NODE_ADDRESS not set and auto-detection failed: %w", err)
		}

		defaultPort := "8001"
		if !cfg.IsLeader {
			defaultPort = "8002"
		}

		cfg.NodeAddress = fmt.Sprintf("%s:%s", autoIP, defaultPort)
		fmt.Printf("[Config] Auto-detected address: %s\n", cfg.NodeAddress)
	}

	host, _, err := net.SplitHostPort(cfg.NodeAddress)
	if err != nil {
		return nil, fmt.Errorf("invalid NODE_ADDRESS format (expected IP:PORT): %w", err)
	}
	cfg.NetworkInterface = host

	return cfg, nil
}

func AutoDetectIP() (string, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}

	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}

			if ip == nil || ip.IsLoopback() {
				continue
			}

			if ip.To4() != nil {
				if isPrivateIP(ip) {
					return ip.String(), nil
				}
			}
		}
	}

	return "", fmt.Errorf("no suitable network interface found")
}

func isPrivateIP(ip net.IP) bool {
	privateRanges := []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
	}

	for _, cidr := range privateRanges {
		_, subnet, _ := net.ParseCIDR(cidr)
		if subnet.Contains(ip) {
			return true
		}
	}
	return false
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return defaultValue
}

func (c *Config) Validate() error {
	if c.NodeAddress == "" {
		return fmt.Errorf("NODE_ADDRESS is required")
	}

	if _, _, err := net.SplitHostPort(c.NodeAddress); err != nil {
		return fmt.Errorf("invalid NODE_ADDRESS format: %w", err)
	}

	if !strings.HasPrefix(c.MulticastGroup, "239.") {
		return fmt.Errorf("multicast group must be in 239.x.x.x range")
	}

	return nil
}

func (c *Config) Print() {
	fmt.Println("=== LogStream Configuration ===")
	fmt.Printf("Node Address:      %s\n", c.NodeAddress)
	fmt.Printf("Is Leader:         %v\n", c.IsLeader)
	fmt.Printf("Multicast Group:   %s\n", c.MulticastGroup)
	fmt.Printf("Broadcast Port:    %d\n", c.BroadcastPort)
	fmt.Printf("Network Interface: %s\n", c.NetworkInterface)
	fmt.Println("================================")
}
