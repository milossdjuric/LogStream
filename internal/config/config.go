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
	MulticastGroup   string
	BroadcastPort    int
	NetworkInterface string

	// Optional: Explicit leader address to bypass broadcast discovery
	// Use this when broadcast doesn't work (e.g., WiFi AP isolation)
	LeaderAddress string

	// Analytics configuration
	AnalyticsWindowSeconds int // Window size in seconds for CountInWindow analytics (default: 60)
}

func LoadConfig() (*Config, error) {
	cfg := &Config{
		NodeAddress:            getEnv("NODE_ADDRESS", ""),
		MulticastGroup:         getEnv("MULTICAST_GROUP", "239.0.0.1:9999"),
		BroadcastPort:          getEnvInt("BROADCAST_PORT", 8888),
		LeaderAddress:          getEnv("LEADER_ADDRESS", ""), // Optional: bypass broadcast discovery
		AnalyticsWindowSeconds: getEnvInt("ANALYTICS_WINDOW_SECONDS", 60),
	}

	if cfg.NodeAddress == "" {
		autoIP, err := AutoDetectIP()
		if err != nil {
			return nil, fmt.Errorf("NODE_ADDRESS not set and auto-detection failed: %w", err)
		}

		// Default port for all nodes (leader determined by election/discovery)
		defaultPort := "8001"
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

	if c.AnalyticsWindowSeconds <= 0 {
		return fmt.Errorf("ANALYTICS_WINDOW_SECONDS must be positive (got %d)", c.AnalyticsWindowSeconds)
	}

	return nil
}

func (c *Config) Print() {
	fmt.Println("=== LogStream Configuration ===")
	fmt.Printf("Node Address:        %s\n", c.NodeAddress)
	fmt.Printf("Multicast Group:     %s\n", c.MulticastGroup)
	fmt.Printf("Broadcast Port:      %d\n", c.BroadcastPort)
	fmt.Printf("Network Interface:   %s\n", c.NetworkInterface)
	if c.LeaderAddress != "" {
		fmt.Printf("Leader Address:      %s (explicit - bypass broadcast)\n", c.LeaderAddress)
	} else {
		fmt.Printf("Leader Address:      (auto-discover via broadcast)\n")
	}
	fmt.Printf("Analytics Window:    %ds\n", c.AnalyticsWindowSeconds)
	fmt.Println("================================")
}

// HasExplicitLeader returns true if an explicit leader address was configured
func (c *Config) HasExplicitLeader() bool {
	return c.LeaderAddress != ""
}
