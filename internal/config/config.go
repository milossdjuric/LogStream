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

	// Storage configuration
	MaxRecordsPerTopic int // Max records to keep per topic log (0 = unlimited, default: 10000)

	// Broker timing configuration
	BrokerHeartbeatInterval  int // Leader->follower heartbeat interval in seconds (default: 5)
	ClientHeartbeatInterval  int // Leader->client heartbeat interval in seconds (default: 10)
	BrokerTimeout            int // Seconds before removing dead brokers (default: 30)
	ClientTimeout            int // Seconds before removing dead clients (default: 30)
	FollowerHeartbeatInterval int // Follower->leader heartbeat interval in seconds (default: 2)
	SuspicionTimeout         int // Seconds without leader HB before suspicion (default: 10)
	FailureTimeout           int // Seconds without leader HB before failure (default: 15)
}

func LoadConfig() (*Config, error) {
	cfg := &Config{
		NodeAddress:            getEnv("NODE_ADDRESS", ""),
		MulticastGroup:         getEnv("MULTICAST_GROUP", "239.0.0.1:9999"),
		BroadcastPort:          getEnvInt("BROADCAST_PORT", 8888),
		LeaderAddress:          getEnv("LEADER_ADDRESS", ""), // Optional: bypass broadcast discovery
		AnalyticsWindowSeconds:   getEnvInt("ANALYTICS_WINDOW_SECONDS", 60),
		MaxRecordsPerTopic:      getEnvInt("MAX_RECORDS_PER_TOPIC", 10000),
		BrokerHeartbeatInterval:  getEnvInt("BROKER_HB_INTERVAL", 5),
		ClientHeartbeatInterval:  getEnvInt("CLIENT_HB_INTERVAL", 10),
		BrokerTimeout:            getEnvInt("BROKER_TIMEOUT", 30),
		ClientTimeout:            getEnvInt("CLIENT_TIMEOUT", 20),
		FollowerHeartbeatInterval: getEnvInt("FOLLOWER_HB_INTERVAL", 2),
		SuspicionTimeout:         getEnvInt("SUSPICION_TIMEOUT", 10),
		FailureTimeout:           getEnvInt("FAILURE_TIMEOUT", 15),
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

	if c.MaxRecordsPerTopic < 0 {
		return fmt.Errorf("MAX_RECORDS_PER_TOPIC must be non-negative (got %d)", c.MaxRecordsPerTopic)
	}

	if c.BrokerHeartbeatInterval <= 0 {
		return fmt.Errorf("BROKER_HB_INTERVAL must be positive (got %d)", c.BrokerHeartbeatInterval)
	}
	if c.ClientHeartbeatInterval <= 0 {
		return fmt.Errorf("CLIENT_HB_INTERVAL must be positive (got %d)", c.ClientHeartbeatInterval)
	}
	if c.BrokerTimeout <= 0 {
		return fmt.Errorf("BROKER_TIMEOUT must be positive (got %d)", c.BrokerTimeout)
	}
	if c.ClientTimeout <= 0 {
		return fmt.Errorf("CLIENT_TIMEOUT must be positive (got %d)", c.ClientTimeout)
	}
	if c.FollowerHeartbeatInterval <= 0 {
		return fmt.Errorf("FOLLOWER_HB_INTERVAL must be positive (got %d)", c.FollowerHeartbeatInterval)
	}
	if c.SuspicionTimeout <= 0 {
		return fmt.Errorf("SUSPICION_TIMEOUT must be positive (got %d)", c.SuspicionTimeout)
	}
	if c.FailureTimeout <= 0 {
		return fmt.Errorf("FAILURE_TIMEOUT must be positive (got %d)", c.FailureTimeout)
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
	if c.MaxRecordsPerTopic > 0 {
		fmt.Printf("Max Records/Topic:   %d\n", c.MaxRecordsPerTopic)
	} else {
		fmt.Printf("Max Records/Topic:   unlimited\n")
	}
	fmt.Printf("Broker HB Interval:  %ds\n", c.BrokerHeartbeatInterval)
	fmt.Printf("Client HB Interval:  %ds\n", c.ClientHeartbeatInterval)
	fmt.Printf("Broker Timeout:      %ds\n", c.BrokerTimeout)
	fmt.Printf("Client Timeout:      %ds\n", c.ClientTimeout)
	fmt.Printf("Follower HB Interval:%ds\n", c.FollowerHeartbeatInterval)
	fmt.Printf("Suspicion Timeout:   %ds\n", c.SuspicionTimeout)
	fmt.Printf("Failure Timeout:     %ds\n", c.FailureTimeout)
	fmt.Println("================================")
}

func (c *Config) HasExplicitLeader() bool {
	return c.LeaderAddress != ""
}
