package protocol

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"net"
	"time"
)

// Create a unique node ID based on address, timestamp, and randomness
func GenerateNodeID(address string) string {
	// Take current timestamp
	timestamp := time.Now().UnixNano()

	// Generate random number
	randomBytes := make([]byte, 8)
	if _, err := rand.Read(randomBytes); err != nil {
		// Fallback to just timestamp if error occurs at randomness generation
		randomBytes = make([]byte, 8)
		binary.BigEndian.PutUint64(randomBytes, uint64(timestamp))
	}
	randomNum := binary.BigEndian.Uint64(randomBytes)

	// Put together address, timestamp, and random number for hashing
	data := fmt.Sprintf("%s-%d-%d", address, timestamp, randomNum)

	// Use SHA-256 to hash the data and get a hash value
	hash := sha256.Sum256([]byte(data))

	// Return first 8 bytes of hash, should be enough to avoid hash collisions
	return fmt.Sprintf("%x", hash[:8])
}

// TODO: see if we need it or just use node IDs directly
// Generate a random Election ID
func GenerateElectionID() int64 {
	randomBytes := make([]byte, 8)
	rand.Read(randomBytes)
	return int64(binary.BigEndian.Uint64(randomBytes))
}

// ShowNetworkInfo displays available network interfaces
func ShowNetworkInfo() {
	fmt.Println("=== Network Interfaces ===")

	interfaces, err := net.Interfaces()
	if err != nil {
		fmt.Printf("Error getting interfaces: %v\n", err)
		return
	}

	var found bool
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
					fmt.Printf("  %s: %s\n", iface.Name, ip.String())
					found = true
				}
			}
		}
	}

	if !found {
		fmt.Println("  No network interfaces found")
	}
	fmt.Println()
}

// isPrivateIP checks if IP is in private range
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
