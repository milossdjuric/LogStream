package protocol

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"time"
)

// GenerateNodeID creates a unique node ID by combining multiple entropy sources:
// - MAC address of the network interface (hardware identity)
// - IP address and port of the node (network identity)
// - Current timestamp in nanoseconds (temporal uniqueness)
// - 8 random bytes from crypto/rand (additional entropy)
//
// This combination is hashed using SHA-256, and the first 16 hexadecimal characters
// (8 bytes) become the unique ID. Using multiple sources minimizes collision risk
// even when nodes start simultaneously or share network characteristics.
//
// For clients (producer/consumer), use GenerateClientID instead.
func GenerateNodeID(address string) string {
	// Get MAC address from the interface associated with this address
	macAddr := getInterfaceMAC(address)
	if macAddr == "" {
		// Log warning but continue - use address as fallback component
		fmt.Printf("[WARNING] No MAC address found for %s, using address only for ID generation\n", address)
	}

	// Take current timestamp in nanoseconds
	timestamp := time.Now().UnixNano()

	// Generate 8 random bytes for additional entropy
	randomBytes := make([]byte, 8)
	if _, err := rand.Read(randomBytes); err != nil {
		// Fallback to timestamp-based entropy if random fails
		randomBytes = make([]byte, 8)
		binary.BigEndian.PutUint64(randomBytes, uint64(timestamp))
	}
	randomNum := binary.BigEndian.Uint64(randomBytes)

	// Combine ALL sources: MAC + address + timestamp + random
	// Even if MAC is empty, include it in the format for consistency
	data := fmt.Sprintf("%s-%s-%d-%d", macAddr, address, timestamp, randomNum)

	// Use SHA-256 to hash the data and get a hash value
	hash := sha256.Sum256([]byte(data))

	// Return first 8 bytes of hash (16 hex chars), enough to avoid collisions
	return fmt.Sprintf("%x", hash[:8])
}

// GenerateClientID creates a unique ID for producer/consumer clients.
// Uses: client type + MAC address (if available) + IP:port address + timestamp + random bytes,
// hashed with SHA-256.
//
// Unlike broker nodes, MAC is optional for clients - if not available, the ID is still
// generated using the other entropy sources.
func GenerateClientID(clientType string, address string) string {
	// Try to get MAC address from primary interface
	macAddr := getPrimaryInterfaceMAC()

	// Take current timestamp in nanoseconds
	timestamp := time.Now().UnixNano()

	// Generate 8 random bytes for additional entropy
	randomBytes := make([]byte, 8)
	if _, err := rand.Read(randomBytes); err != nil {
		// Fallback to timestamp-based randomness
		randomBytes = make([]byte, 8)
		binary.BigEndian.PutUint64(randomBytes, uint64(timestamp^0xDEADBEEF))
	}
	randomNum := binary.BigEndian.Uint64(randomBytes)

	// Combine all sources: client type + MAC (if available) + address + timestamp + random
	var data string
	if macAddr != "" {
		data = fmt.Sprintf("%s-%s-%s-%d-%d", clientType, macAddr, address, timestamp, randomNum)
	} else {
		data = fmt.Sprintf("%s-%s-%d-%d", clientType, address, timestamp, randomNum)
	}

	// Use SHA-256 to hash the data
	hash := sha256.Sum256([]byte(data))

	// Return first 8 bytes of hash (16 hex chars)
	return fmt.Sprintf("%x", hash[:8])
}

// getInterfaceMAC retrieves the MAC address for the network interface
// associated with the given IP address from the address string (IP:PORT)
func getInterfaceMAC(address string) string {
	// Extract IP from address (format: "IP:PORT")
	ipStr := address
	if colonIndex := strings.LastIndex(address, ":"); colonIndex != -1 {
		ipStr = address[:colonIndex]
	}

	// Special case: if using 0.0.0.0, try to find primary interface
	if ipStr == "0.0.0.0" || ipStr == "" {
		return getPrimaryInterfaceMAC()
	}

	// Get all network interfaces
	ifaces, err := net.Interfaces()
	if err != nil {
		return "" // Fallback to address-based ID
	}

	// Find interface with matching IP
	for _, iface := range ifaces {
		// Skip loopback and down interfaces
		if iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagUp == 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			if ipnet, ok := addr.(*net.IPNet); ok {
				if ipnet.IP.String() == ipStr {
					// Found matching interface, return MAC
					if len(iface.HardwareAddr) > 0 {
						return iface.HardwareAddr.String() // Format: "aa:bb:cc:dd:ee:ff"
					}
				}
			}
		}
	}

	return "" // No matching interface found, will fallback to address
}

// getPrimaryInterfaceMAC returns the MAC address of the primary network interface
// (first non-loopback interface with IPv4 address)
func getPrimaryInterfaceMAC() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return ""
	}

	for _, iface := range ifaces {
		// Skip loopback and down interfaces
		if iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagUp == 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		// Check if interface has IPv4 address
		hasIPv4 := false
		for _, addr := range addrs {
			if ipnet, ok := addr.(*net.IPNet); ok {
				if ipnet.IP.To4() != nil {
					hasIPv4 = true
					break
				}
			}
		}

		if hasIPv4 && len(iface.HardwareAddr) > 0 {
			return iface.HardwareAddr.String()
		}
	}

	return ""
}

// TODO: see if we need it or just use node IDs directly
// Generate a random Election ID
func GenerateElectionID() int64 {
	randomBytes := make([]byte, 8)
	rand.Read(randomBytes)
	return int64(binary.BigEndian.Uint64(randomBytes))
}
