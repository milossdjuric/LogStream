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

// GenerateNodeID creates a unique node ID combining MAC, address, timestamp,
// and random bytes, hashed with SHA-256. For clients, use GenerateClientID.
func GenerateNodeID(address string) string {
	macAddr := getInterfaceMAC(address)
	if macAddr == "" {
		// Log warning but continue - use address as fallback component
		fmt.Printf("[WARNING] No MAC address found for %s, using address only for ID generation\n", address)
	}

	timestamp := time.Now().UnixNano()

	randomBytes := make([]byte, 8)
	if _, err := rand.Read(randomBytes); err != nil {
		randomBytes = make([]byte, 8)
		binary.BigEndian.PutUint64(randomBytes, uint64(timestamp))
	}
	randomNum := binary.BigEndian.Uint64(randomBytes)

	// Even if MAC is empty, include it in the format for consistency
	data := fmt.Sprintf("%s-%s-%d-%d", macAddr, address, timestamp, randomNum)

	hash := sha256.Sum256([]byte(data))

	// Return first 8 bytes of hash (16 hex chars), enough to avoid collisions
	return fmt.Sprintf("%x", hash[:8])
}

// GenerateClientID creates a unique ID for producer/consumer clients.
// MAC is optional - if not available, other entropy sources suffice.
func GenerateClientID(clientType string, address string) string {
	macAddr := getPrimaryInterfaceMAC()

	timestamp := time.Now().UnixNano()

	randomBytes := make([]byte, 8)
	if _, err := rand.Read(randomBytes); err != nil {
		randomBytes = make([]byte, 8)
		binary.BigEndian.PutUint64(randomBytes, uint64(timestamp^0xDEADBEEF))
	}
	randomNum := binary.BigEndian.Uint64(randomBytes)

	var data string
	if macAddr != "" {
		data = fmt.Sprintf("%s-%s-%s-%d-%d", clientType, macAddr, address, timestamp, randomNum)
	} else {
		data = fmt.Sprintf("%s-%s-%d-%d", clientType, address, timestamp, randomNum)
	}

	hash := sha256.Sum256([]byte(data))

	// Return first 8 bytes of hash (16 hex chars)
	return fmt.Sprintf("%x", hash[:8])
}

func getInterfaceMAC(address string) string {
	ipStr := address
	if colonIndex := strings.LastIndex(address, ":"); colonIndex != -1 {
		ipStr = address[:colonIndex]
	}

	if ipStr == "0.0.0.0" || ipStr == "" {
		return getPrimaryInterfaceMAC()
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		return ""
	}

	for _, iface := range ifaces {
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
					if len(iface.HardwareAddr) > 0 {
						return iface.HardwareAddr.String() // Format: "aa:bb:cc:dd:ee:ff"
					}
				}
			}
		}
	}

	return ""
}

// First non-loopback interface with IPv4 address
func getPrimaryInterfaceMAC() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return ""
	}

	for _, iface := range ifaces {
		if iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagUp == 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

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
