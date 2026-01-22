package protocol

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
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
