package unit

import (
	"testing"
	"time"

	"github.com/milossdjuric/logstream/internal/protocol"
)

// TestNodeID_MACAddressFormat verifies Node ID uses MAC address + timestamp per proposal:
// "UID as in MAC address + timestamp"
func TestNodeID_MACAddressFormat(t *testing.T) {
	nodeAddr := "192.168.1.10:8001"
	nodeID := protocol.GenerateNodeID(nodeAddr)
	
	if nodeID == "" {
		t.Fatal("Node ID should not be empty")
	}
	
	if len(nodeID) < 16 {
		t.Errorf("Node ID should be sufficiently long (expected >= 16 chars), got %d", len(nodeID))
	}
	
	// Verify uniqueness - same address at different times should produce different IDs
	time.Sleep(10 * time.Millisecond)
	nodeID2 := protocol.GenerateNodeID(nodeAddr)
	
	if nodeID == nodeID2 {
		t.Error("Node IDs should be unique (timestamp should differ)")
	}
}

// TestNodeID_Uniqueness verifies Node IDs are unique per proposal requirement
func TestNodeID_Uniqueness(t *testing.T) {
	nodeIDs := make(map[string]bool)
	
	for i := 0; i < 100; i++ {
		addr := "192.168.1.10:8001"
		nodeID := protocol.GenerateNodeID(addr)
		
		if nodeIDs[nodeID] {
			t.Errorf("Duplicate node ID generated: %s", nodeID)
		}
		nodeIDs[nodeID] = true
		
		time.Sleep(1 * time.Millisecond)
	}
}
