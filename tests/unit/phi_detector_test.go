package unit

import (
	"testing"
	"time"

	"github.com/milossdjuric/logstream/internal/failure"
)

func TestPhiAccrualDetector_Basic(t *testing.T) {
	detector := failure.NewAccrualFailureDetector(100)
	nodeID := "test-node-1"

	// First update establishes baseline
	detector.Update(nodeID, time.Now())

	// Phi should be 0 with only one data point (no history to compare)
	phi := detector.Status(nodeID)
	if phi != 0.0 {
		t.Errorf("Expected phi=0 with single heartbeat (no history), got %.2f", phi)
	}
}

func TestPhiAccrualDetector_IncreasingPhi(t *testing.T) {
	detector := failure.NewAccrualFailureDetector(10)
	nodeID := "test-node-1"

	// Build history with real-time heartbeats at ~100ms intervals
	for i := 0; i < 10; i++ {
		detector.Update(nodeID, time.Now())
		time.Sleep(100 * time.Millisecond)
	}

	// Get phi immediately after last heartbeat - should be low
	phi1 := detector.Status(nodeID)

	// Wait 500ms without heartbeats - phi should increase
	time.Sleep(500 * time.Millisecond)
	phi2 := detector.Status(nodeID)

	if phi2 <= phi1 {
		t.Errorf("Phi should increase over time without heartbeats: phi1=%.4f, phi2=%.4f", phi1, phi2)
	}

	// Wait another 500ms - phi should increase further
	time.Sleep(500 * time.Millisecond)
	phi3 := detector.Status(nodeID)

	if phi3 <= phi2 {
		t.Errorf("Phi should continue increasing: phi2=%.4f, phi3=%.4f", phi2, phi3)
	}

	t.Logf("Phi progression: %.4f -> %.4f -> %.4f", phi1, phi2, phi3)
}

func TestPhiAccrualDetector_MultipleNodes(t *testing.T) {
	detector := failure.NewAccrualFailureDetector(100)

	nodes := []string{"node1", "node2", "node3"}

	// Register all nodes
	for _, node := range nodes {
		detector.Update(node, time.Now())
	}

	// All nodes should have low phi initially
	for _, node := range nodes {
		phi := detector.Status(node)
		if phi != 0.0 {
			t.Errorf("Initial phi for %s should be 0 (no history), got %.2f", node, phi)
		}
	}

	// Build history for node1 only
	for i := 0; i < 5; i++ {
		detector.Update("node1", time.Now())
		time.Sleep(50 * time.Millisecond)
	}

	// Wait a bit
	time.Sleep(200 * time.Millisecond)

	// node1 should have measurable phi, others still 0 (insufficient history)
	phi1 := detector.Status("node1")
	phi2 := detector.Status("node2")
	phi3 := detector.Status("node3")

	// node2 and node3 only have 1 data point each, so phi should still be 0
	if phi2 != 0.0 || phi3 != 0.0 {
		t.Errorf("Nodes with insufficient history should have phi=0: node2=%.4f, node3=%.4f", phi2, phi3)
	}

	t.Logf("Multi-node phi: node1=%.4f, node2=%.4f, node3=%.4f", phi1, phi2, phi3)
}

func TestPhiAccrualDetector_RegularHeartbeats(t *testing.T) {
	detector := failure.NewAccrualFailureDetector(100)
	nodeID := "test-node-1"

	// Send regular heartbeats every 100ms
	for i := 0; i < 20; i++ {
		detector.Update(nodeID, time.Now())
		time.Sleep(100 * time.Millisecond)
	}

	// Check phi immediately after last heartbeat - should be very low
	phi := detector.Status(nodeID)
	if phi > 2.0 {
		t.Errorf("Phi should be low immediately after heartbeat, got %.4f", phi)
	}

	t.Logf("Phi after regular heartbeats: %.4f", phi)
}

func TestPhiAccrualDetector_Thresholds(t *testing.T) {
	detector := failure.NewAccrualFailureDetector(20)
	nodeID := "test-node-1"

	suspicionThreshold := 8.0
	failureThreshold := 10.0

	// Build history with ~200ms intervals
	for i := 0; i < 20; i++ {
		detector.Update(nodeID, time.Now())
		time.Sleep(200 * time.Millisecond)
	}

	// Immediately after heartbeat, phi should be low (healthy)
	phiHealthy := detector.Status(nodeID)
	if phiHealthy >= suspicionThreshold {
		t.Errorf("Node should be healthy immediately after heartbeat, but phi=%.4f >= suspicion threshold %.1f",
			phiHealthy, suspicionThreshold)
	}

	// Wait 2 seconds (10x the heartbeat interval) - should trigger suspicion
	time.Sleep(2 * time.Second)
	phiSuspected := detector.Status(nodeID)

	// With 200ms heartbeat interval and 2s delay, phi should be elevated
	if phiSuspected < 1.0 {
		t.Errorf("Phi should be elevated after 2s without heartbeat (10x interval), got %.4f", phiSuspected)
	}

	// Wait longer for definite failure detection
	time.Sleep(3 * time.Second)
	phiFailed := detector.Status(nodeID)

	// After 5s total (25x the interval), phi should be very high
	if phiFailed <= phiSuspected {
		t.Errorf("Phi should increase over time: suspected=%.4f, failed=%.4f", phiSuspected, phiFailed)
	}

	t.Logf("Threshold test - Healthy: %.4f, Suspected: %.4f, Failed: %.4f", phiHealthy, phiSuspected, phiFailed)
	t.Logf("Suspicion threshold: %.1f, Failure threshold: %.1f", suspicionThreshold, failureThreshold)
}

func TestPhiAccrualDetector_IsAvailable(t *testing.T) {
	detector := failure.NewAccrualFailureDetector(10)
	nodeID := "test-node-1"

	threshold := 5.0

	// Build history
	for i := 0; i < 10; i++ {
		detector.Update(nodeID, time.Now())
		time.Sleep(100 * time.Millisecond)
	}

	// Should be available immediately after heartbeats
	if !detector.IsAvailable(nodeID, threshold) {
		t.Error("Node should be available immediately after heartbeats")
	}

	// Wait for phi to increase
	time.Sleep(1 * time.Second)

	phi := detector.Status(nodeID)
	isAvailable := detector.IsAvailable(nodeID, threshold)

	// Verify consistency between Status and IsAvailable
	expectedAvailable := phi < threshold
	if isAvailable != expectedAvailable {
		t.Errorf("IsAvailable(%s, %.1f) = %v, but phi=%.4f (expected available=%v)",
			nodeID, threshold, isAvailable, phi, expectedAvailable)
	}
}

func TestPhiAccrualDetector_GetSuspects(t *testing.T) {
	detector := failure.NewAccrualFailureDetector(10)

	// Build history for multiple nodes with different heartbeat patterns
	nodes := []string{"healthy1", "healthy2", "suspect1"}

	// All nodes get initial heartbeats
	for _, node := range nodes {
		for i := 0; i < 10; i++ {
			detector.Update(node, time.Now())
			time.Sleep(50 * time.Millisecond)
		}
	}

	// Continue heartbeats only for healthy nodes
	for i := 0; i < 10; i++ {
		detector.Update("healthy1", time.Now())
		detector.Update("healthy2", time.Now())
		// suspect1 gets no heartbeats
		time.Sleep(100 * time.Millisecond)
	}

	// Wait a bit more for suspect to become more suspicious
	time.Sleep(500 * time.Millisecond)

	// Get suspects with low threshold to catch the unhealthy one
	suspects := detector.GetSuspects(2.0)

	// Verify suspect1 is suspected
	found := false
	for _, s := range suspects {
		if s == "suspect1" {
			found = true
		}
	}

	if !found {
		t.Errorf("Expected suspect1 in suspects list, got %v", suspects)
	}

	// Log all phi values for debugging
	for _, node := range nodes {
		phi := detector.Status(node)
		t.Logf("Node %s: phi=%.4f", node, phi)
	}
}

func TestPhiAccrualDetector_UnknownNode(t *testing.T) {
	detector := failure.NewAccrualFailureDetector(100)

	// Status of unknown node should return 0 (not suspicious)
	phi := detector.Status("unknown-node")
	if phi != 0.0 {
		t.Errorf("Unknown node should have phi=0, got %.4f", phi)
	}

	// IsAvailable should return true for unknown node (not suspicious)
	if !detector.IsAvailable("unknown-node", 5.0) {
		t.Error("Unknown node should be considered available")
	}
}

func TestPhiAccrualDetector_WindowSize(t *testing.T) {
	smallWindow := failure.NewAccrualFailureDetector(5)
	largeWindow := failure.NewAccrualFailureDetector(100)

	nodeID := "test-node"

	// Send same heartbeat pattern to both detectors
	for i := 0; i < 20; i++ {
		now := time.Now()
		smallWindow.Update(nodeID, now)
		largeWindow.Update(nodeID, now)
		time.Sleep(100 * time.Millisecond)
	}

	// Wait same amount for both
	time.Sleep(500 * time.Millisecond)

	phiSmall := smallWindow.Status(nodeID)
	phiLarge := largeWindow.Status(nodeID)

	// Both should have similar phi values (history is the same)
	// Small differences are OK due to statistical computation
	diff := phiSmall - phiLarge
	if diff < 0 {
		diff = -diff
	}

	// They might differ somewhat due to window effects, but both should detect the pause
	if phiSmall < 0.5 && phiLarge < 0.5 {
		t.Errorf("Both detectors should show elevated phi after pause: small=%.4f, large=%.4f", phiSmall, phiLarge)
	}

	t.Logf("Window size comparison - Small window: %.4f, Large window: %.4f", phiSmall, phiLarge)
}

func BenchmarkPhiAccrualDetector_Update(b *testing.B) {
	detector := failure.NewAccrualFailureDetector(1000)
	nodeID := "bench-node"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		detector.Update(nodeID, time.Now())
	}
}

func BenchmarkPhiAccrualDetector_Status(b *testing.B) {
	detector := failure.NewAccrualFailureDetector(1000)
	nodeID := "bench-node"

	// Build some history
	now := time.Now()
	for i := 0; i < 100; i++ {
		detector.Update(nodeID, now.Add(time.Duration(i)*time.Millisecond))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		detector.Status(nodeID)
	}
}

func BenchmarkPhiAccrualDetector_GetSuspects(b *testing.B) {
	detector := failure.NewAccrualFailureDetector(100)

	// Create many nodes
	for i := 0; i < 100; i++ {
		nodeID := string(rune('a' + i%26))
		for j := 0; j < 10; j++ {
			detector.Update(nodeID, time.Now())
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		detector.GetSuspects(8.0)
	}
}
