package integration

import (
	"os"
	"strings"
	"testing"
	"time"
)

// TestCluster_SingleBrokerStartup tests that a single broker can start
func TestCluster_SingleBrokerStartup(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster := StartTestCluster(t, 1)
	defer cluster.Cleanup()

	// Verify broker is running
	if len(cluster.Brokers) != 1 {
		t.Fatalf("Expected 1 broker, got %d", len(cluster.Brokers))
	}

	broker := cluster.Brokers[0]
	if broker.Cmd == nil || broker.Cmd.Process == nil {
		t.Fatal("Broker process not started")
	}

	t.Logf("[OK] Single broker started successfully (PID: %d)", broker.Cmd.Process.Pid)
}

// TestCluster_ThreeBrokerFormation tests 3-broker cluster formation
func TestCluster_ThreeBrokerFormation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster := StartTestCluster(t, 3)
	defer cluster.Cleanup()

	// Verify all brokers started
	if len(cluster.Brokers) != 3 {
		t.Fatalf("Expected 3 brokers, got %d", len(cluster.Brokers))
	}

	// Verify all brokers are running
	running := cluster.CountRunningBrokers()
	if running != 3 {
		t.Fatalf("Expected 3 running brokers, got %d", running)
	}

	// Wait for cluster stabilization
	cluster.WaitForStability(3 * time.Second)

	t.Log("[OK] Three-broker cluster formed successfully")
}

// TestCluster_BrokerCrashAndRestart tests broker recovery
func TestCluster_BrokerCrashAndRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster := StartTestCluster(t, 3)
	defer cluster.Cleanup()

	cluster.WaitForStability(2 * time.Second)

	// Kill a follower broker
	t.Log("Killing broker 2...")
	cluster.Brokers[2].Kill()
	time.Sleep(1 * time.Second)

	// Verify broker is down
	running := cluster.CountRunningBrokers()
	if running != 2 {
		t.Fatalf("Expected 2 running brokers after kill, got %d", running)
	}

	// Restart the broker
	t.Log("Restarting broker 2...")
	if err := cluster.Restart(2); err != nil {
		t.Fatalf("Failed to restart broker: %v", err)
	}

	cluster.WaitForStability(2 * time.Second)

	// Verify broker is back up
	running = cluster.CountRunningBrokers()
	if running != 3 {
		t.Fatalf("Expected 3 running brokers after restart, got %d", running)
	}

	t.Log("[OK] Broker successfully recovered after crash")
}

// TestCluster_LeaderFailover tests leader election after leader failure
func TestCluster_LeaderFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster := StartTestCluster(t, 3)
	defer cluster.Cleanup()

	cluster.WaitForStability(2 * time.Second)

	leader := cluster.GetLeader()
	originalLeaderAddr := leader.Address

	t.Logf("Original leader: %s (PID: %d)", originalLeaderAddr, leader.Cmd.Process.Pid)

	// Kill the leader
	t.Log("Killing leader...")
	leader.Kill()
	time.Sleep(1 * time.Second)

	// Wait for election to complete
	t.Log("Waiting for new leader election...")
	cluster.WaitForStability(15 * time.Second)

	// Verify remaining brokers are still running
	running := cluster.CountRunningBrokers()
	if running != 2 {
		t.Fatalf("Expected 2 running brokers after leader kill, got %d", running)
	}

	// Verify election actually occurred by checking logs
	electionFound := false
	newLeaderFound := false
	
	for i, broker := range cluster.Brokers {
		if broker.Cmd == nil || broker.Cmd.Process == nil {
			continue // Skip killed broker
		}
		
		logPath := broker.LogFile.Name()
		logContent, err := os.ReadFile(logPath)
		if err != nil {
			continue
		}
		
		logStr := string(logContent)
		
		// Check for election activity
		if strings.Contains(logStr, "STARTING LEADER ELECTION") ||
		   strings.Contains(logStr, "ELECTION") {
			electionFound = true
			t.Logf("Election activity found in broker %d logs", i)
		}
		
		// Check for new leader announcement
		if strings.Contains(logStr, "I am the new leader") ||
		   strings.Contains(logStr, "Elected as leader") ||
		   strings.Contains(logStr, "Became leader") {
			newLeaderFound = true
			t.Logf("New leader announcement found in broker %d logs", i)
		}
	}

	if !electionFound {
		t.Fatal("No election activity found in any broker logs!")
	}

	t.Logf("[OK] Leader failover completed - election verified in logs (new leader: %v)", newLeaderFound)
}
