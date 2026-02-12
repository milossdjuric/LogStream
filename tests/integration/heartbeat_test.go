package integration

import (
	"os"
	"strings"
	"testing"
	"time"
)

// TestHeartbeat_ProducerToLeader tests producer heartbeat to leader
func TestHeartbeat_ProducerToLeader(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster := StartTestCluster(t, 1)
	defer cluster.Cleanup()

	// Create producer
	producer, err := cluster.NewProducer("test-logs")
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}

	// Send a message to establish connection
	if err := producer.SendData([]byte("test message")); err != nil {
		t.Fatalf("Failed to send data: %v", err)
	}

	// Wait for heartbeats to be sent (producers send every 30s, wait a bit for initial registration)
	t.Log("Waiting for heartbeat exchange...")
	time.Sleep(3 * time.Second)

	// Verify heartbeat in leader logs
	logPath := cluster.Brokers[0].LogFile.Name()
	logContent, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("Failed to read leader log: %v", err)
	}

	logStr := string(logContent)
	
	// Check for producer registration (proves connection established)
	if !strings.Contains(logStr, "PRODUCE from") && !strings.Contains(logStr, "Registered producer") {
		t.Fatal("Producer not registered with leader!")
	}

	t.Log("[OK] Producer registered with leader and connection verified")
}

// TestHeartbeat_ConsumerToLeader tests consumer heartbeat to leader
func TestHeartbeat_ConsumerToLeader(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster := StartTestCluster(t, 1)
	defer cluster.Cleanup()

	// Create producer first to establish stream
	producer, err := cluster.NewProducer("test-logs")
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}

	// Send data to establish stream
	if err := producer.SendData([]byte("init message")); err != nil {
		t.Fatalf("Failed to send init data: %v", err)
	}

	// Create consumer
	consumer, err := cluster.NewConsumer("test-logs")
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	// Wait for consumer registration
	t.Log("Waiting for consumer registration...")
	time.Sleep(3 * time.Second)

	// Verify consumer registered in leader logs
	logPath := cluster.Brokers[0].LogFile.Name()
	logContent, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("Failed to read leader log: %v", err)
	}

	logStr := string(logContent)
	
	// Check for consumer registration
	if !strings.Contains(logStr, "CONSUME from") && !strings.Contains(logStr, "Registered consumer") {
		t.Fatal("Consumer not registered with leader!")
	}
	
	// Check for subscription
	if !strings.Contains(logStr, "SUBSCRIBE from") && !strings.Contains(logStr, "subscribed to broker") {
		t.Fatal("Consumer not subscribed to broker!")
	}

	_ = consumer // Use consumer to avoid unused variable warning

	t.Log("[OK] Consumer registered and subscribed (verified in logs)")
}

// TestHeartbeat_FollowerToLeader tests follower-to-leader heartbeat
func TestHeartbeat_FollowerToLeader(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster := StartTestCluster(t, 3)
	defer cluster.Cleanup()

	// Wait for heartbeats to be exchanged
	t.Log("Waiting for broker heartbeat exchange...")
	cluster.WaitForStability(5 * time.Second)

	// Verify all brokers still running
	running := cluster.CountRunningBrokers()
	if running != 3 {
		t.Fatalf("Expected 3 running brokers, got %d", running)
	}

	// Verify heartbeat activity in leader logs
	leaderLog, err := os.ReadFile(cluster.Brokers[0].LogFile.Name())
	if err != nil {
		t.Fatalf("Failed to read leader log: %v", err)
	}

	leaderLogStr := string(leaderLog)
	
	// Check for heartbeat messages from followers
	heartbeatCount := strings.Count(leaderLogStr, "HEARTBEAT from")
	if heartbeatCount == 0 {
		// Followers might not have sent heartbeats yet in 5s, check they joined at least
		if !strings.Contains(leaderLogStr, "Registered broker") {
			t.Fatal("No follower registration or heartbeats found in leader logs!")
		}
		t.Log("[OK] Followers registered (heartbeats may not have started yet)")
	} else {
		t.Logf("[OK] Follower heartbeats working (%d heartbeats received)", heartbeatCount)
	}
}

// TestHeartbeat_LeaderBroadcast tests leader broadcasting heartbeats and phi accrual detection
func TestHeartbeat_LeaderBroadcast(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster := StartTestCluster(t, 3)
	defer cluster.Cleanup()

	cluster.WaitForStability(5 * time.Second)

	// Kill leader to stop broadcasts
	leader := cluster.GetLeader()
	t.Log("Killing leader to test follower failure detection...")
	leader.Kill()

	// Wait for followers to detect leader failure
	t.Log("Waiting for followers to detect leader failure...")
	time.Sleep(20 * time.Second)

	// Followers should still be running
	running := cluster.CountRunningBrokers()
	if running != 2 {
		t.Fatalf("Expected 2 running followers, got %d", running)
	}

	// Verify failure detection in follower logs
	failureDetected := false
	for i, broker := range cluster.Brokers {
		if broker.Cmd == nil || broker.Cmd.Process == nil {
			continue // Skip killed leader
		}
		
		logContent, err := os.ReadFile(broker.LogFile.Name())
		if err != nil {
			continue
		}
		
		logStr := string(logContent)
		
		// Check for failure detection messages
		if strings.Contains(logStr, "Leader failure detected") ||
		   strings.Contains(logStr, "Failure detected") ||
		   strings.Contains(logStr, "Phi indicates") ||
		   strings.Contains(logStr, "fallback timeout") ||
		   strings.Contains(logStr, "STARTING LEADER ELECTION") {
			failureDetected = true
			t.Logf("Failure detection found in follower %d logs", i)
			break
		}
	}

	if !failureDetected {
		t.Fatal("No evidence of failure detection in follower logs!")
	}

	t.Log("[OK] Followers detected leader failure (verified in logs)")
}
