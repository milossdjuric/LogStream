package integration

import (
	"os"
	"strings"
	"testing"
	"time"
)

// TestDiagnostic_SingleBrokerProducerConnection diagnoses why producer can't connect
func TestDiagnostic_SingleBrokerProducerConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping diagnostic test in short mode")
	}

	t.Log("========================================")
	t.Log("DIAGNOSTIC TEST: Single Broker + Producer Connection")
	t.Log("========================================")

	// Start single broker (becomes leader)
	cluster := StartTestCluster(t, 1)
	defer cluster.Cleanup()

	broker := cluster.Brokers[0]
	t.Logf("[OK] Broker started at %s (PID: %d)", broker.Address, broker.Cmd.Process.Pid)

	// Wait extra time for broker to fully initialize
	t.Log("Waiting 5 seconds for broker to fully initialize...")
	time.Sleep(5 * time.Second)

	// Read broker log to verify it's ready
	logContent, err := os.ReadFile(broker.LogFile.Name())
	if err != nil {
		t.Fatalf("Failed to read broker log: %v", err)
	}
	logStr := string(logContent)

	// Check critical initialization steps
	t.Log("\n--- Broker Initialization Checks ---")
	
	checks := []struct {
		name    string
		pattern string
		required bool
	}{
		{"Node started", "Starting node at", true},
		{"Becoming leader", "Becoming LEADER", true},
		{"Broker registered", "RegisterBroker", false}, // May not log explicitly
		{"Ring synced", "Broker ring synced", true},
		{"Multicast setup", "Multicast", false},
		{"TCP listener", "TCP listener started", false},
		{"UDP DATA listener", "UDP DATA listener", false},
		{"Leader duties", "Starting leader duties", true},
	}

	passedChecks := 0
	for _, check := range checks {
		found := strings.Contains(logStr, check.pattern)
		status := "[OK]"
		if !found {
			if check.required {
				status = "[X] MISSING"
			} else {
				status = "- not found (may be OK)"
			}
		} else {
			passedChecks++
		}
		t.Logf("  [%s] %s", status, check.name)
	}
	t.Logf("Passed %d/%d critical checks\n", passedChecks, len(checks))

	// Check cluster state
	t.Log("\n--- Checking Broker Self-Registration ---")
	
	// Look for evidence of broker in its own cluster state
	if strings.Contains(logStr, "Broker ring synced with") {
		// Extract the number
		lines := strings.Split(logStr, "\n")
		for _, line := range lines {
			if strings.Contains(line, "Broker ring synced with") {
				t.Logf("  Found: %s", strings.TrimSpace(line))
				if strings.Contains(line, "synced with 0 brokers") {
					t.Log("  [!]️  WARNING: Ring has 0 brokers! This is the problem!")
					t.Log("  [!]️  Broker didn't add itself to the hash ring")
				} else if strings.Contains(line, "synced with 1 brokers") {
					t.Log("  [OK] Ring has 1 broker (itself) - this is correct")
				}
				break
			}
		}
	}

	// Now try to create a producer
	t.Log("\n--- Attempting Producer Connection ---")
	t.Logf("Producer will connect to leader at: %s", broker.Address)
	
	producer, err := cluster.NewProducer("test-topic")
	if err != nil {
		t.Logf("[X] Producer connection FAILED: %v", err)
		
		// Analyze the failure
		t.Log("\n--- Failure Analysis ---")
		
		// Re-read broker log to see what happened during PRODUCE request
		time.Sleep(500 * time.Millisecond)
		logContent, _ = os.ReadFile(broker.LogFile.Name())
		logStr = string(logContent)
		
		// Look for PRODUCE request handling
		if strings.Contains(logStr, "<- PRODUCE from") {
			t.Log("  [OK] Broker received PRODUCE request")
			
			// Check what happened next
			if strings.Contains(logStr, "PRODUCE_ACK") {
				t.Log("  [OK] Broker sent PRODUCE_ACK")
				
				// Check if broker address was assigned
				if strings.Contains(logStr, "assigned broker:") {
					t.Log("  [OK] Broker assigned itself")
					// Extract the line
					lines := strings.Split(logStr, "\n")
					for _, line := range lines {
						if strings.Contains(line, "assigned broker:") {
							t.Logf("    %s", strings.TrimSpace(line))
						}
					}
				} else {
					t.Log("  [X] No broker assignment found in response")
				}
			} else {
				t.Log("  [X] No PRODUCE_ACK sent")
			}
			
			// Check for errors
			if strings.Contains(logStr, "Failed to assign broker") {
				t.Log("  [X] Error: Failed to assign broker")
				lines := strings.Split(logStr, "\n")
				for _, line := range lines {
					if strings.Contains(line, "Failed to assign broker") {
						t.Logf("    %s", strings.TrimSpace(line))
					}
				}
			}
			
			if strings.Contains(logStr, "no brokers available") {
				t.Log("  [X] Error: No brokers available (hash ring empty!)")
			}
			
		} else {
			t.Log("  [X] Broker never received PRODUCE request")
			t.Log("     Possible issues:")
			t.Log("     - TCP listener not started")
			t.Log("     - Producer couldn't connect to TCP port")
			t.Log("     - TCP connection timeout")
		}
		
		// Show last 30 lines of broker log for context
		t.Log("\n--- Last 30 Lines of Broker Log ---")
		lines := strings.Split(logStr, "\n")
		start := len(lines) - 30
		if start < 0 {
			start = 0
		}
		for _, line := range lines[start:] {
			if strings.TrimSpace(line) != "" {
				t.Logf("  %s", line)
			}
		}
		
		t.Fatal("Producer connection failed - see analysis above")
	} else {
		t.Log("[OK] Producer connected successfully!")
		
		// Send a test message
		t.Log("\n--- Sending Test Message ---")
		err = producer.SendData([]byte("diagnostic test message"))
		if err != nil {
			t.Fatalf("Failed to send data: %v", err)
		}
		t.Log("[OK] Message sent successfully")
	}
}

// TestDiagnostic_LeaderFailureDetection diagnoses why followers don't detect leader failure
func TestDiagnostic_LeaderFailureDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping diagnostic test in short mode")
	}

	t.Log("========================================")
	t.Log("DIAGNOSTIC TEST: Leader Failure Detection")
	t.Log("========================================")

	// Start 3-broker cluster
	cluster := StartTestCluster(t, 3)
	defer cluster.Cleanup()

	t.Log("[OK] Cluster started with 3 brokers")
	
	// Wait for cluster to stabilize
	t.Log("Waiting 5 seconds for cluster stabilization...")
	time.Sleep(5 * time.Second)

	// Check broker logs for heartbeat activity
	t.Log("\n--- Checking Leader Heartbeat Activity ---")
	
	leaderLog, err := os.ReadFile(cluster.Brokers[0].LogFile.Name())
	if err != nil {
		t.Fatalf("Failed to read leader log: %v", err)
	}
	leaderLogStr := string(leaderLog)
	
	heartbeatsSent := strings.Count(leaderLogStr, "-> HEARTBEAT")
	t.Logf("  Leader heartbeats sent: %d", heartbeatsSent)
	
	if heartbeatsSent == 0 {
		t.Log("  [X] WARNING: Leader hasn't sent any heartbeats!")
		t.Log("  This means followers have nothing to monitor")
	} else {
		t.Logf("  [OK] Leader is sending heartbeats (found %d instances)", heartbeatsSent)
	}

	// Check follower logs for heartbeat reception
	t.Log("\n--- Checking Follower Heartbeat Reception ---")
	
	for i := 1; i < 3; i++ {
		followerLog, err := os.ReadFile(cluster.Brokers[i].LogFile.Name())
		if err != nil {
			continue
		}
		followerLogStr := string(followerLog)
		
		heartbeatsReceived := strings.Count(followerLogStr, "<- HEARTBEAT from")
		phiChecks := strings.Count(followerLogStr, "Phi accrual detector check")
		
		t.Logf("  Follower %d:", i)
		t.Logf("    Heartbeats received: %d", heartbeatsReceived)
		t.Logf("    Phi checks performed: %d", phiChecks)
		
		if heartbeatsReceived == 0 {
			t.Logf("    [X] WARNING: Follower hasn't received any heartbeats!")
			
			// Check if multicast is working
			if strings.Contains(followerLogStr, "Multicast") {
				t.Log("    Multicast listener mentioned in logs")
			} else {
				t.Log("    [X] No multicast setup found in logs")
			}
		}
		
		if phiChecks > 0 {
			// Show a sample phi check
			lines := strings.Split(followerLogStr, "\n")
			for j, line := range lines {
				if strings.Contains(line, "Phi accrual detector check") {
					// Print this check and next few lines
					t.Log("    Sample phi check:")
					for k := j; k < j+10 && k < len(lines); k++ {
						t.Logf("      %s", lines[k])
					}
					break
				}
			}
		}
	}

	// Now kill the leader
	t.Log("\n--- Killing Leader ---")
	leader := cluster.GetLeader()
	leaderPID := leader.Cmd.Process.Pid
	t.Logf("Killing leader (PID: %d)...", leaderPID)
	leader.Kill()
	t.Log("Leader killed")

	// Wait and monitor followers
	t.Log("\n--- Monitoring Followers for Failure Detection ---")
	t.Log("Waiting 25 seconds and checking for failure detection...")
	
	for i := 0; i < 25; i++ {
		time.Sleep(1 * time.Second)
		t.Logf("  [%2ds] Checking follower logs...", i+1)
		
		// Check both followers
		for j := 1; j < 3; j++ {
			followerLog, err := os.ReadFile(cluster.Brokers[j].LogFile.Name())
			if err != nil {
				continue
			}
			followerLogStr := string(followerLog)
			
			// Look for failure detection
			if strings.Contains(followerLogStr, "LEADER FAILURE DETECTED") {
				t.Logf("  [OK] Follower %d detected leader failure at t=%ds!", j, i+1)
				
				// Show the detection message
				lines := strings.Split(followerLogStr, "\n")
				for k, line := range lines {
					if strings.Contains(line, "LEADER FAILURE DETECTED") {
						t.Log("  Detection message:")
						for m := k; m < k+10 && m < len(lines); m++ {
							t.Logf("    %s", lines[m])
						}
						break
					}
				}
				return // Test succeeded!
			}
		}
	}

	// If we get here, detection didn't happen
	t.Log("\n--- Failure Detection FAILED ---")
	t.Log("No follower detected leader failure within 25 seconds")
	
	// Dump final state of followers
	t.Log("\n--- Final Follower State ---")
	for i := 1; i < 3; i++ {
		followerLog, err := os.ReadFile(cluster.Brokers[i].LogFile.Name())
		if err != nil {
			continue
		}
		followerLogStr := string(followerLog)
		
		t.Logf("\nFollower %d - Last 40 lines:", i)
		lines := strings.Split(followerLogStr, "\n")
		start := len(lines) - 40
		if start < 0 {
			start = 0
		}
		for _, line := range lines[start:] {
			if strings.TrimSpace(line) != "" {
				t.Logf("  %s", line)
			}
		}
	}
	
	t.Fatal("Followers did not detect leader failure - see logs above")
}

// TestDiagnostic_BrokerRingState checks the broker ring state
func TestDiagnostic_BrokerRingState(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping diagnostic test in short mode")
	}

	t.Log("========================================")
	t.Log("DIAGNOSTIC TEST: Broker Ring State")
	t.Log("========================================")

	cluster := StartTestCluster(t, 1)
	defer cluster.Cleanup()

	t.Log("[OK] Single broker started")
	
	// Wait for broker to complete startup
	// Discovery takes ~3-4 seconds (multiple attempts with delays)
	// Leader initialization adds another ~1 second
	// Total: ~6-7 seconds minimum
	t.Log("Waiting 8 seconds for broker to complete discovery and become leader...")
	time.Sleep(8 * time.Second)
	
	// Force log file sync
	if err := cluster.Brokers[0].LogFile.Sync(); err != nil {
		t.Logf("Warning: failed to sync log file: %v", err)
	}

	// Read log to check ring state
	logFileName := cluster.Brokers[0].LogFile.Name()
	t.Logf("Reading log file: %s", logFileName)
	logContent, err := os.ReadFile(logFileName)
	if err != nil {
		t.Fatalf("Failed to read log: %v", err)
	}
	logStr := string(logContent)
	t.Logf("Log file size: %d bytes", len(logContent))
	
	// Show FULL log for debugging
	t.Logf("FULL LOG CONTENT:\n%s", logStr)
	
	// Check if key phrases exist
	t.Logf("Contains 'Becoming LEADER': %v", strings.Contains(logStr, "Becoming LEADER"))
	t.Logf("Contains 'Broker ring synced': %v", strings.Contains(logStr, "Broker ring synced"))
	t.Logf("Contains 'Starting leader duties': %v", strings.Contains(logStr, "Starting leader duties"))
	t.Logf("Contains 'DEBUG-STARTUP-E': %v", strings.Contains(logStr, "DEBUG-STARTUP-E"))
	t.Logf("Contains 'DEBUG-STARTUP-F': %v", strings.Contains(logStr, "DEBUG-STARTUP-F"))
	t.Logf("Contains 'DEBUG-DISCOVERY': %v", strings.Contains(logStr, "DEBUG-DISCOVERY"))

	t.Log("\n--- Broker Ring Synchronization ---")
	
	// Find all ring sync messages
	lines := strings.Split(logStr, "\n")
	syncCount := 0
	for _, line := range lines {
		if strings.Contains(line, "Broker ring synced") {
			syncCount++
			t.Logf("  [Sync #%d] %s", syncCount, strings.TrimSpace(line))
		}
	}
	
	if syncCount == 0 {
		t.Fatal("[X] No ring synchronization found - broker didn't sync ring!")
	}

	t.Log("\n--- Broker Registration ---")
	
	// Look for RegisterBroker calls
	registrationFound := false
	for _, line := range lines {
		// The RegisterBroker call might not log explicitly, but we can check for related messages
		if strings.Contains(line, "Becoming LEADER") {
			t.Logf("  Found: %s", strings.TrimSpace(line))
			registrationFound = true
		}
	}
	
	if !registrationFound {
		t.Log("  [!]️  No explicit broker registration log found")
		t.Log("     This is OK if the broker became leader")
	}

	t.Log("\n[OK] Diagnostic complete - check output above for issues")
}
