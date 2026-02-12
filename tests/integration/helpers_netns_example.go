package integration

// This file shows the proposed changes to helpers.go to support network namespaces
// DO NOT use this file directly - it's a reference for modifications to helpers.go

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

// StartTestClusterWithNetns starts N brokers using network namespaces
// This is the modified version that should replace StartTestCluster in helpers.go
func StartTestClusterWithNetns(t *testing.T, numBrokers int, useNetns bool) *TestCluster {
	t.Helper()

	if numBrokers < 1 {
		t.Fatal("numBrokers must be at least 1")
	}

	// Create temp directory for logs
	basedir := fmt.Sprintf("/tmp/logstream-integration-test-%d", time.Now().Unix())
	if err := os.MkdirAll(basedir, 0755); err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	cluster := &TestCluster{
		t:       t,
		Brokers: make([]*TestBroker, 0, numBrokers),
		basedir: basedir,
	}

	// Add cleanup for base directory
	cluster.cleanup = append(cluster.cleanup, func() {
		os.RemoveAll(basedir)
	})

	// Setup network namespaces if requested
	var netnsConfig *NetnsConfig
	if useNetns {
		netnsConfig = DefaultNetnsConfig()
		if err := EnsureNetnsSetup(netnsConfig); err != nil {
			t.Fatalf("Failed to setup network namespaces: %v", err)
		}
		t.Logf("[OK] Network namespaces ready")
		
		// Add cleanup for netns processes
		cluster.cleanup = append(cluster.cleanup, func() {
			CleanupNetnsProcesses(netnsConfig)
		})
		
		// Test connectivity
		if err := TestNetnsConnectivity(netnsConfig); err != nil {
			t.Logf("Warning: Network namespace connectivity test failed: %v", err)
		}
	}

	// Start brokers
	for i := 0; i < numBrokers; i++ {
		var addr string
		var nsName string
		
		if useNetns {
			// Use network namespace IPs
			nsName = GetNetnsName(i)
			addr = GetNetnsAddress(i, netnsConfig.BaseIP, 8001)
		} else {
			// Use localhost (may not work for multi-broker)
			basePort := 9001
			addr = fmt.Sprintf("127.0.0.1:%d", basePort+i)
		}
		
		broker := cluster.startBrokerNetns(addr, nsName, i == 0, useNetns)
		cluster.Brokers = append(cluster.Brokers, broker)

		// Wait between starts to ensure proper ordering
		time.Sleep(500 * time.Millisecond)
	}

	// Wait for cluster to stabilize
	time.Sleep(2 * time.Second)

	t.Logf("[OK] Test cluster started: %d brokers", len(cluster.Brokers))
	return cluster
}

// startBrokerNetns starts a single broker process, optionally in a network namespace
func (c *TestCluster) startBrokerNetns(address string, nsName string, isLeader bool, useNetns bool) *TestBroker {
	c.t.Helper()

	// Create log file
	logName := address
	if useNetns {
		logName = nsName
	}
	logPath := fmt.Sprintf("%s/broker-%s.log", c.basedir, logName)
	logFile, err := os.Create(logPath)
	if err != nil {
		c.t.Fatalf("Failed to create log file: %v", err)
	}

	// Find logstream binary
	logstreamPath := "../../logstream"
	if _, err := os.Stat(logstreamPath); os.IsNotExist(err) {
		c.t.Fatalf("logstream binary not found at %s - run: go build -o logstream main.go", logstreamPath)
	}

	// Prepare environment variables
	env := []string{
		fmt.Sprintf("NODE_ADDRESS=%s", address),
		"MULTICAST_GROUP=239.0.0.1:9999",
		"BROADCAST_PORT=8888",
	}

	var cmd *exec.Cmd
	
	if useNetns {
		// Start in network namespace using ip netns exec
		// Note: This requires sudo
		cmd = exec.Command("sudo", "ip", "netns", "exec", nsName, logstreamPath)
		cmd.Env = env
		c.t.Logf("  Starting broker in netns %s at %s", nsName, address)
	} else {
		// Start normally on localhost
		cmd = exec.Command(logstreamPath)
		cmd.Env = append(os.Environ(), env...)
		c.t.Logf("  Starting broker at %s (localhost)", address)
	}

	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true, // Create new process group for clean killing
	}

	if err := cmd.Start(); err != nil {
		logFile.Close()
		c.t.Fatalf("Failed to start broker: %v", err)
	}

	broker := &TestBroker{
		Address:  address,
		Cmd:      cmd,
		LogFile:  logFile,
		isLeader: isLeader,
	}

	// Add cleanup
	c.cleanup = append(c.cleanup, func() {
		broker.Stop()
	})

	c.t.Logf("  Started broker at %s (leader: %v, PID: %d)", address, isLeader, cmd.Process.Pid)

	// Wait for TCP listener to be ready
	if err := c.waitForTCPReady(address, 30*time.Second); err != nil {
		c.t.Fatalf("TCP listener never became ready: %v", err)
	}

	return broker
}

// Example test showing how to use network namespaces
func ExampleTest_WithNetns(t *testing.T) {
	// Check if we can use netns
	if !IsNetnsAvailable() {
		t.Skip("Network namespaces not available (Linux + sudo required)")
	}
	
	if os.Geteuid() != 0 {
		t.Skip("This test requires sudo: run with 'sudo -E go test'")
	}

	// Start cluster with network namespaces
	cluster := StartTestClusterWithNetns(t, 3, true)
	defer cluster.Cleanup()

	// Now your tests can proceed with proper network isolation
	// All brokers have real IPs (172.20.0.10, 172.20.0.20, 172.20.0.30)
	// UDP multicast will work correctly
	
	t.Log("Cluster started successfully with network namespaces!")
}

// Example test showing fallback to localhost
func ExampleTest_WithoutNetns(t *testing.T) {
	// Start cluster without network namespaces (localhost only)
	// This may fail for multi-broker tests due to UDP multicast limitations
	cluster := StartTestClusterWithNetns(t, 1, false)
	defer cluster.Cleanup()

	t.Log("Single broker started on localhost")
}
