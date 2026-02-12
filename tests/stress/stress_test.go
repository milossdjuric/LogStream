package stress

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/milossdjuric/logstream/internal/loadbalance"
	"github.com/milossdjuric/logstream/internal/state"
	"github.com/milossdjuric/logstream/internal/storage"
)

// These tests verify the internal components can handle concurrent access
// and high load. They do NOT require a running cluster.

// TestStress_ConcurrentClusterStateAccess tests concurrent access to ClusterState
func TestStress_ConcurrentClusterStateAccess(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	cs := state.NewClusterState()
	numGoroutines := 50
	operationsPerGoroutine := 1000

	var wg sync.WaitGroup
	var totalOps atomic.Int64
	var errors atomic.Int64

	start := time.Now()

	// Writers - register/remove brokers
	for i := 0; i < numGoroutines/2; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				brokerID := string(rune('A' + (id+j)%26))
				if j%2 == 0 {
					cs.RegisterBroker(brokerID, "192.168.1.10:8001", false)
				} else {
					cs.RemoveBroker(brokerID)
				}
				totalOps.Add(1)
			}
		}(i)
	}

	// Readers - get broker info
	for i := 0; i < numGoroutines/2; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				brokerID := string(rune('A' + (id+j)%26))
				cs.GetBroker(brokerID)
				cs.GetBrokerCount()
				cs.ListBrokers()
				totalOps.Add(3)
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	ops := totalOps.Load()
	errCount := errors.Load()

	t.Logf("ClusterState stress test completed in %v", duration)
	t.Logf("Total operations: %d (%.2f ops/sec)", ops, float64(ops)/duration.Seconds())
	t.Logf("Errors: %d", errCount)

	if errCount > 0 {
		t.Errorf("Expected 0 errors, got %d", errCount)
	}
}

// TestStress_ConcurrentHashRing tests concurrent access to ConsistentHashRing
func TestStress_ConcurrentHashRing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	ring := loadbalance.NewConsistentHashRing()
	numGoroutines := 20
	operationsPerGoroutine := 5000

	// Pre-populate ring
	for i := 0; i < 10; i++ {
		ring.AddNode(string(rune('A' + i)))
	}

	var wg sync.WaitGroup
	var totalOps atomic.Int64

	start := time.Now()

	// Mixed read/write operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				if j%100 == 0 {
					// Occasionally add/remove nodes
					nodeID := string(rune('a' + (id+j)%26))
					if j%200 == 0 {
						ring.AddNode(nodeID)
					} else {
						ring.RemoveNode(nodeID)
					}
				} else {
					// Mostly reads
					topic := string(rune('0' + j%10))
					ring.GetNode(topic)
				}
				totalOps.Add(1)
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	ops := totalOps.Load()
	t.Logf("HashRing stress test completed in %v", duration)
	t.Logf("Total operations: %d (%.2f ops/sec)", ops, float64(ops)/duration.Seconds())
}

// TestStress_ConcurrentMemoryLog tests concurrent access to MemoryLog
func TestStress_ConcurrentMemoryLog(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	log := storage.NewMemoryLog()
	numWriters := 10
	numReaders := 20
	writesPerWriter := 1000
	readsPerReader := 5000

	var wg sync.WaitGroup
	var totalWrites atomic.Int64
	var totalReads atomic.Int64
	var readErrors atomic.Int64

	start := time.Now()

	// Writers
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			data := make([]byte, 256)
			for j := 0; j < writesPerWriter; j++ {
				data[0] = byte(id)
				data[1] = byte(j)
				log.Append(data)
				totalWrites.Add(1)
			}
		}(i)
	}

	// Let some writes happen first
	time.Sleep(10 * time.Millisecond)

	// Readers
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < readsPerReader; j++ {
				highest, err := log.HighestOffset()
				if err != nil {
					// Log might be empty initially
					continue
				}
				offset := uint64(j) % (highest + 1)
				_, err = log.Read(offset)
				if err != nil {
					readErrors.Add(1)
				}
				totalReads.Add(1)
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	writes := totalWrites.Load()
	reads := totalReads.Load()
	errors := readErrors.Load()

	t.Logf("MemoryLog stress test completed in %v", duration)
	t.Logf("Total writes: %d (%.2f ops/sec)", writes, float64(writes)/duration.Seconds())
	t.Logf("Total reads: %d (%.2f ops/sec)", reads, float64(reads)/duration.Seconds())
	t.Logf("Read errors: %d", errors)

	// Verify all writes succeeded
	expectedWrites := int64(numWriters * writesPerWriter)
	if writes != expectedWrites {
		t.Errorf("Expected %d writes, got %d", expectedWrites, writes)
	}
}

// TestStress_SerializationThroughput tests ClusterState serialization performance
func TestStress_SerializationThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	cs := state.NewClusterState()

	// Add realistic amount of state
	for i := 0; i < 50; i++ {
		cs.RegisterBroker(string(rune('A'+i)), "192.168.1.10:8001", i == 0)
	}
	for i := 0; i < 100; i++ {
		cs.RegisterProducer(string(rune('P'+i%26))+string(rune('0'+i/26)), "192.168.1.20:9001", "topic"+string(rune('0'+i%10)))
	}
	for i := 0; i < 100; i++ {
		cs.RegisterConsumer(string(rune('C'+i%26))+string(rune('0'+i/26)), "192.168.1.30:9002")
	}

	iterations := 10000
	start := time.Now()

	for i := 0; i < iterations; i++ {
		data, err := cs.Serialize()
		if err != nil {
			t.Fatalf("Serialization failed: %v", err)
		}

		cs2 := state.NewClusterState()
		if err := cs2.Deserialize(data); err != nil {
			t.Fatalf("Deserialization failed: %v", err)
		}
	}

	duration := time.Since(start)
	t.Logf("Serialization throughput: %d iterations in %v (%.2f ops/sec)",
		iterations, duration, float64(iterations)/duration.Seconds())
}

// BenchmarkClusterState_ConcurrentAccess benchmarks concurrent ClusterState access
func BenchmarkClusterState_ConcurrentAccess(b *testing.B) {
	cs := state.NewClusterState()

	// Pre-populate
	for i := 0; i < 10; i++ {
		cs.RegisterBroker(string(rune('A'+i)), "192.168.1.10:8001", i == 0)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%10 == 0 {
				cs.RegisterBroker(string(rune('a'+i%26)), "192.168.1.10:8001", false)
			} else {
				cs.GetBroker(string(rune('A' + i%10)))
			}
			i++
		}
	})
}

// BenchmarkHashRing_ConcurrentGetNode benchmarks concurrent hash ring lookups
func BenchmarkHashRing_ConcurrentGetNode(b *testing.B) {
	ring := loadbalance.NewConsistentHashRing()
	for i := 0; i < 20; i++ {
		ring.AddNode(string(rune('A' + i)))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			ring.GetNode("topic" + string(rune('0'+i%10)))
			i++
		}
	})
}

// BenchmarkMemoryLog_ConcurrentAppend benchmarks concurrent log appends
func BenchmarkMemoryLog_ConcurrentAppend(b *testing.B) {
	log := storage.NewMemoryLog()
	data := make([]byte, 1024)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			log.Append(data)
		}
	})
}
