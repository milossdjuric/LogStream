package unit

import (
	"testing"
	"time"

	"github.com/milossdjuric/logstream/internal/failure"
	"github.com/milossdjuric/logstream/internal/loadbalance"
	"github.com/milossdjuric/logstream/internal/state"
	"github.com/milossdjuric/logstream/internal/storage"
)

func BenchmarkHashRing_AddNode_10(b *testing.B) {
	ring := loadbalance.NewConsistentHashRing()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 10; j++ {
			ring.AddNode(string(rune('a' + j)))
		}
	}
}

func BenchmarkHashRing_AddNode_100(b *testing.B) {
	ring := loadbalance.NewConsistentHashRing()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 100; j++ {
			ring.AddNode(string(rune('a' + j%26)))
		}
	}
}

func BenchmarkHashRing_GetNode_10Nodes(b *testing.B) {
	ring := loadbalance.NewConsistentHashRing()
	for i := 0; i < 10; i++ {
		ring.AddNode(string(rune('a' + i)))
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring.GetNode("test-topic")
	}
}

func BenchmarkHashRing_GetNode_100Nodes(b *testing.B) {
	ring := loadbalance.NewConsistentHashRing()
	for i := 0; i < 100; i++ {
		ring.AddNode(string(rune('a' + i%26)))
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring.GetNode("test-topic")
	}
}

func BenchmarkPhiDetector_Update_SingleNode(b *testing.B) {
	detector := failure.NewAccrualFailureDetector(1000)
	nodeID := "test-node"
	now := time.Now()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		detector.Update(nodeID, now.Add(time.Duration(i)*time.Millisecond))
	}
}

func BenchmarkPhiDetector_Update_10Nodes(b *testing.B) {
	detector := failure.NewAccrualFailureDetector(1000)
	nodes := make([]string, 10)
	for i := 0; i < 10; i++ {
		nodes[i] = string(rune('a' + i))
	}
	now := time.Now()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nodeID := nodes[i%10]
		detector.Update(nodeID, now.Add(time.Duration(i)*time.Millisecond))
	}
}

func BenchmarkPhiDetector_Status_SingleNode(b *testing.B) {
	detector := failure.NewAccrualFailureDetector(1000)
	nodeID := "test-node"
	now := time.Now()
	
	for i := 0; i < 100; i++ {
		detector.Update(nodeID, now.Add(time.Duration(i)*time.Millisecond))
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		detector.Status(nodeID)
	}
}

func BenchmarkPhiDetector_Status_10Nodes(b *testing.B) {
	detector := failure.NewAccrualFailureDetector(1000)
	nodes := make([]string, 10)
	for i := 0; i < 10; i++ {
		nodes[i] = string(rune('a' + i))
		now := time.Now()
		for j := 0; j < 10; j++ {
			detector.Update(nodes[i], now.Add(time.Duration(j)*time.Millisecond))
		}
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		detector.Status(nodes[i%10])
	}
}

func BenchmarkClusterState_RegisterBroker_10(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cs := state.NewClusterState()
		for j := 0; j < 10; j++ {
			cs.RegisterBroker(string(rune('a'+j)), "192.168.1.10:8001", false)
		}
	}
}

func BenchmarkClusterState_RegisterBroker_100(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cs := state.NewClusterState()
		for j := 0; j < 100; j++ {
			cs.RegisterBroker(string(rune('a'+j%26)), "192.168.1.10:8001", false)
		}
	}
}

func BenchmarkClusterState_Serialize_10Brokers(b *testing.B) {
	cs := state.NewClusterState()
	for i := 0; i < 10; i++ {
		cs.RegisterBroker(string(rune('a'+i)), "192.168.1.10:8001", false)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cs.Serialize()
	}
}

func BenchmarkClusterState_Serialize_100Brokers(b *testing.B) {
	cs := state.NewClusterState()
	for i := 0; i < 100; i++ {
		cs.RegisterBroker(string(rune('a'+i%26)), "192.168.1.10:8001", false)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cs.Serialize()
	}
}

func BenchmarkClusterState_Deserialize(b *testing.B) {
	cs := state.NewClusterState()
	for i := 0; i < 50; i++ {
		cs.RegisterBroker(string(rune('a'+i%26)), "192.168.1.10:8001", false)
	}
	data, _ := cs.Serialize()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cs2 := state.NewClusterState()
		cs2.Deserialize(data)
	}
}

func BenchmarkMemoryLog_Append_1KB(b *testing.B) {
	log := storage.NewMemoryLog()
	data := make([]byte, 1024)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		log.Append(data)
	}
	
	b.ReportMetric(float64(b.N*1024)/1024/1024, "MB")
}

func BenchmarkMemoryLog_Append_10KB(b *testing.B) {
	log := storage.NewMemoryLog()
	data := make([]byte, 10*1024)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		log.Append(data)
	}
	
	b.ReportMetric(float64(b.N*10*1024)/1024/1024, "MB")
}

func BenchmarkMemoryLog_Read_Sequential(b *testing.B) {
	log := storage.NewMemoryLog()
	data := make([]byte, 1024)
	
	for i := 0; i < 1000; i++ {
		log.Append(data)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		log.Read(uint64(i % 1000))
	}
}

func BenchmarkMemoryLog_Read_Random(b *testing.B) {
	log := storage.NewMemoryLog()
	data := make([]byte, 1024)
	
	for i := 0; i < 1000; i++ {
		log.Append(data)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := uint64((i * 997) % 1000)
		log.Read(offset)
	}
}

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

func BenchmarkMemoryLog_ConcurrentRead(b *testing.B) {
	log := storage.NewMemoryLog()
	data := make([]byte, 1024)
	
	for i := 0; i < 10000; i++ {
		log.Append(data)
	}
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			log.Read(uint64(i % 10000))
			i++
		}
	})
}

func BenchmarkMemoryLog_MixedReadWrite(b *testing.B) {
	log := storage.NewMemoryLog()
	data := make([]byte, 1024)
	
	for i := 0; i < 1000; i++ {
		log.Append(data)
	}
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%10 == 0 {
				log.Append(data)
			} else {
				log.Read(uint64(i % 1000))
			}
			i++
		}
	})
}
