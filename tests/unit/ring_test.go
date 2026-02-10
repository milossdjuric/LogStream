package unit

import (
	"testing"

	"github.com/milossdjuric/logstream/internal/loadbalance"
)

func TestConsistentHashRing_AddNode(t *testing.T) {
	ring := loadbalance.NewConsistentHashRing()

	ring.AddNode("node1")
	ring.AddNode("node2")
	ring.AddNode("node3")

	node := ring.GetNode("topic1")
	if node == "" {
		t.Fatal("Expected non-empty node assignment")
	}
}

func TestConsistentHashRing_Consistency(t *testing.T) {
	ring := loadbalance.NewConsistentHashRing()
	ring.AddNode("node1")
	ring.AddNode("node2")
	ring.AddNode("node3")

	topic := "test-topic"
	node1 := ring.GetNode(topic)
	node2 := ring.GetNode(topic)

	if node1 != node2 {
		t.Errorf("Same topic should map to same node: got %s and %s", node1, node2)
	}
}

func TestConsistentHashRing_Distribution(t *testing.T) {
	ring := loadbalance.NewConsistentHashRing()
	nodes := []string{"node1", "node2", "node3", "node4"}
	for _, node := range nodes {
		ring.AddNode(node)
	}

	assignments := make(map[string]int)
	for i := 0; i < 1000; i++ {
		topic := string(rune('a' + i%26)) + string(rune('0' + i%10))
		node := ring.GetNode(topic)
		assignments[node]++
	}

	for _, node := range nodes {
		count := assignments[node]
		if count == 0 {
			t.Errorf("Node %s received no assignments", node)
		}
	}

	t.Logf("Distribution: %v", assignments)
}

func TestConsistentHashRing_RemoveNode(t *testing.T) {
	ring := loadbalance.NewConsistentHashRing()
	ring.AddNode("node1")
	ring.AddNode("node2")
	ring.AddNode("node3")

	before := ring.GetNode("topic1")
	ring.RemoveNode("node2")
	after := ring.GetNode("topic1")

	if before == "node2" && after == "node2" {
		t.Error("Removed node should not be assigned")
	}
}

func TestConsistentHashRing_GetNodeExcluding(t *testing.T) {
	ring := loadbalance.NewConsistentHashRing()
	ring.AddNode("leader")
	ring.AddNode("follower1")
	ring.AddNode("follower2")

	// With 3 nodes, GetNodeExcluding should never return the excluded node
	for i := 0; i < 100; i++ {
		topic := string(rune('a'+i%26)) + string(rune('0'+i%10))
		node := ring.GetNodeExcluding(topic, "leader")
		if node == "leader" {
			t.Errorf("GetNodeExcluding should not return excluded node for topic %s", topic)
		}
		if node == "" {
			t.Errorf("GetNodeExcluding should return a non-empty node for topic %s", topic)
		}
	}
}

func TestConsistentHashRing_GetNodeExcluding_SingleNode(t *testing.T) {
	ring := loadbalance.NewConsistentHashRing()
	ring.AddNode("leader")

	// With only 1 node, GetNodeExcluding must return it even if excluded
	node := ring.GetNodeExcluding("topic1", "leader")
	if node != "leader" {
		t.Errorf("Expected 'leader' when it's the only node, got %s", node)
	}
}

func TestConsistentHashRing_NodeCount(t *testing.T) {
	ring := loadbalance.NewConsistentHashRing()
	if ring.NodeCount() != 0 {
		t.Errorf("Expected 0 nodes, got %d", ring.NodeCount())
	}

	ring.AddNode("node1")
	ring.AddNode("node2")
	if ring.NodeCount() != 2 {
		t.Errorf("Expected 2 nodes, got %d", ring.NodeCount())
	}

	ring.RemoveNode("node1")
	if ring.NodeCount() != 1 {
		t.Errorf("Expected 1 node, got %d", ring.NodeCount())
	}
}

func TestConsistentHashRing_GetNodeExcluding_Distribution(t *testing.T) {
	ring := loadbalance.NewConsistentHashRing()
	ring.AddNode("leader")
	ring.AddNode("follower1")
	ring.AddNode("follower2")
	ring.AddNode("follower3")

	assignments := make(map[string]int)
	for i := 0; i < 1000; i++ {
		topic := string(rune('a'+i%26)) + string(rune('0'+i%10))
		node := ring.GetNodeExcluding(topic, "leader")
		assignments[node]++
	}

	if assignments["leader"] > 0 {
		t.Errorf("Leader should never be assigned, got %d assignments", assignments["leader"])
	}

	for _, name := range []string{"follower1", "follower2", "follower3"} {
		if assignments[name] == 0 {
			t.Errorf("Follower %s received no assignments", name)
		}
	}

	t.Logf("Distribution (excluding leader): %v", assignments)
}

func BenchmarkConsistentHashRing_GetNode(b *testing.B) {
	ring := loadbalance.NewConsistentHashRing()
	for i := 0; i < 100; i++ {
		ring.AddNode(string(rune('a' + i%26)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring.GetNode("test-topic")
	}
}
