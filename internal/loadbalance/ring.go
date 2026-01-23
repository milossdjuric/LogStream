package loadbalance

import (
	"hash/crc32"
	"sort"
	"sync"
)

type ConsistentHashRing struct {
	nodes       map[uint32]string // Hash -> NodeID
	sortedNodes []uint32          // Sorted Hashes
	mu          sync.RWMutex
}

func NewConsistentHashRing() *ConsistentHashRing {
	return &ConsistentHashRing{
		nodes:       make(map[uint32]string),
		sortedNodes: []uint32{},
	}
}

func (r *ConsistentHashRing) AddNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	hash := crc32.ChecksumIEEE([]byte(nodeID))
	r.nodes[hash] = nodeID
	r.sortedNodes = append(r.sortedNodes, hash)
	sort.Slice(r.sortedNodes, func(i, j int) bool {
		return r.sortedNodes[i] < r.sortedNodes[j]
	})
}

func (r *ConsistentHashRing) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	hash := crc32.ChecksumIEEE([]byte(nodeID))
	delete(r.nodes, hash)

	// Rebuild sorted slice
	newSorted := make([]uint32, 0, len(r.nodes))
	for h := range r.nodes {
		newSorted = append(newSorted, h)
	}
	sort.Slice(newSorted, func(i, j int) bool {
		return newSorted[i] < newSorted[j]
	})
	r.sortedNodes = newSorted
}

func (r *ConsistentHashRing) GetNode(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.sortedNodes) == 0 {
		return ""
	}

	hash := crc32.ChecksumIEEE([]byte(key))

	// Find the first node with hash >= key hash
	idx := sort.Search(len(r.sortedNodes), func(i int) bool {
		return r.sortedNodes[i] >= hash
	})

	// Wrap around
	if idx == len(r.sortedNodes) {
		idx = 0
	}

	return r.nodes[r.sortedNodes[idx]]
}
