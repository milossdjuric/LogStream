package failure

import (
	"math"
	"sync"
	"time"
)

const (
	// DefaultWindowSize is the size of the sliding window for interval history
	DefaultWindowSize = 1000
	// MinStdDev prevents division by zero and extreme sensitivity
	MinStdDev = 100 * time.Millisecond
)

type NodeState struct {
	lastHeartbeat time.Time
	intervals     []time.Duration
	windowSize    int
}

type AccrualFailureDetector struct {
	mu     sync.RWMutex
	nodes  map[string]*NodeState
	window int
}

func NewAccrualFailureDetector(windowSize int) *AccrualFailureDetector {
	if windowSize <= 0 {
		windowSize = DefaultWindowSize
	}
	return &AccrualFailureDetector{
		nodes:  make(map[string]*NodeState),
		window: windowSize,
	}
}

// Update records a new heartbeat from a node
func (d *AccrualFailureDetector) Update(nodeID string, arrivalTime time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()

	state, exists := d.nodes[nodeID]
	if !exists {
		d.nodes[nodeID] = &NodeState{
			lastHeartbeat: arrivalTime,
			windowSize:    d.window,
			intervals:     make([]time.Duration, 0, d.window),
		}
		return
	}

	interval := arrivalTime.Sub(state.lastHeartbeat)
	state.lastHeartbeat = arrivalTime

	// Add to sliding window
	if len(state.intervals) >= state.windowSize {
		state.intervals = state.intervals[1:]
	}
	state.intervals = append(state.intervals, interval)
}

// Status methodology:
// 1. Calculate mean and stddev of historical intervals.
// 2. Probability P_later(t) = probability that interval > t ... using Normal Distribution CDF.
// 3. Phi = -log10(P_later(t))
func (d *AccrualFailureDetector) Status(nodeID string) float64 {
	d.mu.RLock()
	state, exists := d.nodes[nodeID]
	d.mu.RUnlock()

	if !exists {
		return 0.0 // Unknown nodes are not suspect, or could be infinite? Let's say 0.
	}

	// If we don't have enough history, return 0 (assume alive)
	if len(state.intervals) < 2 {
		return 0.0
	}

	timeSinceLast := time.Since(state.lastHeartbeat)

	mean, stdDev := d.calculateStats(state.intervals)

	// Ensure min stdDev to avoid division by zero or extreme sensitivity
	if stdDev < float64(MinStdDev) {
		stdDev = float64(MinStdDev)
	}

	return d.calculatePhi(float64(timeSinceLast), mean, stdDev)
}

func (d *AccrualFailureDetector) calculateStats(intervals []time.Duration) (mean float64, stdDev float64) {
	var sum float64
	for _, i := range intervals {
		sum += float64(i)
	}
	mean = sum / float64(len(intervals))

	var varianceSum float64
	for _, i := range intervals {
		diff := float64(i) - mean
		varianceSum += diff * diff
	}
	variance := varianceSum / float64(len(intervals))
	stdDev = math.Sqrt(variance)
	return
}

func (d *AccrualFailureDetector) calculatePhi(timeSinceLast, mean, stdDev float64) float64 {
	y := (timeSinceLast - mean) / stdDev

	pLater := 0.5 * math.Erfc(y/math.Sqrt2)

	if pLater == 0 {
		return 100.0 // Extremely high suspicion
	}

	return -math.Log10(pLater)
}

func (d *AccrualFailureDetector) IsAvailable(nodeID string, threshold float64) bool {
	return d.Status(nodeID) < threshold
}

func (d *AccrualFailureDetector) GetSuspects(threshold float64) []string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var suspects []string
	for nodeID := range d.nodes {
		phi := d.Status(nodeID)
		if phi >= threshold {
			suspects = append(suspects, nodeID)
		}
	}
	return suspects
}
