package state

import (
	"fmt"
	"sync"
)

// ViewState manages view-synchronous group membership
type ViewState struct {
	mu              sync.RWMutex
	viewNumber      int64
	leaderID        string
	memberIDs       []string
	memberAddresses map[string]string
	agreedSeqNum    int64
	frozen          bool
}

func NewViewState() *ViewState {
	return &ViewState{
		viewNumber:      0,
		memberAddresses: make(map[string]string),
		frozen:          false,
	}
}

func (v *ViewState) GetViewNumber() int64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.viewNumber
}

func (v *ViewState) GetLeaderID() string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.leaderID
}

func (v *ViewState) IsFrozen() bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.frozen
}

func (v *ViewState) Freeze() {
	v.mu.Lock()
	defer v.mu.Unlock()
	
	if !v.frozen {
		v.frozen = true
		fmt.Printf("[ViewState] Operations FROZEN for view change\n")
	}
}

func (v *ViewState) Unfreeze() {
	v.mu.Lock()
	defer v.mu.Unlock()
	
	if v.frozen {
		v.frozen = false
		fmt.Printf("[ViewState] Operations UNFROZEN - view %d active\n", v.viewNumber)
	}
}

// InstallView installs a new view
func (v *ViewState) InstallView(viewNumber int64, leaderID string, memberIDs []string, memberAddresses []string, agreedSeqNum int64) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if viewNumber <= v.viewNumber {
		return fmt.Errorf("cannot install older view %d (current: %d)", viewNumber, v.viewNumber)
	}

	if len(memberIDs) != len(memberAddresses) {
		return fmt.Errorf("memberIDs and memberAddresses length mismatch: %d vs %d", 
			len(memberIDs), len(memberAddresses))
	}

	v.viewNumber = viewNumber
	v.leaderID = leaderID
	v.memberIDs = make([]string, len(memberIDs))
	copy(v.memberIDs, memberIDs)
	
	v.memberAddresses = make(map[string]string)
	for i, id := range memberIDs {
		v.memberAddresses[id] = memberAddresses[i]
	}
	
	v.agreedSeqNum = agreedSeqNum
	v.frozen = false

	var leaderStr string
	if len(leaderID) > 8 {
		leaderStr = leaderID[:8]
	} else {
		leaderStr = leaderID
	}

	fmt.Printf("[ViewState] Installed view %d: leader=%s members=%d agreedSeq=%d\n",
		viewNumber, leaderStr, len(memberIDs), agreedSeqNum)

	return nil
}

// PrintStatus prints current view state
func (v *ViewState) PrintStatus() {
	v.mu.RLock()
	defer v.mu.RUnlock()

	fmt.Println("\n=== View State ===")
	fmt.Printf("View Number:  %d\n", v.viewNumber)
	
	var leaderStr string
	if len(v.leaderID) > 8 {
		leaderStr = v.leaderID[:8]
	} else if v.leaderID == "" {
		leaderStr = "(none)"
	} else {
		leaderStr = v.leaderID
	}
	fmt.Printf("Leader:       %s\n", leaderStr)
	
	fmt.Printf("Members:      %d\n", len(v.memberIDs))
	for _, id := range v.memberIDs {
		var shortID string
		if len(id) > 8 {
			shortID = id[:8]
		} else {
			shortID = id
		}
		fmt.Printf("  - %s (%s)\n", shortID, v.memberAddresses[id])
	}
	
	fmt.Printf("Agreed Seq:   %d\n", v.agreedSeqNum)
	fmt.Printf("Frozen:       %v\n", v.frozen)
	fmt.Println("==================")
}
