package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = 0

// for debug
func (rf *Raft) PrintLogStatus() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for _, v := range rf.log {
		if v.Command == nil {
			fmt.Printf("%d:-- ", v.Term)
		} else {
			fmt.Printf("%d:%d ", v.Term, v.Command)
		}
	}
	fmt.Println()
}

func (rf *Raft) PrintLogStatusWithoutLock() {
	for _, v := range rf.log {
		if v.Command == nil {
			fmt.Printf("%d:-- ", v.Term)
		} else {
			fmt.Printf("%d:%d ", v.Term, v.Command)
		}
	}
	fmt.Println()
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func Max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func Min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}
