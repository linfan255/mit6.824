package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = 0

// for debug
/*
func (rf *Raft) PrintLogStatus() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentState == Leader {
		fmt.Printf(" *ci:%d* ", rf.commitIndex)
	} else {
		fmt.Printf(" -ci:%d- ", rf.commitIndex)
	}
	for _, v := range rf.Log {
		if v.Command == nil {
			fmt.Printf("%d:-- ", v.Term)
		} else {
			fmt.Printf("%d:%d ", v.Term, v.Command)
		}
	}
	fmt.Printf(" | ")
	for i, v := range rf.nextIndex {
		fmt.Printf("%d:%d ", i, v)
	}
	fmt.Println()
}
*/
func (rf *Raft) PrintLogStatus() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentState == Leader {
		fmt.Printf(" *ci:%d lenLog:%d ", rf.commitIndex, len(rf.Log))
	} else {
		fmt.Printf(" -ci:%d lenLog:%d ", rf.commitIndex, len(rf.Log))
	}
	fmt.Printf("| ")
	for i, v := range rf.nextIndex {
		fmt.Printf("%d:%d ", i, v)
	}
	fmt.Println()
}

func (rf *Raft) PrintLogStatusWithoutLock() {
	for _, v := range rf.Log {
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
