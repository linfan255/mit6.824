package raft

import (
	"fmt"
	"log"
)

// Debugging
// -2 : close all debug log
// -1 : open PrintLog
// >= 0: open all log
const Debug = -1

// for debug
func (rf *Raft) PrintLogStatus() {
	if Debug < -1 {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentState == Leader {
		fmt.Printf(" [%d]l-ci:%d lenLog:%d ", rf.CurrentTerm, rf.commitIndex, len(rf.Log))
	} else if rf.currentState == Candidate {
		fmt.Printf(" [%d]c-ci:%d lenLog:%d ", rf.CurrentTerm, rf.commitIndex, len(rf.Log))
	} else if rf.currentState == Follower {
		fmt.Printf(" [%d]f-ci:%d lenLog:%d ", rf.CurrentTerm, rf.commitIndex, len(rf.Log))
	}
	fmt.Printf("| ")
	for _, v := range rf.Log {
		if v.Command == nil {
			fmt.Printf("%d:-- ", v.Term)
		} else {
			fmt.Printf("%d:%d ", v.Term, v.Command)
		}
	}
	fmt.Printf("| ")
	for i, v := range rf.nextIndex {
		fmt.Printf("%d:%d ", i, v)
	}
	fmt.Printf("\n")
}

/*
func (rf *Raft) PrintLogStatus() {
	if Debug < -1 {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentState == Leader {
		fmt.Printf(" [%d]l-ci:%d lenLog:%d ", rf.CurrentTerm, rf.commitIndex, len(rf.Log))
	} else if rf.currentState == Candidate {
		fmt.Printf(" [%d]c-ci:%d lenLog:%d ", rf.CurrentTerm, rf.commitIndex, len(rf.Log))
	} else if rf.currentState == Follower {
		fmt.Printf(" [%d]f-ci:%d lenLog:%d ", rf.CurrentTerm, rf.commitIndex, len(rf.Log))
	}

	term, cnt := -1, 0
	for _, v := range rf.Log {
		if v.Command != nil {
			if v.Term != term {
				if term != -1 {
					fmt.Printf("%d:%d ", term, cnt)
				}
				term = v.Term
				cnt = 1
			} else if v.Term == term {
				cnt++
			}
		}
	}
	fmt.Printf("%d:%d ", term, cnt)

	fmt.Printf("| ")
	for i, v := range rf.nextIndex {
		fmt.Printf("%d:%d ", i, v)
	}
	fmt.Printf("\n")
}
*/

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
