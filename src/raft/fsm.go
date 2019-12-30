package raft

import (
	"labrpc"
	"log"
	"sync"
	"time"
)

type RaftState int
type RaftEvent int
type RaftHandler func(*Raft, ...interface{}) bool

const (
	Leader    RaftState = 0
	Follower  RaftState = 1
	Candidate RaftState = 2

	TimeoutEvent       RaftEvent = 0
	GetVoteEvent       RaftEvent = 1
	GetHigherTermEvent RaftEvent = 2
	DetectHbEvent      RaftEvent = 3
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// persistent state on all servers
	currentTerm int
	votedFor    interface{}
	log         []Entry

	// volatile state on all servers
	commitIndex int
	lastApplied int
	applyCh     chan ApplyMsg

	// volatile state on leader
	// reinittialized after election
	nextIndex  []int
	matchIndex []int

	// FSM
	// start up as follower
	// =====================================================================================
	// event								 | state change
	// =====================================================================================
	// timeout or granting vote to others:   | follower -> candidate, candidate -> candidate
	// receives votes from majority servers: | candidate -> leader
	// discover server with higher term:     | leader -> follower
	// =====================================================================================
	currentState RaftState
	// used to signal when events occur
	getVoteCh chan int

	handlers map[RaftState]map[RaftEvent]RaftHandler
	voteNum  int // how many votes does this peer get

	electionTimer *time.Timer
}

func (rf *Raft) addHandler(state RaftState, event RaftEvent, handler RaftHandler) bool {
	if _, ok := rf.handlers[state]; !ok {
		rf.handlers[state] = make(map[RaftEvent]RaftHandler)
	}
	if _, ok := rf.handlers[state][event]; ok {
		log.Printf("state[%d] event[%d] handler have been set!\n", state, event)
		return false
	}
	rf.handlers[state][event] = handler
	return true
}

func (rf *Raft) callHandler(event RaftEvent, args ...interface{}) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if handler, ok := rf.handlers[rf.currentState][event]; ok {
		if !handler(rf, args...) {
			log.Printf("state(%d) handle event(%d) failed\n",
				rf.currentState, event)
			return false
		}
		return true
	}
	return false
}

///////////////////////////////////////////////////////////////////
// some rules for event handler:
// 1) return bool. true denotes success
// 2) first argument must be " rf *Raft "
// 3) do not lock!! or call a function with rf.mu.Lock inside
// 4) do not use "for {...}", use it in goroutine if it is needed
// 5) not get blocked inside handler
///////////////////////////////////////////////////////////////////

func startElection(rf *Raft, args ...interface{}) bool {
	DPrintf("[term:%d] server %d timeout, start election\n", rf.currentTerm, rf.me)
	rf.currentState = Candidate
	rf.votedFor = rf.me
	rf.voteNum = 1
	rf.currentTerm++

	rf.resetTimer()

	requestVoteArgs := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	for i, _ := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVote(i, requestVoteArgs)
		}
	}
	return true
}

func receiveVote(rf *Raft, args ...interface{}) bool {
	if rf.currentState != Candidate {
		return true
	}
	if len(args) != 1 {
		log.Fatalf("receive %d arguments, but expect one", len(args))
		return false
	}
	if sendTerm, ok := args[0].(int); ok {
		if sendTerm != rf.currentTerm {
			return true
		}
	} else {
		log.Fatalf("receive argument must be an Integar")
		return false
	}

	rf.voteNum++
	if rf.voteNum > len(rf.peers)/2 {
		DPrintf("[term:%d] %d become leader", rf.currentTerm, rf.me)
		rf.currentState = Leader
		rf.initLeader()
		go rf.sendHeartbeat(rf.currentTerm)
	}
	return true
}
