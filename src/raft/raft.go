package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//
// savepoint: why term changed even there is no failure, may be timeout for detectHeartBeat

import "sync"
import "labrpc"
import "math/rand"
import "time"

import "fmt"
import "log"

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Entry struct {
	Term    int
	Command interface{} // TODO type?
}

type RaftState int

const (
	Leader    RaftState = 0
	Follower  RaftState = 1
	Candidate RaftState = 2
)

const (
	RetryIntervalMs         int = 10
	ApplyLogIntervalMs      int = 300
	SendHeartBeatIntervalMs int = 100
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	currentTerm int
	votedFor    interface{} // TODO: type?
	log         []Entry

	// volatile state on all servers
	commitIndex int
	lastApplied int
	applyCh     chan ApplyMsg

	// volatile state on leader
	// TODO reinittialized after election
	nextIndex  []int
	matchIndex []int

	currentState RaftState
	lastRcvMs    int64 // in millionseconds
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.currentState == Leader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     interface{} // TODO type?
	LeaderCommit int

	PrevLogIndex int
	PrevLogTerm  int

	Entries []Entry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.Success = false
		return
	}
	if len(rf.log) <= args.PrevLogIndex ||
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	rf.lastRcvMs = time.Now().UnixNano() / 1e6

	if args.Term >= rf.currentTerm {
		rf.currentState = Follower
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = nil
		}
	}

	if args.Entries != nil {
		// check if there exists conflict entries
		splitIndex := 0
		findConflict := false
		for ; splitIndex < len(args.Entries); splitIndex++ {
			if args.PrevLogIndex+splitIndex+1 >= len(rf.log) {
				break
			}
			if rf.log[args.PrevLogIndex+splitIndex+1].Term != args.Entries[splitIndex].Term {
				findConflict = true
				break
			}
		}

		if findConflict {
			rf.log = rf.log[:args.PrevLogIndex+splitIndex+1]
		}

		for i := splitIndex; i < len(args.Entries); i++ {
			rf.log = append(rf.log, args.Entries[i])
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		oldCommitIndex := rf.commitIndex
		rf.commitIndex = args.LeaderCommit
		if rf.commitIndex > args.PrevLogIndex+len(args.Entries) &&
			args.PrevLogIndex+len(args.Entries) >= oldCommitIndex {
			rf.commitIndex = args.PrevLogIndex + len(args.Entries)
		}
		//fmt.Printf("update peer %d commitIndex from %d to %d, leaderCommit=%d, index of new entry=%d\n",
		//rf.me, oldCommitIndex, rf.commitIndex, args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	}
	reply.Success = true
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received
}

func isMoreUpToDate(lastLogTerm1 int, lastLogIndex1 int, lastLogTerm2 int, lastLogIndex2 int) bool {
	if lastLogTerm1 > lastLogTerm2 ||
		(lastLogTerm1 == lastLogTerm2 && lastLogIndex1 >= lastLogIndex2) {
		return true
	}
	return false
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = nil
		rf.currentState = Follower
	}

	if (rf.votedFor == nil || rf.votedFor == args.CandidateId) &&
		isMoreUpToDate(args.LastLogTerm, args.LastLogIndex, lastLogTerm, lastLogIndex) {
		reply.VoteGranted = true

		rf.votedFor = args.CandidateId
		rf.currentState = Candidate
		rf.lastRcvMs = time.Now().UnixNano() / 1e6
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int) {
	for {
		rf.mu.Lock()
		args := &AppendEntriesArgs{}
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.PrevLogIndex = rf.nextIndex[server] - 1
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		args.Entries = rf.log[rf.nextIndex[server]:]
		args.LeaderCommit = rf.commitIndex
		rf.mu.Unlock()

		reply := &AppendEntriesReply{}
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		if ok {
			isBreak := false

			rf.mu.Lock()
			if reply.Success {
				// if reply success, nextIndex and matchIndex should increase monotonically
				newNextIndex := args.PrevLogIndex + len(args.Entries) + 1
				newMatchIndex := args.PrevLogIndex + len(args.Entries)
				if newNextIndex > rf.nextIndex[server] && newNextIndex <= len(rf.log) {
					rf.nextIndex[server] = newNextIndex
				}
				if newMatchIndex > rf.matchIndex[server] && newMatchIndex <= len(rf.log) {
					rf.matchIndex[server] = newMatchIndex
				}
				isBreak = true
			} else {
				if reply.Term > rf.currentTerm {
					log.Printf("leader %d, term %d < follower %d, term %d\n",
						rf.me, rf.currentTerm, server, reply.Term)
					rf.currentState = Follower
					rf.currentTerm = reply.Term
					rf.votedFor = nil
					isBreak = true // TODO break after recieved a higher term ?
				} else {
					if rf.nextIndex[server] > 0 {
						rf.nextIndex[server]--
					}
				}
			}
			rf.mu.Unlock()

			if isBreak {
				break
			}
		}
		time.Sleep(time.Duration(RetryIntervalMs) * time.Millisecond)
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//fmt.Printf("leader %d start agreement on cmd[%d]\n", rf.me, command)
	isLeader = (rf.currentState == Leader)
	if !isLeader {
		return index, term, isLeader
	}

	if len(rf.peers) != len(rf.nextIndex) ||
		len(rf.peers) != len(rf.matchIndex) {
		log.Fatalf("len(peer)[%d] != len(nextIndex)[%d] != len(matchIndex)[%d]\n",
			len(rf.peers), len(rf.nextIndex), len(rf.matchIndex))
	}

	entry := Entry{rf.currentTerm, command}
	rf.log = append(rf.log, entry)

	index = len(rf.log) - 1
	term = rf.currentTerm

	for i, _ := range rf.peers {
		if i != rf.me && len(rf.log)-1 >= rf.nextIndex[i] {
			go rf.sendAppendEntries(i)
		} else if i == rf.me {
			if len(rf.log) > rf.nextIndex[i] {
				rf.nextIndex[i] = len(rf.log)
			}
			if len(rf.log)-1 > rf.matchIndex[i] {
				rf.matchIndex[i] = len(rf.log) - 1
			}
		}
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// log index start from 1, so add a useless element firstly
	rf.log = make([]Entry, 1)
	rf.log[0].Term = 0
	rf.log[0].Command = nil
	rf.currentTerm = 0
	rf.votedFor = nil
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.currentState = Follower
	rf.lastRcvMs = time.Now().UnixNano() / 1e6

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startRaft()

	return rf
}

func (rf *Raft) detectHeartBeat() {
	seedNum := time.Now().UnixNano()
	rand.Seed(seedNum)
	timeout := rand.Int()%150 + 500 // timeout is a random number bettween 500~650

	for {
		rf.mu.Lock()
		lastRcvMs := rf.lastRcvMs
		currentState := rf.currentState
		rf.mu.Unlock()

		if currentState != Follower {
			break
		}

		nowMs := time.Now().UnixNano() / 1e6
		if nowMs-lastRcvMs > int64(timeout) {
			rf.mu.Lock()
			rf.currentState = Candidate
			rf.votedFor = nil
			rf.mu.Unlock()
			break
		}
		time.Sleep(time.Duration(RetryIntervalMs) * time.Millisecond)
	}
}

func (rf *Raft) sendHeartBeat(peersId int) {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	commitIndex := rf.commitIndex
	prevLogIndex := rf.nextIndex[peersId] - 1
	prevLogTerm := rf.log[prevLogIndex].Term
	rf.mu.Unlock()

	args := &AppendEntriesArgs{}
	args.Term = currentTerm
	args.LeaderId = rf.me

	args.PrevLogIndex = prevLogIndex
	args.PrevLogTerm = prevLogTerm
	args.Entries = nil // nil denote to be heartbeat
	args.LeaderCommit = commitIndex

	reply := &AppendEntriesReply{}

	//fmt.Printf("leader %d send heartbeat to %d, PrevLogIndex=%d, PrevLogTerm=%d LeaderCommit=%d\n",
	//	rf.me, peersId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
	ok := rf.peers[peersId].Call("Raft.AppendEntries", args, reply)
	if ok && reply.Term > currentTerm {
		rf.mu.Lock()
		rf.currentState = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = nil
		rf.mu.Unlock()
	}
}

// notice!! not thread safe
func (rf *Raft) initLeader() {
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
	}

	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) onCandidate() {
	rf.mu.Lock()
	if rf.votedFor == nil {
		rf.currentTerm++
		rf.votedFor = rf.me // vote for self
	}
	rf.lastRcvMs = time.Now().UnixNano() / 1e6

	votedFor := rf.votedFor
	currentTerm := rf.currentTerm
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	rf.mu.Unlock()

	var votedNumMu sync.Mutex
	votedNum := 0
	if votedFor == rf.me {
		votedNum++
	}

	// send request vote to other peers
	args := &RequestVoteArgs{}
	args.Term = currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = lastLogIndex
	args.LastLogTerm = lastLogTerm
	for i, peer := range rf.peers {
		if i != rf.me && peer != nil {
			peerId := i
			go func() {
				reply := &RequestVoteReply{}
				ok := rf.peers[peerId].Call("Raft.RequestVote", args, reply)
				if ok {
					if reply.VoteGranted {
						votedNumMu.Lock()
						votedNum++
						votedNumMu.Unlock()
					}
					if reply.Term > currentTerm {
						rf.mu.Lock()
						rf.currentState = Follower
						rf.currentTerm = reply.Term
						rf.votedFor = nil
						rf.mu.Unlock()
					}
				}
			}()
		}
	}

	// waited(500~650ms) to gain majority peers' vote
	rand.Seed(time.Now().UnixNano())
	timeout := rand.Int()%150 + 500
	for {
		rf.mu.Lock()
		currentState := rf.currentState
		lastRcvMs := rf.lastRcvMs
		rf.mu.Unlock()

		// if received RPC from other peers, currentState may change to follower
		if currentState != Candidate {
			break
		}

		votedNumMu.Lock()
		mVotedNum := votedNum
		votedNumMu.Unlock()

		// check timeout
		nowMs := time.Now().UnixNano() / 1e6
		if nowMs-lastRcvMs > int64(timeout) {
			// timeout!! need to start new election
			rf.mu.Lock()
			rf.votedFor = nil
			rf.mu.Unlock()
			break
		}

		// check if gain majority peers' vote
		if mVotedNum > len(rf.peers)/2 {
			rf.mu.Lock()
			rf.currentState = Leader
			fmt.Printf("[term %d]:peer %d become leader\n", rf.currentTerm, rf.me)
			rf.initLeader() // reinitialize after election
			rf.mu.Unlock()
			break
		}
		time.Sleep(time.Duration(RetryIntervalMs) * time.Millisecond)
	}
}

func (rf *Raft) onLeader() {
	for i, _ := range rf.peers {
		if i != rf.me {
			go rf.sendHeartBeat(i)
		}
	}
	// send heart beat request no more than 10 times in 1s
	time.Sleep(time.Duration(SendHeartBeatIntervalMs) * time.Millisecond)
}

func (rf *Raft) onFollower() {
	rf.detectHeartBeat()
}

func (rf *Raft) startRaft() {
	go rf.startApplier()
	for {
		rf.mu.Lock()
		state := rf.currentState
		rf.mu.Unlock()

		switch state {
		case Leader:
			rf.onLeader()

		case Follower:
			rf.onFollower()

		case Candidate:
			rf.onCandidate()

		default:
			return
		}
	}
}

func (rf *Raft) startApplier() {
	for {
		rf.mu.Lock()

		// update commitIndex for leader
		if rf.currentState == Leader {
			for i := len(rf.log) - 1; i > rf.commitIndex; i-- {
				count := 0
				for j := 0; j < len(rf.matchIndex); j++ {
					if rf.matchIndex[j] >= i {
						count++
					}
				}
				if count > len(rf.peers)/2 &&
					rf.log[i].Term == rf.currentTerm {
					rf.commitIndex = i
					break
				}
			}
		}

		for rf.commitIndex > rf.lastApplied &&
			rf.lastApplied < len(rf.log)-1 {
			var applyMsg ApplyMsg
			applyMsg.CommandValid = true
			applyMsg.Command = rf.log[rf.lastApplied+1].Command
			applyMsg.CommandIndex = rf.lastApplied + 1
			fmt.Printf("[term %d]:peer %d apply cmd %d index %d\n",
				rf.currentTerm, rf.me, applyMsg.Command, rf.lastApplied+1)
			rf.applyCh <- applyMsg
			rf.lastApplied++
		}

		rf.mu.Unlock()
		time.Sleep(time.Duration(ApplyLogIntervalMs) * time.Millisecond)
	}
}
