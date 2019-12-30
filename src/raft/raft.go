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

// import "sync"
import "labrpc"
import "math/rand"
import "time"

// import "fmt"
// import "log"

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

const (
	MinTimeout int = 400
	MaxTimeout int = 650

	DetectTimeoutIntervalMs int = 5
	SendHbIntervalMs        int = 100
)

func (rf *Raft) resetTimer() {
	rand.Seed(time.Now().UnixNano())
	timeout := rand.Int()%(MaxTimeout-MinTimeout) + MinTimeout
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(time.Duration(timeout) * time.Millisecond)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := rf.currentState == Leader
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

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs) {
	reply := &RequestVoteReply{}
	DPrintf("[term:%d] %d send RequestVote to %d\n",
		args.Term, args.CandidateId, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return
	}

	if reply.VoteGranted {
		rf.getVoteCh <- args.Term
	} else {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.currentState = Follower
			rf.votedFor = nil
			rf.voteNum = 0
		}
		rf.mu.Unlock()
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

func (rf *Raft) sendHeartbeat(sendHbTerm int) {
	for {
		rf.mu.Lock()
		currentTerm := rf.currentTerm
		currentState := rf.currentState
		commitIndex := rf.commitIndex
		rf.mu.Unlock()

		if currentState != Leader || currentTerm != sendHbTerm {
			return
		}

		args := &AppendEntriesArgs{
			Term:         currentTerm,
			LeaderId:     rf.me,
			Entries:      nil,
			LeaderCommit: commitIndex,
		}

		for i, _ := range rf.peers {
			if i != rf.me {
				go func(follower int, args *AppendEntriesArgs) {
					rf.mu.Lock()
					args.PrevLogIndex = Min(rf.nextIndex[follower]-1, len(rf.log)-1)
					args.PrevLogIndex = Max(args.PrevLogIndex, 0)
					args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
					rf.mu.Unlock()

					reply := &AppendEntriesReply{}
					ok := rf.peers[follower].Call("Raft.AppendEntries", args, reply)

					rf.mu.Lock()
					if ok && reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.currentState = Follower
						rf.votedFor = nil
						rf.voteNum = 0
					}
					rf.mu.Unlock()
				}(i, args)
			}
		}

		duration := time.Duration(SendHbIntervalMs)
		time.Sleep(duration * time.Millisecond)
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

func (rf *Raft) registerHandler() {
	rf.addHandler(Follower, TimeoutEvent, startElection)
	rf.addHandler(Candidate, TimeoutEvent, startElection)
	rf.addHandler(Candidate, GetVoteEvent, receiveVote)
}

func (rf *Raft) startRaft() {
	rf.registerHandler()
	for {
		select {
		case <-rf.electionTimer.C:
			rf.callHandler(TimeoutEvent)

		case sendTerm := <-rf.getVoteCh:
			rf.callHandler(GetVoteEvent, sendTerm)
		}
	}
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
	rf.getVoteCh = make(chan int, 10)
	rf.handlers = make(map[RaftState]map[RaftEvent]RaftHandler)
	rf.voteNum = 0

	rand.Seed(time.Now().UnixNano())
	timeout := rand.Int()%(MaxTimeout-MinTimeout) + MinTimeout
	rf.electionTimer = time.NewTimer(time.Duration(timeout) * time.Millisecond)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.startRaft()
	return rf
}
