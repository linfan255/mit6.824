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

import "labrpc"
import "log"
import "math/rand"
import "time"
import "bytes"
import "labgob"

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
	MinTimeout int = 800
	MaxTimeout int = 1000

	RetryIntervalMs  int = 10
	SendHbIntervalMs int = 100
	ApplyIntervalMs  int = 100
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

	term := rf.CurrentTerm
	isleader := rf.currentState == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&rf.Log) != nil {
		log.Fatalln("readPersist failed")
	}
	rf.CurrentTerm = currentTerm
	rf.VotedFor = votedFor
}

func (rf *Raft) trySendAppendEntries(server int) {
	for !rf.sendAppendEntries(server, false) {
		duration := time.Duration(RetryIntervalMs)
		time.Sleep(duration * time.Millisecond)
	}
}

func (rf *Raft) sendHeartbeat(sendTerm int) {
	duration := time.Duration(SendHbIntervalMs)
	for {
		rf.mu.Lock()
		term := rf.CurrentTerm
		isleader := rf.currentState == Leader
		isRunning := rf.isRunning
		rf.mu.Unlock()

		if !isRunning || !isleader || sendTerm != term {
			return
		}
		for i, _ := range rf.peers {
			go rf.sendAppendEntries(i, true)
		}
		time.Sleep(duration * time.Millisecond)
	}
}

func (rf *Raft) sendAppendEntries(server int, isHeartbeat bool) bool {
	rf.mu.Lock()
	if !rf.isRunning || rf.me == server || rf.currentState != Leader {
		rf.mu.Unlock()
		return true
	}
	if !isHeartbeat && (len(rf.Log) <= rf.nextIndex[server] ||
		rf.nextIndex[server] <= 0) {
		rf.mu.Unlock()
		return true
	}

	args := AppendEntriesArgs{}
	args.Term = rf.CurrentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex
	args.PrevLogIndex = Min(rf.nextIndex[server]-1, len(rf.Log)-1)
	args.PrevLogIndex = Max(args.PrevLogIndex, 0)
	args.PrevLogTerm = rf.Log[args.PrevLogIndex].Term

	if isHeartbeat {
		args.Entries = nil
	} else {
		args.Entries = rf.Log[rf.nextIndex[server]:]
	}
	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.CurrentTerm != args.Term || !rf.isRunning {
		return true
	}

	if ok {
		if reply.Success {
			newNextIndex := args.PrevLogIndex + 1 + len(args.Entries)
			oldMatchIndex := rf.matchIndex[server]
			rf.nextIndex[server] = Max(rf.nextIndex[server], newNextIndex)
			rf.matchIndex[server] = Max(rf.matchIndex[server], newNextIndex-1)
			if oldMatchIndex < rf.matchIndex[server] {
				rf.updateCommitIndex()
			}
			return true
		} else {
			if reply.Term > rf.CurrentTerm {
				rf.currentState = Follower
				rf.CurrentTerm = reply.Term
				rf.VotedFor = -1
				rf.voteNum = 0
				rf.resetTimer()
				rf.persist()
			} else {
				// meet conflict
				findConflictTerm := false
				for i := len(rf.Log) - 1; i > 0; i-- {
					if rf.Log[i].Term == reply.ConflictTerm &&
						(i == len(rf.Log)-1 || rf.Log[i+1].Term != reply.ConflictTerm) {
						rf.nextIndex[server] = i + 1
						findConflictTerm = true
						break
					}
				}
				if !findConflictTerm {
					rf.nextIndex[server] = reply.ConflictIndex
				}
			}
		}
	}
	return false
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs) {
	if server == rf.me {
		return
	}

	DPrintf("[term %d] peer(%d) sendRequestVote to %d LastLogTerm=%d LastLogIndex=%d\n",
		args.Term, rf.me, server, args.LastLogTerm, args.LastLogIndex)
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.CurrentTerm != args.Term ||
		rf.currentState != Candidate || !rf.isRunning {
		return
	}

	if reply.VoteGranted {
		rf.getVoteCh <- args.Term
	} else {
		if reply.Term > rf.CurrentTerm {
			rf.currentState = Follower
			rf.CurrentTerm = reply.Term
			rf.VotedFor = -1
			rf.voteNum = 0
			rf.resetTimer()
			rf.persist()
		}
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.endCh <- struct{}{}
}

// notice!! not thread safe
func (rf *Raft) initLeader() {
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.Log)
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

	rf.addHandler(Leader, RaftEndEvent, endRaft)
	rf.addHandler(Follower, RaftEndEvent, endRaft)
	rf.addHandler(Candidate, RaftEndEvent, endRaft)
}

func (rf *Raft) startRaft() {
	rf.registerHandler()
	for {
		select {
		case <-rf.electionTimer.C:
			rf.callHandler(TimeoutEvent)

		case sendTerm := <-rf.getVoteCh:
			rf.callHandler(GetVoteEvent, sendTerm)

		case <-rf.endCh:
			rf.callHandler(RaftEndEvent)
			return
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
	rf.Log = make([]Entry, 1)
	rf.Log[0].Term = 0
	rf.Log[0].Command = nil
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh

	rf.currentState = Follower
	rf.getVoteCh = make(chan int, 3000)
	rf.handlers = make(map[RaftState]map[RaftEvent]RaftHandler)
	rf.voteNum = 0

	rand.Seed(time.Now().UnixNano())
	timeout := rand.Int()%(MaxTimeout-MinTimeout) + MinTimeout
	rf.electionTimer = time.NewTimer(time.Duration(timeout) * time.Millisecond)
	rf.isRunning = true
	rf.endCh = make(chan interface{}, 10)
	DPrintf("peer(%d) startup at timeMillisecond %d\n",
		rf.me, time.Now().UnixNano()/1e6)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.startRaft()
	return rf
}
