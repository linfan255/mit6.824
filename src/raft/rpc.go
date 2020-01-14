package raft

// import "fmt"

type Entry struct {
	Term    int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	LeaderCommit int

	PrevLogIndex int
	PrevLogTerm  int

	Entries []Entry
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isRunning {
		return
	}

	reply.Term = rf.CurrentTerm
	if rf.CurrentTerm > args.Term {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = -1
		return
	}
	rf.resetTimer()

	if len(rf.Log) <= args.PrevLogIndex {
		reply.ConflictIndex = len(rf.Log)
		reply.ConflictTerm = -1
		reply.Success = false
		return
	}
	if rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.Log[args.PrevLogIndex].Term
		ci := args.PrevLogIndex
		for rf.Log[ci].Term == reply.ConflictTerm && ci > 0 {
			ci--
		}
		reply.ConflictIndex = ci + 1
		reply.Success = false
		return
	}

	rf.currentState = Follower
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.voteNum = 0
	}

	if args.Entries != nil {
		// check if there exists conflict entries
		splitIndex := 0
		findConflict := false
		for ; splitIndex < len(args.Entries); splitIndex++ {
			if args.PrevLogIndex+splitIndex+1 >= len(rf.Log) {
				break
			}
			if rf.Log[args.PrevLogIndex+splitIndex+1].Term != args.Entries[splitIndex].Term {
				findConflict = true
				break
			}
		}

		if findConflict {
			rf.Log = rf.Log[:args.PrevLogIndex+splitIndex+1]
		}

		for i := splitIndex; i < len(args.Entries); i++ {
			rf.Log = append(rf.Log, args.Entries[i])
		}
	}
	newCommitIndex := Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	rf.commitIndex = Max(rf.commitIndex, newCommitIndex)
	rf.applyLog()
	rf.persist()

	reply.Success = true
	reply.Term = rf.CurrentTerm
}

/**************************************************/

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isRunning {
		return
	}

	lastLogIndex := len(rf.Log) - 1
	lastLogTerm := rf.Log[lastLogIndex].Term

	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.voteNum = 0
		rf.currentState = Follower
	}

	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) &&
		isMoreUpToDate(args.LastLogTerm, args.LastLogIndex, lastLogTerm, lastLogIndex) {
		DPrintf("[term %d] peer(%d) grant vote to %d\n",
			rf.CurrentTerm, rf.me, args.CandidateId)
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateId
		rf.resetTimer()
	} else {
		DPrintf("[term %d] peer(%d) rf.VotedFor=%d args.Candidate=%d args.LastLogTerm=%d args.LastLogIndex=%d lastLogTerm=%d lastLogIndex=%d\n",
			rf.CurrentTerm, rf.me, rf.VotedFor, args.CandidateId, args.LastLogTerm, args.LastLogIndex, lastLogTerm, lastLogIndex)
		reply.VoteGranted = false
	}
	rf.persist()
}
