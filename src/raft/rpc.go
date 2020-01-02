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

	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = -1
		return
	}
	if len(rf.log) <= args.PrevLogIndex {
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1
		reply.Success = false
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		ci := args.PrevLogIndex
		for rf.log[ci].Term == reply.ConflictTerm && ci > 0 {
			ci--
		}
		reply.ConflictIndex = ci + 1
		reply.Success = false
		return
	}

	rf.resetTimer()

	if args.Term >= rf.currentTerm {
		rf.currentState = Follower
		if args.Term > rf.currentTerm {
			DPrintf("[term:%d] %d get higher AppendEntriesArgs.Term(%d)\n",
				rf.currentTerm, rf.me, args.Term)
			rf.currentTerm = args.Term
			rf.votedFor = nil
			rf.voteNum = 0
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
	newCommitIndex := Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	rf.commitIndex = Max(rf.commitIndex, newCommitIndex)
	rf.applyLog()
	reply.Success = true
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

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		DPrintf("[term:%d] %d get higher RequestVoteArgs.Term(%d)\n",
			rf.currentTerm, rf.me, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = nil
		rf.voteNum = 0
		rf.currentState = Follower
	}

	if (rf.votedFor == nil || rf.votedFor == args.CandidateId) &&
		isMoreUpToDate(args.LastLogTerm, args.LastLogIndex, lastLogTerm, lastLogIndex) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.resetTimer()
	} else {
		reply.VoteGranted = false
	}
}
