package raft

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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := (rf.currentState == Leader)
	if !isLeader {
		return index, term, isLeader
	}

	entry := Entry{rf.currentTerm, command}
	rf.log = append(rf.log, entry)

	index = len(rf.log) - 1
	term = rf.currentTerm

	for i, _ := range rf.peers {
		go rf.trySendAppendEntries(i)
	}

	rf.nextIndex[rf.me] = Max(rf.nextIndex[rf.me], len(rf.log))
	rf.matchIndex[rf.me] = Max(rf.matchIndex[rf.me], len(rf.log)-1)
	rf.updateCommitIndex()

	return index, term, isLeader
}

func (rf *Raft) updateCommitIndex() {
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
	if rf.commitIndex > rf.lastApplied {
		rf.applyLog()
	}
}

func (rf *Raft) applyLog() {
	for rf.commitIndex > rf.lastApplied &&
		rf.lastApplied < len(rf.log)-1 {
		var applyMsg ApplyMsg
		applyMsg.CommandValid = true
		applyMsg.Command = rf.log[rf.lastApplied+1].Command
		applyMsg.CommandIndex = rf.lastApplied + 1
		rf.applyCh <- applyMsg
		rf.lastApplied++
	}
}
