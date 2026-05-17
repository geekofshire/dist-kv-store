package main

func (rf *RaftNode) becomeCandidate() {
	rf.mu.Lock()

	rf.currentTerm += 1
	rf.votedFor = rf.id
	rf.resetElectionTimer()
	rf.mu.Unlock()

	// Transport layer here to receive votes

	votes := 3 // random number for now
	if votes >= (len(rf.peers) / 2) + 1 {
		rf.role = Leader
		// sendAppendEntry to followers (in leader.go)
	}
}

func (rf *RaftNode) RequestVote(term int, candidateID string, lastLogIndex, lastLogTerm int) (int, bool) {
	rf.mu.Lock()
    defer rf.mu.Unlock()

	if term < rf.currentTerm {
		return rf.currentTerm, false
	}

 	if term > rf.currentTerm {
        rf.currentTerm = term
        rf.role = Follower
        rf.votedFor = ""
    }

    if rf.votedFor != "" && rf.votedFor != candidateID {
        return rf.currentTerm, false
    }

    currentLastTerm := 0
    currentLastIndex := 0
    
    if len(rf.log.entries) > 0 {
        last := rf.log.entries[len(rf.log.entries)-1]
        currentLastTerm = last.Term
        currentLastIndex = last.Index
    }

    upToDate := lastLogTerm > currentLastTerm || (lastLogTerm == currentLastTerm && lastLogIndex >= currentLastIndex)

    if !upToDate {
   		return rf.currentTerm, false
    }

    rf.votedFor = candidateID
    rf.resetElectionTimer()
	return rf.currentTerm, true
}