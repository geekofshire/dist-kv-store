package raft

import (
	"sync"
)

func (rf *RaftNode) runCandidate() {
	rf.mu.Lock()

	rf.currentTerm += 1
	rf.votedFor = rf.id
	rf.persistLocked()
	rf.mu.Unlock()
	rf.resetElectionTimer()
	rf.mu.Lock()
	currentLastTerm := 0
	currentLastIndex := 0
	electionTerm := rf.currentTerm

	if len(rf.log) > 0 {
		last := rf.log[len(rf.log)-1]
		currentLastTerm = last.Term
		currentLastIndex = last.Index
	}

	request_vote_args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.id,
		LastLogIndex: currentLastIndex,
		LastLogTerm:  currentLastTerm,
	}

	var wg sync.WaitGroup
	results := make(chan RequestVoteReply, len(rf.peers))
	rf.mu.Unlock()

	for _, peer := range rf.peers {
		if peer == rf.id {
			continue
		}

		wg.Add(1)
		go func(peer string) {
			defer wg.Done()

			resp, err := rf.transport.RequestVote(peer, request_vote_args)
			if err != nil {
				return
			}

			select {
			case <-rf.stopCh:
				return

			case results <- resp:
			}
		}(peer)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	voteCount := 1 // self-vote included

	for {

		select {

		case <-rf.stopCh:
			return

		case result, ok := <-results:
			if !ok {
				return
			}

			rf.mu.Lock()

			if rf.currentTerm != electionTerm {
				rf.mu.Unlock()
				return
			}

			if result.Term > rf.currentTerm {
				rf.currentTerm = result.Term
				rf.votedFor = ""
				rf.persistLocked()
				rf.mu.Unlock()
				rf.transitionToFollower()
				return
			}

			if !result.VoteGranted {
				rf.mu.Unlock()
				continue
			}

			voteCount += 1
			shouldBecomeLeader :=
				voteCount >= (len(rf.peers)/2)+1 &&
					rf.role == Candidate &&
					rf.currentTerm == electionTerm

			rf.mu.Unlock()

			if shouldBecomeLeader {
				rf.transitionToLeader()
				return
			}
		}
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

	if len(rf.log) > 0 {
		last := rf.log[len(rf.log)-1]
		currentLastTerm = last.Term
		currentLastIndex = last.Index
	}

	upToDate := lastLogTerm > currentLastTerm || (lastLogTerm == currentLastTerm && lastLogIndex >= currentLastIndex)

	if !upToDate {
		return rf.currentTerm, false
	}

	rf.votedFor = candidateID
	rf.resetElectionTimer()
	rf.persistLocked()
	return rf.currentTerm, true
}
