package main

import (
	"math/rand"
	"time"
)

func randomElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (rf *RaftNode) becomeFollower() {
	timeout := time.NewTimer(randomElectionTimeout())
	defer timeout.Stop()

	for {
		select {
		case <-rf.resetElectionCh:
			if !timeout.Stop() {
				select {
				case <-timeout.C:
				default:
				}
			}

			timeout.Reset(randomElectionTimeout())

		case <-timeout.C:
			rf.mu.Lock()
			rf.role = Candidate
			rf.mu.Unlock()
			return
		}
	}
}

func (rf *RaftNode) AppendEntries(
	leaderTerm int,
	prevLogIndex int,
	prevLogTerm int,
	entries []Entry,
	leaderCommit int,
) (int, bool) {

	var (
		shouldResetElection bool
		shouldNotifyCommit  bool
	)

	rf.mu.Lock()

	// reject stale leader
	if leaderTerm < rf.currentTerm {
		term := rf.currentTerm
		rf.mu.Unlock()
		return term, false
	}

	if leaderTerm > rf.currentTerm {
		rf.currentTerm = leaderTerm
	}
	rf.role = Follower

	if prevLogIndex >= 0 {

		// some entries are missing
		if prevLogIndex >= len(rf.log.entries) {
			term := rf.currentTerm
			rf.mu.Unlock()
			return term, false
		}

		if rf.log.entries[prevLogIndex].Term != prevLogTerm {
			term := rf.currentTerm
			rf.mu.Unlock()
			return term, false
		}
	}

	insertIndex := prevLogIndex + 1
	for index, entry := range entries {
		currentIndex := insertIndex + index

		if currentIndex < len(rf.log.entries) {
			if rf.log.entries[currentIndex].Term != entry.Term {
				rf.log.entries = rf.log.entries[:currentIndex]

				rf.log.entries = append(rf.log.entries, entries[index:]...)
				break
			}
			continue
		}
		rf.log.entries = append(rf.log.entries, entries[index:]...)
		break
	}

	if leaderCommit > rf.commitIndex {
		lastIndex := len(rf.log.entries) - 1
		newCommitIndex := min(leaderCommit, lastIndex)

		if newCommitIndex > rf.commitIndex {
			rf.commitIndex = newCommitIndex
			shouldNotifyCommit = true
		}
	}

	term := rf.currentTerm
	shouldResetElection = true
	rf.mu.Unlock()

	if shouldNotifyCommit {
		rf.notifyCommit()
	}

	if shouldResetElection {
		rf.resetElectionTimer()
	}

	return term, true
}

func (rf *RaftNode) resetElectionTimer() {
	select {
	case rf.resetElectionCh <- struct{}{}:
	default:
	}
}

func (rf *RaftNode) notifyCommit() {
	select {
	case rf.commitNotifyCh <- struct{}{}:
	default:
	}
}

func (rf *RaftNode) ApplyLoop() {
	for {
		<-rf.commitNotifyCh

		rf.mu.Lock()

		if rf.commitIndex <= rf.lastApplied {
			rf.mu.Unlock()
			continue
		}

		entries := append(
			[]Entry(nil),
			rf.log.entries[rf.lastApplied+1:rf.commitIndex+1]...,
		)

		rf.lastApplied = rf.commitIndex

		rf.mu.Unlock()

		for _, entry := range entries {
			rf.store.ApplyLog(entry)
		}
	}
}
