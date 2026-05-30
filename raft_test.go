package main

import (
	"testing"
	"time"
)

func TestRequestVoteRejectsStaleTerm(t *testing.T) {
	nodes, _ := newTestCluster(t, []string{"leader", "follower"})

	leader := nodes[0]
	follower := nodes[1]

	follower.mu.Lock()
	follower.currentTerm = 99
	follower.mu.Unlock()

	leader.transitionToCandidate()
	resp := requestVote(t, leader, follower)

	if resp.VoteGranted {
		t.Fatal("expected vote not be granted since term is outdated")
	}
}

func TestRequestVoteGrantsFreshTerm(t *testing.T) {
	nodes, _ := newTestCluster(t, []string{"leader", "follower"})

	leader := nodes[0]
	follower := nodes[1]

	leader.mu.RLock()
	currentTerm := leader.currentTerm
	leader.mu.RUnlock()

	leader.transitionToCandidate()
	follower.transitionToFollower()

	resp := requestVote(t, leader, follower)

	if !resp.VoteGranted {
		t.Fatal("expected vote to be granted")
	}

	if resp.Term != currentTerm {
		t.Fatalf("expected term to be updated, got %d want %d", leader.currentTerm, resp.Term)
	}
}

// if lastLogTerm is greater than the candidate requesting vote
// or if term is equal but lastLogIndex is greater than candidates' lastLogIndex
func TestRequestVoteRejectsStaleLog(t *testing.T) {
	nodes, _ := newTestCluster(t, []string{"leader", "follower"})

	leader := nodes[0]
	follower := nodes[1]

	appendTestEntry(leader, 1, Set, "x", "1")
	appendTestEntry(follower, 1, Set, "x", "1")
	appendTestEntry(follower, 2, Set, "y", "2")

	leader.mu.Lock()
	leader.currentTerm = 3
	leader.role = Candidate
	leader.mu.Unlock()

	resp := requestVote(t, leader, follower)

	if resp.VoteGranted {
		t.Fatal("expected stale log candidate to be rejected")
	}
}

func TestRequestVoteAllowsDuplicateForSameCandidate(t *testing.T) {
	nodes, _ := newTestCluster(t, []string{"leader", "follower"})

	leader := nodes[0]
	follower := nodes[1]

	leader.transitionToCandidate()
	follower.transitionToFollower()

	resp := requestVote(t, leader, follower)

	if !resp.VoteGranted {
		t.Fatal("expected vote to be granted")
	}

	if resp.Term != leader.currentTerm {
		t.Fatalf("expected term to be updated, got %d want %d", leader.currentTerm, resp.Term)
	}

	resp = requestVote(t, leader, follower)

	if !resp.VoteGranted {
		t.Fatal("expected vote to be granted second time too")
	}

	if resp.Term != leader.currentTerm {
		t.Fatalf("expected term to be updated, got %d want %d", leader.currentTerm, resp.Term)
	}
}

func TestAppendEntriesRejectStaleTerm(t *testing.T) {
	nodes, _ := newTestCluster(t, []string{"leader", "follower"})

	leader := nodes[0]
	follower := nodes[1]

	leader.transitionToLeader()

	follower.mu.Lock()
	follower.currentTerm = 99
	follower.mu.Unlock()

	leader.mu.RLock()
	args := AppendEntriesArgs{
		LeaderID:     leader.id,
		LeaderTerm:   leader.currentTerm,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []Entry{},
		LeaderCommit: leader.commitIndex,
	}
	leader.mu.RUnlock()

	resp := appendEntries(t, leader, follower, args)
	if resp.Success {
		t.Fatal("expected request to be rejected when term is stale")
	}
}

func TestAppendEntriesRejectMissingLogIndex(t *testing.T) {
	nodes, _ := newTestCluster(t, []string{"leader", "follower"})

	leader := nodes[0]
	follower := nodes[1]

	appendTestEntry(leader, 1, Set, "x", "1")
	appendTestEntry(leader, 1, Set, "y", "2")
	appendTestEntry(follower, 1, Set, "x", "1")

	leader.mu.RLock()
	args := AppendEntriesArgs{
		LeaderID:     leader.id,
		LeaderTerm:   leader.currentTerm,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []Entry{},
		LeaderCommit: leader.commitIndex,
	}
	leader.mu.RUnlock()

	resp := appendEntries(t, leader, follower, args)
	if resp.Success {
		t.Fatal("expected request to be rejected when prev index is missing")
	}
}

func TestAppendEntriesRejectMismatchTerm(t *testing.T) {
	nodes, _ := newTestCluster(t, []string{"leader", "follower"})

	leader := nodes[0]
	follower := nodes[1]

	appendTestEntry(leader, 2, Set, "x", "1")
	appendTestEntry(leader, 2, Set, "y", "2")
	appendTestEntry(follower, 1, Set, "x", "1")

	leader.mu.Lock()
	leader.currentTerm = 2
	leader.mu.Unlock()

	leader.mu.RLock()
	args := AppendEntriesArgs{
		LeaderID:     leader.id,
		LeaderTerm:   leader.currentTerm,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []Entry{},
		LeaderCommit: leader.commitIndex,
	}
	leader.mu.RUnlock()

	resp := appendEntries(t, leader, follower, args)
	if resp.Success {
		t.Fatal("expected request to be rejected when term does not match")
	}
}

func TestAppendEntriesAppendsNewEntries(t *testing.T) {
	nodes, _ := newTestCluster(t, []string{"leader", "follower"})

	leader := nodes[0]
	follower := nodes[1]

	appendTestEntry(leader, 1, Set, "x", "1")
	appendTestEntry(leader, 1, Set, "y", "2")
	appendTestEntry(follower, 1, Set, "x", "1")

	args := makeAppendEntriesArgs(t, leader, follower)

	resp := appendEntries(t, leader, follower, args)

	if !resp.Success {
		t.Fatal("expected proper entries to be accepted")
	}
}

func TestAppendEntriesTruncatesConflictingEntries(t *testing.T) {
	nodes, _ := newTestCluster(t, []string{"leader", "follower"})

	leader := nodes[0]
	follower := nodes[1]

	appendTestEntry(leader, 1, Set, "a", "1")
	appendTestEntry(leader, 1, Set, "b", "2")
	appendTestEntry(leader, 3, Set, "c", "3")
	appendTestEntry(leader, 3, Set, "d", "4")

	appendTestEntry(follower, 1, Set, "a", "1")
	appendTestEntry(follower, 1, Set, "b", "2")
	appendTestEntry(follower, 2, Set, "x", "bad")
	appendTestEntry(follower, 2, Set, "y", "bad")

	args := AppendEntriesArgs{
		LeaderID:     leader.id,
		LeaderTerm:   3,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: append(
			[]Entry(nil),
			leader.log[2:]...,
		),
	}

	resp, err := leader.transport.AppendEntries(
		follower.id,
		args,
	)

	if err != nil {
		t.Fatal(err)
	}

	if !resp.Success {
		t.Fatal("expected append success")
	}

	log := getLog(follower)

	if len(log) != 4 {
		t.Fatalf("got log len %d", len(log))
	}

	if log[2].Term != 3 {
		t.Fatal("expected conflicting entry replaced")
	}

	if log[2].Key != "c" {
		t.Fatal("expected leader entry")
	}
}

func TestAppendEntriesAdvancesCommitIndexFromLeaderCommit(t *testing.T) {
	nodes, _ := newTestCluster(t, []string{"leader", "follower"})

	leader := nodes[0]
	follower := nodes[1]

	appendTestEntry(follower, 1, Set, "a", "1")
	appendTestEntry(follower, 1, Set, "b", "2")
	appendTestEntry(follower, 1, Set, "c", "3")

	args := AppendEntriesArgs{
		LeaderID:     leader.id,
		LeaderTerm:   1,
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries:      nil,
		LeaderCommit: 2,
	}

	resp := appendEntries(t, leader, follower, args)

	if !resp.Success {
		t.Fatal("expected success")
	}

	waitUntil(t, time.Second, func() bool {
		return getCommitIndex(follower) == 2
	})
}
