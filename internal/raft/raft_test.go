package raft

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

func TestRequestVoteGrantsCurrentTerm(t *testing.T) {
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

	leader.mu.RLock()
	if resp.Term != leader.currentTerm {
		t.Fatalf("expected term to be updated, got %d want %d", leader.currentTerm, resp.Term)
	}
	leader.mu.RUnlock()

	resp = requestVote(t, leader, follower)

	if !resp.VoteGranted {
		t.Fatal("expected vote to be granted second time too")
	}

	leader.mu.RLock()
	defer leader.mu.RUnlock()
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
	appendTestEntry(follower, 1, Set, "y", "2")

	leader.mu.Lock()
	leader.currentTerm = 2
	leader.mu.Unlock()

	leader.mu.RLock()
	args := AppendEntriesArgs{
		LeaderID:     leader.id,
		LeaderTerm:   leader.currentTerm,
		PrevLogIndex: 1,
		PrevLogTerm:  2,
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

func TestTransitionToLeaderAdvancesNextIndex(t *testing.T) {
	nodes, _ := newTestCluster(t, []string{"leader", "follower"})

	leader := nodes[0]
	appendTestEntry(leader, 1, Set, "x", "1")
	appendTestEntry(leader, 1, Set, "y", "2")

	leader.transitionToLeader()

	leader.mu.RLock()
	defer leader.mu.RUnlock()
	for _, peer := range leader.peers {
		// checking against 2 since we are using 0 based indexing
		if leader.nextIndex[peer] != 2 {
			t.Fatalf("expected next index for %s to be %d got %d", peer, 2, leader.nextIndex[peer])
		}
	}
}

func TestTransitionToLeaderInitializeMatchIndex(t *testing.T) {
	nodes, _ := newTestCluster(t, []string{"leader", "follower"})

	leader := nodes[0]
	appendTestEntry(leader, 1, Set, "x", "1")
	appendTestEntry(leader, 1, Set, "y", "2")

	leader.transitionToLeader()

	for _, peer := range leader.peers {
		_, ok := leader.matchIndex[peer]
		if !ok {
			t.Fatal("expected match index to be initialized for leaders")
		}
	}
}

func TestFailedAppendEntriesReplyDecrementNextIndex(t *testing.T) {
	nodes, _ := newTestCluster(t, []string{"leader", "follower"})

	leader := nodes[0]

	leader.transitionToLeader()
	leader.mu.Lock()
	leader.nextIndex["follower"] = 5

	// mock args & response
	args := AppendEntriesArgs{
		LeaderTerm: leader.currentTerm,
	}

	resp := AppendEntriesReply{
		Success: false,
	}

	leader.mu.Unlock()

	leader.handleAppendEntriesReply("follower", args, resp)

	leader.mu.RLock()
	defer leader.mu.RUnlock()

	if leader.nextIndex["follower"] != 4 {
		t.Fatalf("got %d expected %d", leader.nextIndex["follower"], 4)
	}
}

func TestSuccessfulAppendEntriesUpdateMatchAndNextIndex(t *testing.T) {
	nodes, _ := newTestCluster(t, []string{"leader", "follower"})

	leader := nodes[0]
	follower := nodes[1]

	appendTestEntry(leader, 1, Set, "a", "1")
	appendTestEntry(leader, 1, Set, "b", "2")
	appendTestEntry(leader, 1, Set, "c", "3")

	appendTestEntry(follower, 1, Set, "a", "1")
	appendTestEntry(follower, 1, Set, "b", "2")

	leader.mu.Lock()
	leader.role = Leader
	leader.currentTerm = 1
	leader.matchIndex["follower"] = 1
	leader.nextIndex["follower"] = 2
	leader.mu.Unlock()

	args := makeAppendEntriesArgs(t, leader, follower)

	resp := appendEntries(t, leader, follower, args)

	leader.handleAppendEntriesReply("follower", args, resp)

	if leader.matchIndex["follower"] != 2 {
		t.Fatalf("expected match index to be %d got %d", 2, leader.matchIndex["follower"])
	}

	if leader.nextIndex["follower"] != 3 {
		t.Fatalf("expected next index to be %d got %d", 3, leader.nextIndex["follower"])
	}
}

func TestTryAdvanceCommitIndexNoMajority(t *testing.T) {
	nodes, _ := newTestCluster(t, []string{"leader", "f1", "f2"})

	leader := nodes[0]

	appendTestEntry(leader, 0, Set, "x", "1")

	leader.transitionToLeader()
	leader.mu.Lock()
	leader.commitIndex = -1
	leader.matchIndex["f1"] = -1
	leader.matchIndex["f2"] = -1
	leader.mu.Unlock()

	leader.tryAdvanceCommitIndex()

	leader.mu.RLock()
	defer leader.mu.RUnlock()

	if leader.commitIndex != -1 {
		t.Fatalf("expected commit index to be %d got %d", -1, leader.commitIndex)
	}
}

func TestTryAdvanceCommitIndexWithMajority(t *testing.T) {
	nodes, _ := newTestCluster(t, []string{"leader", "f1", "f2"})

	leader := nodes[0]

	appendTestEntry(leader, 0, Set, "x", "1")

	leader.transitionToLeader()
	leader.mu.Lock()
	leader.commitIndex = -1
	leader.matchIndex["f1"] = 0
	leader.matchIndex["f2"] = -1
	leader.mu.Unlock()

	leader.tryAdvanceCommitIndex()

	leader.mu.RLock()
	defer leader.mu.RUnlock()

	if leader.commitIndex != 0 {
		t.Fatalf("expected commit index to be %d got %d", 0, leader.commitIndex)
	}
}

func TestTryAdvanceCommitIndexSkipsOldTermEntries(t *testing.T) {
	nodes, _ := newTestCluster(t, []string{"leader", "f1", "f2"})

	leader := nodes[0]

	appendTestEntry(leader, 0, Set, "x", "1")

	leader.transitionToLeader()
	leader.mu.Lock()
	leader.currentTerm = 2
	leader.commitIndex = -1
	leader.matchIndex["f1"] = 0
	leader.matchIndex["f2"] = 0
	leader.mu.Unlock()

	leader.tryAdvanceCommitIndex()

	leader.mu.RLock()
	defer leader.mu.RUnlock()

	if leader.commitIndex != -1 {
		t.Fatalf("expected commit index to be %d got %d", -1, leader.commitIndex)
	}
}

func TestLeaderReplicatesEntryToFollower(t *testing.T) {
	nodes, _ := newTestCluster(t, []string{"leader", "follower"})

	leader := nodes[0]
	follower := nodes[1]

	leader.transitionToLeader()
	appendTestEntry(leader, 0, Set, "x", "1")
	follower.transitionToFollower()

	go leader.runLeader()

	t.Cleanup(func() {
		close(leader.stopCh)
	})

	waitUntil(
		t,
		2*time.Second,
		func() bool {
			follower.mu.RLock()
			defer follower.mu.RUnlock()
			return len(follower.log) == 1
		},
	)

	follower.mu.RLock()
	defer follower.mu.RUnlock()

	leader.mu.RLock()
	defer leader.mu.RUnlock()

	if leader.matchIndex["follower"] != 0 {
		t.Fatal("matchIndex not updated")
	}

	if leader.nextIndex["follower"] != 1 {
		t.Fatal("nextIndex not updated")
	}

	lastFollowerLog := follower.log[len(follower.log)-1]

	if lastFollowerLog.Key != "x" || lastFollowerLog.Value != "1" || lastFollowerLog.Cmd != Set {
		t.Fatal("log not replicated successfully")
	}
}

func TestLeaderCommitsAfterMajorityReplication(t *testing.T) {
	nodes, _ := newTestCluster(t, []string{"leader", "f1", "f2"})

	leader := nodes[0]
	f1 := nodes[1]

	leader.transitionToLeader()
	appendTestEntry(leader, 0, Set, "x", "1")

	args := makeAppendEntriesArgs(t, leader, f1)

	resp := appendEntries(t, leader, f1, args)

	leader.handleAppendEntriesReply("f1", args, resp)

	waitUntil(t, 1*time.Second, func() bool {
		leader.mu.RLock()
		defer leader.mu.RUnlock()

		return leader.commitIndex == 0
	})
}

func TestCommittedEntryAppliesToLeaderStore(t *testing.T) {
	nodes, _ := newTestCluster(t, []string{"leader", "follower"})

	leader := nodes[0]
	follower := nodes[1]

	leader.transitionToLeader()
	appendTestEntry(leader, leader.currentTerm, Set, "x", "1")
	follower.transitionToFollower()

	go leader.runLeader()
	go leader.ApplyLoop()

	t.Cleanup(func() {
		close(leader.stopCh)
	})

	waitUntil(t, 2*time.Second, func() bool {
		leader.mu.RLock()
		defer leader.mu.RUnlock()

		val, ok := leader.store.Get("x")

		return ok && val == "1"
	})
}

func TestFollowerAppliesLeaderCommit(t *testing.T) {
	nodes, _ := newTestCluster(t, []string{"leader", "follower"})

	leader := nodes[0]
	follower := nodes[1]

	leader.transitionToLeader()
	appendTestEntry(leader, leader.currentTerm, Set, "x", "1")

	follower.transitionToFollower()
	appendTestEntry(follower, leader.currentTerm, Set, "x", "1")
	go follower.ApplyLoop()

	t.Cleanup(func() {
		close(follower.stopCh)
	})

	leader.mu.Lock()
	leader.commitIndex = 0
	leader.mu.Unlock()

	args := makeAppendEntriesArgs(t, leader, follower)

	resp := appendEntries(t, leader, follower, args)
	if !resp.Success {
		t.Fatal("expected append success")
	}

	waitUntil(t, 2*time.Second, func() bool {
		follower.mu.RLock()
		defer follower.mu.RUnlock()

		val, ok := follower.store.Get("x")

		return ok && val == "1"
	})
}

func TestThreeNodeClusterReplicatesAndAppliesCommand(t *testing.T) {
	nodes, _ := newTestCluster(t, []string{"node1", "node2", "node3"})

	// node1 := nodes[0]
	// node2 := nodes[1]
	// node3 := nodes[2]
	var leader *RaftNode

	for _, node := range nodes {
		go node.run()
		go node.ApplyLoop()
	}

	waitUntil(t, 2*time.Second, func() bool {
		isAnyoneLeader := false

		for _, node := range nodes {
			if getRole(node) == Leader {
				leader = node
				isAnyoneLeader = true
			}
		}

		return isAnyoneLeader
	})

	leader.Append(Set, "x", "1")

	waitUntil(t, 2*time.Second, func() bool {
		for _, node := range nodes {
			val, ok := node.store.Get("x")

			if !ok || val != "1" {
				return false
			}
		}
		return true
	})
}
