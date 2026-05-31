package main

import (
	"testing"
	"time"
)

func newTestCluster(t *testing.T, ids []string) ([]*RaftNode, Transport) {
	t.Helper()

	mt := NewMockTransport()
	var nodes []*RaftNode

	for _, id := range ids {
		node := NewRaftNode(id, ids, mt)
		mt.AppendNodes(id, node)
		nodes = append(nodes, node)
	}

	return nodes, mt
}

func forceLeader(t *testing.T, node *RaftNode) {
	t.Helper()
	node.transitionToLeader()
}

func waitUntil(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {

		if condition() {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("condition not met before timeout")
}

func appendTestEntry(
	rf *RaftNode,
	term int,
	cmd CommandType,
	key string,
	value string,
) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := len(rf.log)

	rf.log = append(rf.log, Entry{
		Cmd:   cmd,
		Key:   key,
		Value: value,
		Term:  term,
		Index: index,
	})
}

func getRole(rf *RaftNode) Role {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	return rf.role
}

func getLog(rf *RaftNode) []Entry {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	entryCopy := make([]Entry, len(rf.log))

	copy(entryCopy, rf.log)
	return entryCopy
}

func getCommitIndex(rf *RaftNode) int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	return rf.commitIndex
}

func assertLogLen(t *testing.T, rf *RaftNode, want int) {
	t.Helper()

	rf.mu.Lock()
	defer rf.mu.Unlock()

	got := len(rf.log)

	if got != want {
		t.Fatalf("got %d want %d", got, want)
	}
}

func assertKeyValue(t *testing.T, rf *RaftNode, key, want string) {
	t.Helper()

	got, ok := rf.store.Get(key)
	if !ok {
		t.Fatalf("error getting value of key : %s", key)
	}

	if got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

func (rf *RaftNode) getLastLogIndexAndTerm() (int, int) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	currentLastTerm := 0
	currentLastIndex := 0
	if len(rf.log) > 0 {
		last := rf.log[len(rf.log)-1]
		currentLastTerm = last.Term
		currentLastIndex = last.Index
	}

	return currentLastIndex, currentLastTerm
}

func requestVote(t *testing.T, candidate, target *RaftNode) RequestVoteReply {
	t.Helper()

	lastIndex, lastTerm :=
		candidate.getLastLogIndexAndTerm()

	candidate.mu.RLock()

	args := RequestVoteArgs{
		Term:         candidate.currentTerm,
		CandidateID:  candidate.id,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}

	candidate.mu.RUnlock()

	target.mu.RLock()
	targetID := target.id
	target.mu.RUnlock()

	resp, err := candidate.transport.RequestVote(
		targetID,
		args,
	)

	if err != nil {
		t.Fatal(err)
	}

	return resp
}

func makeAppendEntriesArgs(t *testing.T, leader, target *RaftNode) AppendEntriesArgs {
	t.Helper()

	leader.mu.RLock()
	defer leader.mu.RUnlock()

	next := leader.nextIndex[target.id]

	next = max(
		0,
		min(next, len(leader.log)),
	)

	prevIndex := next - 1

	prevTerm := 0

	if prevIndex >= 0 &&
		prevIndex < len(leader.log) {

		prevTerm = leader.log[prevIndex].Term
	}

	entries := append(
		[]Entry(nil),
		leader.log[next:]...,
	)

	return AppendEntriesArgs{
		LeaderID:     leader.id,
		LeaderTerm:   leader.currentTerm,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: leader.commitIndex,
	}
}

func appendEntries(t *testing.T, leader, target *RaftNode, args AppendEntriesArgs) AppendEntriesReply {
	t.Helper()

	resp, err := leader.transport.AppendEntries(
		target.id,
		args,
	)

	if err != nil {
		t.Fatal(err)
	}

	return resp
}
