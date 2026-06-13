package raft

import (
	"fmt"
	"sync"
)

type MockTransport struct {
	nodes map[string]*RaftNode
	mu    sync.Mutex
}

func NewMockTransport() *MockTransport {
	return &MockTransport{
		nodes: make(map[string]*RaftNode),
	}
}

func (mt *MockTransport) AppendNodes(nodeID string, node *RaftNode) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	mt.nodes[nodeID] = node
}

func (mt *MockTransport) LeaderNode() (*RaftNode, error) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	for _, node := range mt.nodes {
		node.mu.Lock()
		if node.role == Leader {
			node.mu.Unlock()
			return node, nil
		}
		node.mu.Unlock()
		continue
	}

	return nil, fmt.Errorf("No leader node found")
}

func (mt *MockTransport) StartAll() {
	mt.mu.Lock()
	nodes := make([]*RaftNode, 0, len(mt.nodes))
	for _, node := range mt.nodes {
		nodes = append(nodes, node)
	}
	mt.mu.Unlock()

	for _, node := range nodes {
		go node.Run()
		go node.ApplyLoop()
	}
}

func (mt *MockTransport) RequestVote(
	nodeID string,
	args RequestVoteArgs,
) (RequestVoteReply, error) {

	mt.mu.Lock()
	node, ok := mt.nodes[nodeID]
	mt.mu.Unlock()

	if !ok {
		return RequestVoteReply{}, fmt.Errorf("node not found")
	}

	term, granted := node.RequestVote(
		args.Term,
		args.CandidateID,
		args.LastLogIndex,
		args.LastLogTerm,
	)

	return RequestVoteReply{
		Term:        term,
		VoteGranted: granted,
	}, nil
}

func (mt *MockTransport) AppendEntries(
	nodeID string,
	args AppendEntriesArgs,
) (AppendEntriesReply, error) {

	mt.mu.Lock()
	node, ok := mt.nodes[nodeID]
	mt.mu.Unlock()

	if !ok {
		return AppendEntriesReply{}, fmt.Errorf("node not found")
	}

	term, success := node.AppendEntries(
		args.LeaderID,
		args.LeaderTerm,
		args.PrevLogIndex,
		args.PrevLogTerm,
		args.Entries,
		args.LeaderCommit,
	)

	return AppendEntriesReply{
		Term:    term,
		Success: success,
	}, nil
}
