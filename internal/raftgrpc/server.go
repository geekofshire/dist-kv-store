package raftgrpc

import (
	"context"

	"github.com/geekofshire/dist-kv-store/internal/raft"
	raftv1 "github.com/geekofshire/dist-kv-store/internal/raftpb/raftv1"
)

type Server struct {
	raftv1.UnimplementedRaftServiceServer
	node *raft.RaftNode
}

func NewServer(node *raft.RaftNode) *Server {
	return &Server{node: node}
}

func (s *Server) RequestVote(
	ctx context.Context,
	req *raftv1.RequestVoteRequest,
) (*raftv1.RequestVoteResponse, error) {
	term, granted := s.node.RequestVote(
		int(req.GetTerm()),
		req.GetCandidateId(),
		int(req.GetLastLogIndex()),
		int(req.GetLastLogTerm()),
	)

	return &raftv1.RequestVoteResponse{
		Term:        int32(term),
		VoteGranted: granted,
	}, nil
}

func (s *Server) AppendEntries(
	ctx context.Context,
	req *raftv1.AppendEntriesRequest,
) (*raftv1.AppendEntriesResponse, error) {
	term, success := s.node.AppendEntries(
		req.GetLeaderId(),
		int(req.GetLeaderTerm()),
		int(req.GetPrevLogIndex()),
		int(req.GetPrevLogTerm()),
		entriesFromProto(req.GetEntries()),
		int(req.GetLeaderCommit()),
	)

	return &raftv1.AppendEntriesResponse{
		Term:    int32(term),
		Success: success,
	}, nil
}
