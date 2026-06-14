package raftgrpc

import (
	"context"
	"fmt"
	"time"

	"github.com/geekofshire/dist-kv-store/internal/raft"
	raftv1 "github.com/geekofshire/dist-kv-store/internal/raftpb/raftv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Transport struct {
	clients map[string]raftv1.RaftServiceClient
	conns   map[string]*grpc.ClientConn
	timeout time.Duration
}

func NewTransport(peerAddrs map[string]string, timeout time.Duration) (*Transport, error) {
	if timeout == 0 {
		timeout = 200 * time.Millisecond
	}

	t := &Transport{
		clients: make(map[string]raftv1.RaftServiceClient, len(peerAddrs)),
		conns:   make(map[string]*grpc.ClientConn, len(peerAddrs)),
		timeout: timeout,
	}

	for peerID, addr := range peerAddrs {
		conn, err := grpc.NewClient(
			addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			t.Close()
			return nil, fmt.Errorf("create grpc client for peer %s: %w", peerID, err)
		}

		t.conns[peerID] = conn
		t.clients[peerID] = raftv1.NewRaftServiceClient(conn)
	}

	return t, nil
}

func (t *Transport) Close() error {
	var firstErr error
	for peerID, conn := range t.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close peer %s connection: %w", peerID, err)
		}
	}
	return firstErr
}

func (t *Transport) RequestVote(
	nodeID string,
	args raft.RequestVoteArgs,
) (raft.RequestVoteReply, error) {
	client, ok := t.clients[nodeID]
	if !ok {
		return raft.RequestVoteReply{}, fmt.Errorf("peer %s not found", nodeID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()

	resp, err := client.RequestVote(ctx, &raftv1.RequestVoteRequest{
		Term:         int32(args.Term),
		CandidateId:  args.CandidateID,
		LastLogIndex: int32(args.LastLogIndex),
		LastLogTerm:  int32(args.LastLogTerm),
	})
	if err != nil {
		return raft.RequestVoteReply{}, err
	}

	return raft.RequestVoteReply{
		Term:        int(resp.GetTerm()),
		VoteGranted: resp.GetVoteGranted(),
	}, nil
}

func (t *Transport) AppendEntries(
	nodeID string,
	args raft.AppendEntriesArgs,
) (raft.AppendEntriesReply, error) {
	client, ok := t.clients[nodeID]
	if !ok {
		return raft.AppendEntriesReply{}, fmt.Errorf("peer %s not found", nodeID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()

	resp, err := client.AppendEntries(ctx, &raftv1.AppendEntriesRequest{
		LeaderId:     args.LeaderID,
		LeaderTerm:   int32(args.LeaderTerm),
		PrevLogIndex: int32(args.PrevLogIndex),
		PrevLogTerm:  int32(args.PrevLogTerm),
		Entries:      entriesToProto(args.Entries),
		LeaderCommit: int32(args.LeaderCommit),
	})
	if err != nil {
		return raft.AppendEntriesReply{}, err
	}

	return raft.AppendEntriesReply{
		Term:    int(resp.GetTerm()),
		Success: resp.GetSuccess(),
	}, nil
}
