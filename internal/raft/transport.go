package raft

type Transport interface {
	RequestVote(
		nodeID string,
		args RequestVoteArgs,
	) (RequestVoteReply, error)

	AppendEntries(
		nodeID string,
		args AppendEntriesArgs,
	) (AppendEntriesReply, error)
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  string
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	LeaderID     string
	LeaderTerm   int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}