package main

import "sync"

type Role string

const (
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
	Leader    Role = "Leader"
)

type RaftNode struct {
	id    string
	peers []string // peer node addresses
	role  Role

	// data that needs to be written to disk
	currentTerm int
	votedFor    string
	log         *LogEntry

	// kv store
	store *Store

	// volatile fields
	commitIndex int
	lastApplied int

	// for each server index of the log_entry sent
	nextIndex map[string]int
	// for each server index of the
	// highest log_entry known to be commited
	matchIndex map[string]int

	// channels to change state or notify
	resetElectionCh chan struct{}
	commitNotifyCh  chan struct{}
	stopCh          chan struct{}

	mu sync.RWMutex
}

func NewRaftNode(id string, peers []string) *RaftNode {
	return &RaftNode{
		id:              id,
		peers:           peers,
		role:            Follower,
		log:             NewLogEntry(),
		store:           NewStore(),
		nextIndex:       make(map[string]int),
		matchIndex:      make(map[string]int),
		resetElectionCh: make(chan struct{}),
		commitNotifyCh:  make(chan struct{}),
		stopCh:          make(chan struct{}),
	}
}

func (rf *RaftNode) run() {
	rf.mu.Lock()
	role := rf.role
	rf.mu.Unlock()

	for {
		switch role {
		case Follower:
			// raftNode.runFollower()

		case Leader:
			// raftNode.runLeader()

		case Candidate:
			// raft.runCandidate()
		}
	}
}
