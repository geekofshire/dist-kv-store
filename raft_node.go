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

	transport Transport
}

func NewRaftNode(id string, peers []string, transport Transport) *RaftNode {
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
		transport:       transport,
	}
}

func (rf *RaftNode) run() {
	for {
		rf.mu.RLock()
		role := rf.role
		rf.mu.RUnlock()

		switch role {
		case Follower:
			rf.becomeFollower()

		case Leader:
			// rf.becomeLeader()

		case Candidate:
			rf.becomeCandidate()
		}
	}
}
