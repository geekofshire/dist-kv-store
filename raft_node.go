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

	// volatile leaderID
	leaderID string

	// data that needs to be written to disk
	currentTerm int
	votedFor    string
	log         []Entry

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
		log:             make([]Entry, 0, 100),
		store:           NewStore(),
		nextIndex:       make(map[string]int),
		matchIndex:      make(map[string]int),
		resetElectionCh: make(chan struct{}, 2),
		commitNotifyCh:  make(chan struct{}, 2),
		stopCh:          make(chan struct{}),
		transport:       transport,
		commitIndex:     -1,
		lastApplied:     -1,
		leaderID:        "",
	}
}

func (rf *RaftNode) run() {
	for {
		rf.mu.RLock()
		role := rf.role
		rf.mu.RUnlock()

		switch role {
		case Follower:
			rf.runFollower()

		case Leader:
			rf.runLeader()

		case Candidate:
			rf.runCandidate()
		}
	}
}

func (rf *RaftNode) transitionToFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.role = Follower
	rf.votedFor = ""
}

func (rf *RaftNode) transitionToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLogIndex := func() int {
		if len(rf.log) == 0 {
			return -1
		}
		return rf.log[len(rf.log)-1].Index
	}()

	rf.role = Leader
	rf.leaderID = rf.id
	rf.votedFor = ""
	for _, peer := range rf.peers {
		rf.nextIndex[peer] = lastLogIndex + 1
		rf.matchIndex[peer] = -1
	}
}

func (rf *RaftNode) transitionToCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.role = Candidate
	rf.leaderID = ""
	rf.votedFor = rf.id
}

func (rf *RaftNode) Append(cmd CommandType, key string, value string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLogIndex := func() int {
		if len(rf.log) == 0 {
			return -1
		}
		return rf.log[len(rf.log)-1].Index
	}()

	entry := &Entry{
		Cmd:   cmd,
		Key:   key,
		Value: value,
		Term:  rf.currentTerm,
		Index: lastLogIndex + 1,
	}

	lastLogIndex = lastLogIndex + 1

	rf.log = append(rf.log, *entry)
	rf.persist()

	rf.matchIndex[rf.id] = lastLogIndex
	rf.nextIndex[rf.id] = lastLogIndex + 1
}
