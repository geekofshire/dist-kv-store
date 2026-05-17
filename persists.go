package main

import (
	"encoding/json"
	"os"
)

type PersistentState struct {
	CurrentTerm int
	VotedFor string
	Log *LogEntry
}

func (rf *RaftNode) persist() error {
    state := PersistentState{
        CurrentTerm: rf.currentTerm,
        VotedFor:    rf.votedFor,
        Log:         rf.log,
    }

    data, err := json.Marshal(state)
    if err != nil {
        return err
    }

    return os.WriteFile("raft_state.json", data, 0644)
}

func (rf *RaftNode) restore() error {
	data, err := os.ReadFile("raft_state.json")
	if err != nil {
		return err
	}

	var state PersistentState
	err = json.Unmarshal(data, &state)
	if err != nil {
		return err
	}

	rf.currentTerm = state.CurrentTerm
	rf.votedFor = state.VotedFor
	rf.log = state.Log

	return nil
}