package main

import (
	"encoding/json"
	"os"
)

type PersistentState struct {
	CurrentTerm int
	VotedFor    string
	Entry       []Entry
}

func (rf *RaftNode) persist() error {
	state := PersistentState{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		Entry:       rf.log.entries,
	}

	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	fileName := rf.getPersistentFileName()
	return os.WriteFile(fileName, data, 0644)
}

func (rf *RaftNode) restore() error {
	fileName := rf.getPersistentFileName()
	data, err := os.ReadFile(fileName)
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
	rf.log.entries = state.Entry

	return nil
}

func (rf *RaftNode) getPersistentFileName() string {
	return "raft_state" + rf.id + ".json"
}
