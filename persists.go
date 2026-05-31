package main

import (
	"encoding/json"
	"os"
	"path/filepath"
)

type PersistentState struct {
	CurrentTerm int
	VotedFor    string
	Entry       []Entry
	LeaderID    string
}

func (rf *RaftNode) persist() error {
	state := PersistentState{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		Entry:       rf.log,
		LeaderID:    rf.leaderID,
	}

	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	fileName, err := rf.getPersistentFileName()
	if err != nil {
		return err
	}

	return os.WriteFile(fileName, data, 0644)
}

func (rf *RaftNode) restore() error {
	fileName, err := rf.getPersistentFileName()
	if err != nil {
		return err
	}

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
	rf.log = state.Entry
	rf.leaderID = state.LeaderID

	return nil
}

func (rf *RaftNode) getPersistentFileName() (string, error) {
	folderPath := "disk_store"
	fileName := "raft_state_" + rf.id + ".json"

	destPath := filepath.Join(folderPath, fileName)

	err := os.MkdirAll(folderPath, 0755)
	if err != nil {
		return "", err
	}

	return destPath, nil
}
