package raft

import (
	"encoding/json"
	"os"
	"path/filepath"
)

type PersistentState struct {
	CurrentTerm int
	VotedFor    string
	Entry       []Entry
}

func (rf *RaftNode) persist() error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.persistLocked()
}

func (rf *RaftNode) persistLocked() error {
	state := PersistentState{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		Entry:       append([]Entry(nil), rf.log...),
	}

	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	fileName, err := rf.getPersistentFileName()
	if err != nil {
		return err
	}

	tmpFileName := fileName + ".tmp"
	if err := os.WriteFile(tmpFileName, data, 0644); err != nil {
		return err
	}

	return os.Rename(tmpFileName, fileName)
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm = state.CurrentTerm
	rf.votedFor = state.VotedFor
	rf.log = append([]Entry(nil), state.Entry...)

	return nil
}

func (rf *RaftNode) getPersistentFileName() (string, error) {
	folderPath := rf.dataDir
	if folderPath == "" {
		folderPath = "disk_store"
	}

	fileName := "raft_state_" + rf.id + ".json"

	destPath := filepath.Join(folderPath, fileName)

	err := os.MkdirAll(folderPath, 0755)
	if err != nil {
		return "", err
	}

	return destPath, nil
}
