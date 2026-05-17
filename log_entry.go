package main

import (
	"sync"
)

type CommandType int

const (
	Set CommandType = iota
	Delete
)

func (c CommandType) String() string {
	switch c {
	case Set:
		return "Set"
	case Delete:
		return "Delete"
	default:
		return "Unknown"
	}
}

type Entry struct {
	Cmd   CommandType
	Key   string
	Value string
	Index int
	Term  int
}

type LogEntry struct {
	mu sync.Mutex

	entries []Entry
	applied int
}

func NewLogEntry() *LogEntry {
	return &LogEntry{
		entries: make([]Entry, 0),
	}
}

func (l *LogEntry) Append(cmd CommandType, key, value string, term, index int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	new_entry := Entry{Cmd: cmd, Key: key, Value: value, Index: index, Term: term}
	l.entries = append(l.entries, new_entry)
}
