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
}

type LogEntry struct {
	mu   sync.Mutex
	cond *sync.Cond

	entries []Entry
	applied int
}

func NewLogEntry() *LogEntry {
	l := &LogEntry{
		entries: make([]Entry, 0),
	}
	l.cond = sync.NewCond(&l.mu)

	return l
}

func (l *LogEntry) Append(cmd CommandType, key, value string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	new_entry := Entry{Cmd: cmd, Key: key, Value: value, Index: len(l.entries)}
	l.entries = append(l.entries, new_entry)
	l.cond.Signal()
}

func (l *LogEntry) ApplyLoop() {
	for {
		l.mu.Lock()

		for l.applied >= len(l.entries) {
			l.cond.Wait()
		}

		unapplied_entries := make([]Entry, len(l.entries[l.applied:]))
		copy(unapplied_entries, l.entries[l.applied:])
		start := l.applied
		l.mu.Unlock()

		for i, entry := range unapplied_entries {
			store.ApplyLog(entry)

			l.mu.Lock()
			l.applied = l.applied + start + i
			l.mu.Unlock()
		}
	}
}
