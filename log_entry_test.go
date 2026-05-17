package main

import (
	"testing"
	"time"
)

func TestAppend(t *testing.T) {
	log_entry := NewLogEntry()

	log_entry.Append(Set, "name", "alice", 0, 0)

	if len(log_entry.entries) != 1 {
		t.Fatalf("expected length 1 got %d", len(log_entry.entries))
	}
}

func TestApplyLoop(t *testing.T) {

	server := &Server{
		store:    NewStore(),
		logEntry: NewLogEntry(),
	}

	go server.ApplyLoop()

	server.logEntry.Append(Set, "name", "alice", 0, 0)

	start := time.Now()
	timeout := 1 * time.Second
	interval := 5 * time.Millisecond

	for time.Since(start) < timeout {
		value, ok := server.store.Get("name")
		if ok && value == "alice" {
			return
		}

		time.Sleep(interval)
	}

	t.Fatalf("expected name key to be present")
}
