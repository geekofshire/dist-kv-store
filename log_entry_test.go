package main

import (
	"log"
	"testing"
)

func TestAppend(t *testing.T) {
	log_entry := NewLogEntry()

	log_entry.Append(Set, "name", "alice")

	if log_entry.applied != 0 {
		log.Fatalf("expected index 0 got %d", log_entry.applied)
	}
}
