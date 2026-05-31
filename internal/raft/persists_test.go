package raft

import "testing"

func TestPersistRestoreRoundTrip(t *testing.T) {
	mt := NewMockTransport()
	dataDir := t.TempDir()

	node := NewRaftNode("node1", []string{"node1"}, mt)
	node.SetDataDir(dataDir)

	node.mu.Lock()
	node.currentTerm = 7
	node.votedFor = "node2"
	node.log = append(node.log, Entry{
		Cmd:   Set,
		Key:   "x",
		Value: "1",
		Index: 0,
		Term:  7,
	})
	if err := node.persistLocked(); err != nil {
		node.mu.Unlock()
		t.Fatal(err)
	}
	node.mu.Unlock()

	restored := NewRaftNode("node1", []string{"node1"}, mt)
	restored.SetDataDir(dataDir)

	if err := restored.restore(); err != nil {
		t.Fatal(err)
	}

	restored.mu.RLock()
	defer restored.mu.RUnlock()

	if restored.currentTerm != 7 {
		t.Fatalf("got term %d want %d", restored.currentTerm, 7)
	}

	if restored.votedFor != "node2" {
		t.Fatalf("got vote %q want %q", restored.votedFor, "node2")
	}

	if len(restored.log) != 1 {
		t.Fatalf("got log len %d want %d", len(restored.log), 1)
	}

	if restored.log[0].Key != "x" || restored.log[0].Value != "1" {
		t.Fatalf("unexpected restored log entry: %+v", restored.log[0])
	}
}

func TestRestoreDoesNotPersistVolatileState(t *testing.T) {
	mt := NewMockTransport()
	dataDir := t.TempDir()

	node := NewRaftNode("node1", []string{"node1", "node2"}, mt)
	node.SetDataDir(dataDir)

	node.mu.Lock()
	node.currentTerm = 3
	node.votedFor = "node2"
	node.leaderID = "node2"
	node.commitIndex = 4
	node.lastApplied = 4
	node.nextIndex["node2"] = 9
	node.matchIndex["node2"] = 8
	if err := node.persistLocked(); err != nil {
		node.mu.Unlock()
		t.Fatal(err)
	}
	node.mu.Unlock()

	restored := NewRaftNode("node1", []string{"node1", "node2"}, mt)
	restored.SetDataDir(dataDir)

	if err := restored.restore(); err != nil {
		t.Fatal(err)
	}

	restored.mu.RLock()
	defer restored.mu.RUnlock()

	if restored.leaderID != "" {
		t.Fatalf("leaderID should not be restored, got %q", restored.leaderID)
	}

	if restored.commitIndex != -1 {
		t.Fatalf("commitIndex should remain default, got %d", restored.commitIndex)
	}

	if restored.lastApplied != -1 {
		t.Fatalf("lastApplied should remain default, got %d", restored.lastApplied)
	}

	if len(restored.nextIndex) != 0 {
		t.Fatalf("nextIndex should remain empty, got %+v", restored.nextIndex)
	}

	if len(restored.matchIndex) != 0 {
		t.Fatalf("matchIndex should remain empty, got %+v", restored.matchIndex)
	}
}
