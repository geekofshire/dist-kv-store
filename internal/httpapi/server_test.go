package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/geekofshire/dist-kv-store/internal/raft"
)

type testServer struct {
	server *Server
	node   *raft.RaftNode
}

func createServerInstance(t *testing.T) testServer {
	t.Helper()

	mt := raft.NewMockTransport()

	node1 := raft.NewRaftNode("A", []string{"A", "B", "C"}, mt)
	node2 := raft.NewRaftNode("B", []string{"A", "B", "C"}, mt)
	node3 := raft.NewRaftNode("C", []string{"A", "B", "C"}, mt)

	mt.AppendNodes("A", node1)
	mt.AppendNodes("B", node2)
	mt.AppendNodes("C", node3)

	node1.ForceLeader()

	return testServer{
		server: NewServer(node1),
		node:   node1,
	}
}

func TestGetHandler(t *testing.T) {
	fixture := createServerInstance(t)
	fixture.node.SetLocal("name", "alice")

	mux := http.NewServeMux()
	fixture.server.Routes(mux)

	req := httptest.NewRequest(
		http.MethodGet,
		"/get/name",
		nil,
	)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf(
			"expected status 200, got %d",
			rec.Code,
		)
	}
}

func TestSetHandler(t *testing.T) {
	fixture := createServerInstance(t)

	mux := http.NewServeMux()
	fixture.server.Routes(mux)

	body := strings.NewReader(`{"key": "name", "value": "alice"}`)

	req := httptest.NewRequest(
		http.MethodPost,
		"/set",
		body,
	)
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d", rec.Code)
	}
}

func TestDeleteHandler(t *testing.T) {
	fixture := createServerInstance(t)

	mux := http.NewServeMux()
	fixture.server.Routes(mux)
	fixture.node.SetLocal("name", "alice")

	req := httptest.NewRequest(
		http.MethodDelete,
		"/delete/name",
		nil,
	)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected %d got %d", http.StatusNoContent, rec.Code)
	}
}

func TestSetHandlerReturnsLeaderIDWhenNodeIsNotLeader(t *testing.T) {
	mt := raft.NewMockTransport()

	leader := raft.NewRaftNode("A", []string{"A", "B", "C"}, mt)
	follower := raft.NewRaftNode("B", []string{"A", "B", "C"}, mt)

	mt.AppendNodes("A", leader)
	mt.AppendNodes("B", follower)

	leader.ForceLeader()
	follower.AppendEntries("A", 1, -1, 0, nil, -1)

	server := NewServer(follower)
	mux := http.NewServeMux()
	server.Routes(mux)

	body := strings.NewReader(`{"key": "name", "value": "alice"}`)
	req := httptest.NewRequest(http.MethodPost, "/set", body)
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status %d, got %d", http.StatusServiceUnavailable, rec.Code)
	}

	var response struct {
		Error    string `json:"error"`
		LeaderID string `json:"leader_id"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if response.Error != "not leader" {
		t.Fatalf("expected not leader error, got %q", response.Error)
	}

	if response.LeaderID != "A" {
		t.Fatalf("expected leader A, got %q", response.LeaderID)
	}
}
