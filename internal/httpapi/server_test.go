package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/geekofshire/dist-kv-store/internal/raft"
)

func createServerInstance() *Server {
	mt := raft.NewMockTransport()

	node1 := raft.NewRaftNode("A", []string{"A", "B", "C"}, mt)
	node2 := raft.NewRaftNode("B", []string{"A", "B", "C"}, mt)
	node3 := raft.NewRaftNode("C", []string{"A", "B", "C"}, mt)

	mt.AppendNodes("A", node1)
	mt.AppendNodes("B", node2)
	mt.AppendNodes("C", node3)

	node1.ForceLeader()

	server := NewServer(mt)

	return server
}

func TestGetHandler(t *testing.T) {
	server := createServerInstance()
	node, err := server.mt.LeaderNode()
	if err != nil {
		t.Fatalf("leader node not working")
	}
	node.SetLocal("name", "alice")

	mux := http.NewServeMux()
	server.Routes(mux)

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
	server := createServerInstance()

	mux := http.NewServeMux()
	server.Routes(mux)

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
	server := createServerInstance()

	mux := http.NewServeMux()
	server.Routes(mux)

	node, err := server.mt.LeaderNode()
	if err != nil {
		t.Fatalf("leader node not working")
	}
	node.SetLocal("name", "alice")

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
