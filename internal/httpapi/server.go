package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/geekofshire/dist-kv-store/internal/raft"
)

type Server struct {
	node *raft.RaftNode
}

func NewServer(node *raft.RaftNode) *Server {
	return &Server{node: node}
}

func writeJSON(w http.ResponseWriter, status int, v any) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	return json.NewEncoder(w).Encode(v)
}

func readJSON(r *http.Request, v any) error {
	defer r.Body.Close()

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(v); err != nil {
		return err
	}

	if decoder.More() {
		return errors.New("multiple JSON objects in request body")
	}

	return nil
}

func (s *Server) writeNotLeader(w http.ResponseWriter) {
	type response struct {
		Error    string `json:"error"`
		LeaderID string `json:"leader_id"`
	}

	_, _, leaderID, _, _, _ := s.node.Status()

	if err := writeJSON(w, http.StatusServiceUnavailable, response{
		Error:    "not leader",
		LeaderID: leaderID,
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) Get(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	type response struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	value, ok := s.node.Get(key)
	if !ok {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}

	err := writeJSON(w, http.StatusOK, &response{Key: key, Value: value})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) Delete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	if err := s.node.Propose(raft.Delete, key, ""); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			s.writeNotLeader(w)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) Set(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	if err := readJSON(r, &request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if request.Key == "" || strings.TrimSpace(request.Key) == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	if err := s.node.Propose(raft.Set, request.Key, request.Value); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			s.writeNotLeader(w)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (s *Server) Status(w http.ResponseWriter, r *http.Request) {
	type response struct {
		ID          string `json:"id"`
		Role        string `json:"role"`
		LeaderID    string `json:"leader_id"`
		Term        int    `json:"term"`
		CommitIndex int    `json:"commit_index"`
		LastApplied int    `json:"last_applied"`
	}

	id, role, leaderID, term, commitIndex, lastApplied := s.node.Status()

	err := writeJSON(w, http.StatusOK, response{
		ID:          id,
		Role:        string(role),
		LeaderID:    leaderID,
		Term:        term,
		CommitIndex: commitIndex,
		LastApplied: lastApplied,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) Routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /get/{key}", s.Get)
	mux.HandleFunc("POST /set", s.Set)
	mux.HandleFunc("PUT /set", s.Set)
	mux.HandleFunc("DELETE /delete/{key}", s.Delete)
	mux.HandleFunc("GET /status", s.Status)
}
