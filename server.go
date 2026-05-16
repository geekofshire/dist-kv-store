package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

type Server struct {
	store *Store
	log_entry *LogEntry
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

func (s *Server) Get(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	type response struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	value, ok := s.store.Get(key)
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
	s.log_entry.Append(Delete, key, "")
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

	s.log_entry.Append(Set, request.Key, request.Value)
	w.WriteHeader(http.StatusCreated)
}

func (s *Server) ApplyLoop() {
	for {
		l := s.log_entry
		l.mu.Lock()

		for l.applied >= len(l.entries) {
			l.cond.Wait()
		}

		unapplied_entries := make([]Entry, len(l.entries[l.applied:]))
		copy(unapplied_entries, l.entries[l.applied:])
		start := l.applied
		l.mu.Unlock()

		for i, entry := range unapplied_entries {
			s.store.ApplyLog(entry)

			l.mu.Lock()
			l.applied = l.applied + start + i
			l.mu.Unlock()
		}
	}
}

func (s *Server) routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /get/{key}", s.Get)
	mux.HandleFunc("POST /set", s.Set)
	mux.HandleFunc("PUT /set", s.Set)
	mux.HandleFunc("DELETE /delete/{key}", s.Delete)
}
