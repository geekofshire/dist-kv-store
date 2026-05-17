package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

type Server struct {
	store    *Store
	logEntry *LogEntry
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
	s.logEntry.Append(Delete, key, "", 0, 0)
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

	s.logEntry.Append(Set, request.Key, request.Value, 0, 0)
	w.WriteHeader(http.StatusCreated)
}

func (s *Server) routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /get/{key}", s.Get)
	mux.HandleFunc("POST /set", s.Set)
	mux.HandleFunc("PUT /set", s.Set)
	mux.HandleFunc("DELETE /delete/{key}", s.Delete)
}
