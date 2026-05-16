package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGetHandler(t *testing.T) {
	store := NewStore()
	store.Set("name", "alice")

	server := &Server{
		store: NewStore(),
		log_entry: NewLogEntry(),
	}

	mux := http.NewServeMux()
	server.routes(mux)

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
	server := &Server{}

	mux := http.NewServeMux()
	server.routes(mux)

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
