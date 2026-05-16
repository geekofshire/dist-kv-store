package main

import (
	"fmt"
	"net/http"
	"time"
)

var (
	store = NewStore()
	log_entry = NewLogEntry()
)

func main() {
	mux := http.ServeMux{}
	handler := &Server{}

	handler.routes(&mux)
	srv := &http.Server{
		Addr:         ":8081",
		Handler:      &mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	fmt.Println("listening on :8081")
	srv.ListenAndServe()
}
