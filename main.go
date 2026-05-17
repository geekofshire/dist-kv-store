package main

import (
	"fmt"
	"net/http"
	"time"
)

func main() {
	mux := http.ServeMux{}
	handler := &Server{
		store:    NewStore(),
		logEntry: NewLogEntry(),
	}

	handler.routes(&mux)
	srv := &http.Server{
		Addr:         ":8081",
		Handler:      &mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// go handler.ApplyLoop()

	fmt.Println("listening on :8081")
	srv.ListenAndServe()
}
