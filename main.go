package main

import (
	"fmt"
	"net/http"
	"time"
)

func main() {
	mt := NewMockTransport()

	node1 := NewRaftNode("A", []string{"A", "B", "C"}, mt)
	node2 := NewRaftNode("B", []string{"A", "B", "C"}, mt)
	node3 := NewRaftNode("C", []string{"A", "B", "C"}, mt)

	mt.AppendNodes("A", node1)
	mt.AppendNodes("B", node2)
	mt.AppendNodes("C", node3)

	mux := http.ServeMux{}
	handler := &Server{
		mt: mt,
	}

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
