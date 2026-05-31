package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/geekofshire/dist-kv-store/internal/httpapi"
	"github.com/geekofshire/dist-kv-store/internal/raft"
)

func main() {
	mt := raft.NewMockTransport()

	node1 := raft.NewRaftNode("A", []string{"A", "B", "C"}, mt)
	node2 := raft.NewRaftNode("B", []string{"A", "B", "C"}, mt)
	node3 := raft.NewRaftNode("C", []string{"A", "B", "C"}, mt)

	mt.AppendNodes("A", node1)
	mt.AppendNodes("B", node2)
	mt.AppendNodes("C", node3)

	mt.StartAll()

	mux := http.ServeMux{}
	handler := httpapi.NewServer(mt)

	handler.Routes(&mux)
	srv := &http.Server{
		Addr:         ":8081",
		Handler:      &mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	fmt.Println("listening on :8081")
	srv.ListenAndServe()
}
