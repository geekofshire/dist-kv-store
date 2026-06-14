package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/geekofshire/dist-kv-store/internal/httpapi"
	"github.com/geekofshire/dist-kv-store/internal/raft"
	"github.com/geekofshire/dist-kv-store/internal/raftgrpc"
	raftv1 "github.com/geekofshire/dist-kv-store/internal/raftpb/raftv1"
	"google.golang.org/grpc"
)

func main() {
	id := flag.String("id", "A", "raft node id")
	httpAddr := flag.String("http-addr", ":8081", "HTTP client API address")
	raftAddr := flag.String("raft-addr", ":9001", "gRPC raft address")
	peers := flag.String("peers", "A=localhost:9001", "comma-separated raft peers, for example A=localhost:9001,B=localhost:9002")
	dataDir := flag.String("data-dir", "disk_store", "directory for raft persistent state")
	rpcTimeout := flag.Duration("rpc-timeout", 200*time.Millisecond, "raft RPC timeout")
	flag.Parse()

	peerAddrs, err := parsePeers(*peers)
	if err != nil {
		log.Fatal(err)
	}
	peerAddrs[*id] = *raftAddr

	remotePeerAddrs := make(map[string]string, len(peerAddrs))
	peerIDs := make([]string, 0, len(peerAddrs))
	for peerID, addr := range peerAddrs {
		peerIDs = append(peerIDs, peerID)
		if peerID != *id {
			remotePeerAddrs[peerID] = addr
		}
	}
	sort.Strings(peerIDs)

	transport, err := raftgrpc.NewTransport(remotePeerAddrs, *rpcTimeout)
	if err != nil {
		log.Fatal(err)
	}
	defer transport.Close()

	node := raft.NewRaftNode(*id, peerIDs, transport)
	node.SetDataDir(filepath.Join(*dataDir, *id))
	if err := node.Restore(); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Fatal(err)
	}

	go node.Run()
	go node.ApplyLoop()

	grpcServer := grpc.NewServer()
	raftv1.RegisterRaftServiceServer(grpcServer, raftgrpc.NewServer(node))

	listener, err := net.Listen("tcp", *raftAddr)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		log.Printf("raft gRPC listening on %s as node %s", *raftAddr, *id)
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatal(err)
		}
	}()

	mux := http.NewServeMux()
	handler := httpapi.NewServer(node)
	handler.Routes(mux)

	httpServer := &http.Server{
		Addr:         *httpAddr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	log.Printf("HTTP API listening on %s as node %s", *httpAddr, *id)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func parsePeers(raw string) (map[string]string, error) {
	peers := make(map[string]string)

	raw = strings.TrimSpace(raw)
	if raw == "" {
		return peers, nil
	}

	for _, peer := range strings.Split(raw, ",") {
		peer = strings.TrimSpace(peer)
		if peer == "" {
			continue
		}

		id, addr, ok := strings.Cut(peer, "=")
		if !ok {
			return nil, fmt.Errorf("peer %q must use id=address format", peer)
		}

		id = strings.TrimSpace(id)
		addr = strings.TrimSpace(addr)
		if id == "" || addr == "" {
			return nil, fmt.Errorf("peer %q must include both id and address", peer)
		}

		peers[id] = addr
	}

	return peers, nil
}
