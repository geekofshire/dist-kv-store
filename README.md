# dist-kv-store

A small distributed key-value store implemented in Go using the Raft consensus algorithm.

This project is built as a learning-focused Raft implementation: the core consensus logic is kept separate from networking, tests use an in-memory transport, and real nodes communicate over gRPC.

## Current Features

- Raft roles: follower, candidate, leader
- Leader election with randomized election timeouts
- RequestVote and AppendEntries RPCs
- Log replication from leader to followers
- Commit and apply loop for updating the local key-value store
- Persistent Raft state for term, vote, and log
- In-memory mock transport for unit/integration tests
- gRPC transport for running real multi-node clusters
- HTTP API for client reads and writes

## Project Layout

```text
cmd/kvstore/              runnable node process
internal/raft/            core Raft implementation
internal/raftgrpc/        gRPC transport and server adapter
internal/raftpb/raftv1/   generated protobuf code
internal/httpapi/         HTTP key-value API
internal/store/           local key-value store
proto/raft/v1/            Raft protobuf definitions
```

## Project Setup

```bash
git clone <repo-url>
cd dist-kv-store
go mod download
go test ./...
```

The generated protobuf files are checked in, so a normal setup does not require `protoc`. If the proto definitions change, regenerate the files before running the app.

## Local Testing Modes

The project supports two ways to exercise Raft locally:

- **In-memory Raft cluster:** used by tests through `MockTransport`. This runs multiple Raft nodes in one Go process without opening network ports, which makes elections, replication, stale terms, and commit behavior easier to test deterministically.
- **gRPC Raft cluster:** used by the runnable server. Each process owns one `RaftNode`, exposes Raft RPCs over gRPC, and exposes client operations over HTTP.

Run the in-memory test suite with:

```bash
go test ./internal/raft
```

## Run Tests

```bash
go test ./...
go test -race ./...
```

## Run A Local 3-Node Cluster

Start each node in a separate terminal:

```bash
go run ./cmd/kvstore -id A -http-addr :8081 -raft-addr localhost:9001 -peers A=localhost:9001,B=localhost:9002,C=localhost:9003
go run ./cmd/kvstore -id B -http-addr :8082 -raft-addr localhost:9002 -peers A=localhost:9001,B=localhost:9002,C=localhost:9003
go run ./cmd/kvstore -id C -http-addr :8083 -raft-addr localhost:9003 -peers A=localhost:9001,B=localhost:9002,C=localhost:9003
```

## Use The HTTP API

Only the leader accepts writes. Followers currently return `503 not leader`.

```bash
curl -i -X POST localhost:8081/set \
  -H 'Content-Type: application/json' \
  -d '{"key":"name","value":"alice"}'

curl localhost:8081/get/name
curl localhost:8082/get/name
curl localhost:8083/get/name

curl -i -X DELETE localhost:8081/delete/name
```

If the write returns `503`, try the same request against the other HTTP ports to find the current leader.

## Persistence

Each node writes Raft state under `disk_store/<node-id>/` by default. Use `-data-dir` to choose a different location.

```bash
go run ./cmd/kvstore -id A -data-dir ./data ...
```

## Protobuf

Raft RPC definitions live in `proto/raft/v1/raft.proto`. Generated Go files are checked in under `internal/raftpb/raftv1/`.

## Next Improvements

- Add `GET /status` for node ID, role, leader ID, term, commit index, and applied index
- Return leader hints from followers so clients can redirect writes
- Add real gRPC integration tests with multiple listeners
- Improve shutdown handling for Raft loops and servers
- Add snapshots once the replicated log grows large
