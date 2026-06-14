package raftgrpc

import (
	"github.com/geekofshire/dist-kv-store/internal/raft"
	raftv1 "github.com/geekofshire/dist-kv-store/internal/raftpb/raftv1"
)

func commandToProto(cmd raft.CommandType) raftv1.CommandType {
	switch cmd {
	case raft.Set:
		return raftv1.CommandType_SET
	case raft.Delete:
		return raftv1.CommandType_DELETE
	default:
		return raftv1.CommandType_SET
	}
}

func commandFromProto(cmd raftv1.CommandType) raft.CommandType {
	switch cmd {
	case raftv1.CommandType_SET:
		return raft.Set
	case raftv1.CommandType_DELETE:
		return raft.Delete
	default:
		return raft.Set
	}
}

func entryToProto(entry raft.Entry) *raftv1.Entry {
	return &raftv1.Entry{
		Cmd:   commandToProto(entry.Cmd),
		Key:   entry.Key,
		Value: entry.Value,
		Index: int32(entry.Index),
		Term:  int32(entry.Term),
	}
}

func entryFromProto(entry *raftv1.Entry) raft.Entry {
	if entry == nil {
		return raft.Entry{}
	}

	return raft.Entry{
		Cmd:   commandFromProto(entry.GetCmd()),
		Key:   entry.GetKey(),
		Value: entry.GetValue(),
		Index: int(entry.GetIndex()),
		Term:  int(entry.GetTerm()),
	}
}

func entriesToProto(entries []raft.Entry) []*raftv1.Entry {
	out := make([]*raftv1.Entry, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entryToProto(entry))
	}
	return out
}

func entriesFromProto(entries []*raftv1.Entry) []raft.Entry {
	out := make([]raft.Entry, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entryFromProto(entry))
	}
	return out
}
