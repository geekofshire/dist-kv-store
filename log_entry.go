package main

type CommandType int

const (
	Set CommandType = iota
	Delete
)

func (c CommandType) String() string {
	switch c {
	case Set:
		return "Set"
	case Delete:
		return "Delete"
	default:
		return "Unknown"
	}
}

type Entry struct {
	Cmd   CommandType
	Key   string
	Value string
	Index int
	Term  int
}
