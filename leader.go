package main

import (
	"time"
)

func (rf *RaftNode) runLeader() {
	ticker := time.NewTicker(time.Millisecond * 50)
	defer ticker.Stop()
	for {
		select {
		case <-rf.stopCh:
			return

		case <-ticker.C:
			for _, peer := range rf.peers {
				rf.mu.Lock()

				if rf.role != Leader {
					rf.mu.Unlock()
					break
				}

				next := rf.nextIndex[peer]
				next = min(0, next)
				next = max(next, len(rf.log) - 1)
				
				prevIndex := next - 1

				prevTerm := func() int{
					if prevIndex <= -1 {
						return 0
					}

					if prevIndex >= len(rf.log) {
						return 0
					}

					return rf.log[prevIndex].Term
				}()

				entries := make([]Entry, len(rf.log[next:]))
				copy(entries, rf.log[next:])

				args := AppendEntriesArgs{
					LeaderID:     rf.id,
					LeaderTerm:   rf.currentTerm,
					PrevLogIndex: prevIndex,
					PrevLogTerm:  prevTerm,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}

				rf.mu.Unlock()

				go func(peer string, args AppendEntriesArgs) {
					if peer == rf.id {
						return
					}

					resp, err := rf.transport.AppendEntries(peer, args)
					if err != nil {
						return
					}

					rf.mu.Lock()
					currentTerm := rf.currentTerm

					if resp.Term > currentTerm {
						rf.currentTerm = resp.Term
						rf.mu.Unlock()
						rf.transitionToFollower()
						rf.persist()
						return
					}

					if rf.role != Leader || args.LeaderTerm != currentTerm {
						rf.mu.Unlock()
						return
					}

					if resp.Success {
						rf.matchIndex[peer] = prevIndex + len(args.Entries)
						rf.nextIndex[peer] = rf.matchIndex[peer] + 1
						rf.mu.Unlock()
						go rf.tryAdvanceCommitIndex()
						return
					} else {
						rf.nextIndex[peer] = max(0, rf.nextIndex[peer] - 1)
					}

					rf.mu.Unlock()

				}(peer, args)
			}
		}
	}
}

func (rf *RaftNode) tryAdvanceCommitIndex() {
	rf.mu.Lock()

	lastLogIndex := func() int {
		if len(rf.log) == 0 {
			return -1
		}

		return rf.log[len(rf.log) - 1].Index
	}()

	for n := lastLogIndex; n > rf.commitIndex; n-- {
		if rf.log[n].Term != rf.currentTerm {
			continue
		}

		majority := (len(rf.peers) / 2) + 1
		count := 0
		for id, val := range rf.matchIndex {
			if val >= n || rf.id == id {
				count ++
			}
		}

		if count >= majority {
			rf.commitIndex = n
			rf.notifyCommit()
			break
		}
	}

	rf.mu.Unlock()
}