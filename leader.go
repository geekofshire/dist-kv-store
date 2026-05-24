package main

import "time"

func (rf *RaftNode) runLeader() {
	results := make(chan AppendEntriesReply)

	go func() {
		for result := range results {
			if result.Term > rf.currentTerm {
				rf.mu.Lock()
				rf.currentTerm = result.Term
				rf.mu.Unlock()
				rf.transitionToFollower()
			}
		}
	}()

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

				prev_log_index := func() int {
					if len(rf.log.entries) == 0 {
						return -1
					}
					return rf.log.entries[len(rf.log.entries)-1].Index
				}()

				prev_log_term := func() int {
					if len(rf.log.entries) == 0 {
						return 0
					}
					return rf.log.entries[len(rf.log.entries)-1].Term
				}()

				args := AppendEntriesArgs{
					LeaderID:     rf.id,
					LeaderTerm:   rf.currentTerm,
					PrevLogIndex: prev_log_index,
					PrevLogTerm:  prev_log_term,
					Entries:      []Entry{},
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
					results <- resp
				}(peer, args)
			}
		}
	}
}
