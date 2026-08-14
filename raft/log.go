package raft

import kvstore "raftconsensus/kvstore"

type LogEntry struct {
	reqId int
	term  int
	cmd   kvstore.Command
}

func (n *Node) lastLogIndex() int {
	if len(n.log) <= 0 {
		return -1
	}
	return len(n.log) - 1
}

func (n *Node) lastLogTerm() int {
	if n.lastLogIndex() < 0 {
		return -1
	}
	return n.log[n.lastLogIndex()].term
}

// method for leader to get outbound entries for follower
func (n *Node) getFollowerEntries(followerId int) []LogEntry {
	idx := max(n.nextIndex[followerId], 0)
	if idx > len(n.log) {
		idx = len(n.log)
	}
	return n.log[idx:]
}

// return map[reqId]
func (n *Node) applyEntries(entries ...LogEntry) (results map[int]kvstore.Result) {
	results = make(map[int]kvstore.Result, len(entries))
	for _, entry := range entries {
		result := n.stateMachine.Exec(entry.cmd)
		results[entry.reqId] = result
	}
	return results
}
