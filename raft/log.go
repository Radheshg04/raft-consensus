package raft

import (
	"log"
	kvstore "raftconsensus/kvstore"
)

type LogEntry struct {
	reqId uint
	term  int
	cmd   kvstore.Command
}

func (n *Node) getLogTerm(idx int) int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.log[idx].term
}

func (n *Node) getLogLen() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return len(n.log)
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
	n.mu.Lock()
	defer n.mu.Unlock()
	idx := max(n.nextIndex[followerId], 0)
	if idx > len(n.log) {
		idx = len(n.log)
	}
	return n.log[idx:]
}

// return map[reqId]
func (n *Node) applyEntries(entries ...LogEntry) (results map[uint]kvstore.Result) {
	results = make(map[uint]kvstore.Result, len(entries))
	for _, entry := range entries {
		log.Printf("node %d performed op: %d", n.Id(), entry.cmd.Op)
		result := n.StateMachine.Exec(entry.cmd)
		results[entry.reqId] = result
	}
	return results
}
