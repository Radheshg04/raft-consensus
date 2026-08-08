package raft

import (
	"math/rand"
	"time"
)

func (n *Node) startElection() (voteCount int) {
	voteCount = 1
	n.votedFor = n.id

	for id := range n.cluster.config.NodeCount {
		if id != n.id {
			go n.cluster.SendMessage(RequestVoteRequest{
				term:         n.currentTerm,
				candidateId:  n.id,
				lastLogIndex: n.lastLogIndex(),
				lastLogTerm:  n.lastLogTerm(),
			}, id)
		}
	}
	return voteCount
}

func (n *Node) getElectionTimeout() time.Duration {
	return time.Duration(BaseElectionTime+rand.Intn(50)) * time.Millisecond
}

func (n *Node) logUptoDate(req RequestVoteRequest) bool {
	prevlogTerm := n.lastLogTerm()
	prevlogIdx := n.lastLogIndex()
	if req.lastLogTerm != prevlogTerm {
		return req.lastLogTerm > prevlogTerm
	}
	return req.lastLogIndex >= prevlogIdx
}
