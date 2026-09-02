package raft

import (
	"log"
	kvstore "raftconsensus/kvstore"
	"sync"
)

type Node struct {
	currentTerm int
	votedFor    int // initialize with -1 for nil
	log         []LogEntry

	// on all servers
	commitIndex int
	lastApplied int

	// leader specific
	nextIndex  []int
	matchIndex []int

	id           int
	inbox        chan RPC
	state        State
	StateMachine *kvstore.StateMachine
	cluster      *Cluster
	done         chan struct{}

	// track for client requests
	leaderId int
	mu       *sync.RWMutex
}

// getters for safe access
func (n *Node) Id() int {
	return n.id
}

func (n *Node) State() State {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

func (n *Node) VotedFor() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.votedFor
}

func (n *Node) Term() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

func (n *Node) LeaderId() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderId
}

func (n *Node) Run() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Node %d is down. Error: %v", n.id, r)
		}
		n.cluster.wg.Done()
	}()

	for {
		select {
		case <-n.done:
			log.Printf("Node %d shutting down.", n.id)
			return
		default:
		}

		switch n.State() {
		case Follower:
			n.runFollower()
		case Candidate:
			n.runCandidate()
		case Leader:
			n.runLeader()

		}
	}
}
