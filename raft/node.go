package raft

import (
	"context"
	"fmt"
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

	// track for client requests
	leaderId int
	mu       *sync.RWMutex
}

func (c *Cluster) Submit(ctx context.Context, cmd *kvstore.Command) (<-chan kvstore.Result, error) {
	leader := c.WaitForElectLeader()
	if leader == nil {
		return nil, fmt.Errorf("No leader")
	}
	id := c.nextRequestId()
	resultCh := make(chan kvstore.Result, 1)

	leader.mu.Lock()
	defer leader.mu.Unlock()

	log := LogEntry{
		reqId: id,
		cmd:   *cmd,
		term:  leader.currentTerm,
	}
	leader.log = append(leader.log, log)

	c.pending[id] = pendingRequest{
		logIndex: len(leader.log) - 1,
		resultCh: resultCh,
	}

	go func() {
		<-ctx.Done()
		c.CancelReq(id)
	}()

	return resultCh, nil
}

func (c *Cluster) nextRequestId() uint {
	c.reqSeq++
	return c.reqSeq
}

func (c *Cluster) getLeader() (*Node, error) {
	for _, node := range c.Nodes {
		if node.State() == Leader {
			return node, nil
		}
	}
	return nil, fmt.Errorf("No leader")
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
			log.Printf("Node %d is down.", n.id)
		}
	}()
	for {
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
