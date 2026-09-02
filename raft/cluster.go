package raft

import (
	"context"
	"fmt"
	"log"
	kvstore "raftconsensus/kvstore"
	"sync"
	"time"
)

type Cluster struct {
	startOnce sync.Once
	wg        sync.WaitGroup
	Nodes     []*Node
	reqSeq    uint
	Network   map[int]chan RPC        // map[id]inbox
	pending   map[uint]pendingRequest // map[reqId]logIndex
	config    ServerConfig
	mu        *sync.RWMutex
}

type pendingRequest struct {
	logIndex int
	resultCh chan kvstore.Result
}

func InitializeCluster(config *ServerConfig) *Cluster {
	if config.NodeCount == 0 {
		config.NodeCount = DefaultNodeCount
	}

	if config.NodeInboxSize == 0 {
		config.NodeInboxSize = DefaultInboxSize
	}

	if config.RequestBuffer == 0 {
		config.RequestBuffer = DefaultRequestBuffer
	}

	if config.RequestTimeout == 0 {
		config.RequestTimeout = DefaultRequestTimeout
	}

	c := Cluster{
		config:  *config,
		Nodes:   make([]*Node, config.NodeCount),
		Network: make(map[int]chan RPC, config.NodeCount),
		pending: make(map[uint]pendingRequest, config.RequestBuffer),
		mu:      &sync.RWMutex{},
	}
	nodeCounter := 0
	for nodeCounter < config.NodeCount {
		c.AddNode(c.InitializeNode(nodeCounter))
		nodeCounter++
	}

	return &c
}

func (c *Cluster) InitializeNode(nodeId int) *Node {
	n := Node{
		id:           nodeId,
		votedFor:     -1,
		commitIndex:  -1,
		lastApplied:  -1,
		StateMachine: kvstore.New(),
		cluster:      c,
		inbox:        make(chan RPC, c.config.NodeInboxSize), // initialize buffered channel so that leader does not stall on slow follower
		done:         make(chan struct{}),
		nextIndex:    make([]int, c.config.NodeCount),
		matchIndex:   make([]int, c.config.NodeCount),
		mu:           &sync.RWMutex{},
	}

	return &n
}

func (c *Cluster) AddNode(node *Node) {
	c.Network[node.id] = node.inbox
	c.Nodes[node.id] = node
	node.cluster = c
}

func (c *Cluster) KillNode(node *Node) {
	c.mu.Lock()
	delete(c.Network, node.id)
	c.mu.Unlock()
	close(node.done)
}

func (c *Cluster) Start() {
	c.startOnce.Do(func() {
		c.wg.Add(len(c.Nodes))
		for _, node := range c.Nodes {
			go node.Run()
		}
	})
}

func (c *Cluster) Shutdown() {
	for _, node := range c.Nodes {
		c.KillNode(node)
	}
	c.wg.Wait()
}

func (c *Cluster) Submit(ctx context.Context, cmd *kvstore.Command) (<-chan kvstore.Result, <-chan error) {
	errCh := make(chan error)
	leader, err := c.getLeader()
	if err != nil {
		errCh <- err
		return nil, errCh
	}
	id := c.nextRequestId()
	resultCh := make(chan kvstore.Result, 1)

	leader.mu.Lock()
	log := LogEntry{
		reqId: id,
		cmd:   *cmd,
		term:  leader.currentTerm,
	}
	leader.log = append(leader.log, log)

	c.mu.Lock()
	c.pending[id] = pendingRequest{
		logIndex: len(leader.log) - 1,
		resultCh: resultCh,
	}
	leader.mu.Unlock()
	c.mu.Unlock()

	go func() {
		reqTimer := time.NewTimer(time.Duration(c.config.RequestTimeout) * time.Millisecond)
		defer reqTimer.Stop()
		select {
		case <-reqTimer.C:
			errCh <- fmt.Errorf("req timed out")
			c.cancelReq(id)
		case <-ctx.Done():
			errCh <- fmt.Errorf("ctx was cancelled")
			c.cancelReq(id)
		}
	}()

	return resultCh, errCh
}

func (c *Cluster) cancelReq(reqId uint) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.pending, reqId)
}

func (c *Cluster) nextRequestId() uint {
	c.mu.Lock()
	defer c.mu.Unlock()
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

func (c *Cluster) SendMessage(message RPC, id int) {
	c.mu.RLock()
	nodeInbox, ok := c.Network[id]
	c.mu.RUnlock()

	if !ok {
		log.Printf("raft: dropped message to dead node %d", id)
	}
	select {
	case nodeInbox <- message:
	default:
		log.Printf("raft: dropped message to node %d, inbox full", id)
	}

}
