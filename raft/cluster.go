package raft

import (
	"log"
	kvstore "raftconsensus/kvstore"
	"sync"
	"time"
)

const (
	DefaultNodeCount     = 5
	DefaultInboxSize     = 16
	DefaultRequestBuffer = 8

	BroadcastTime    = 50  // timer for hearbeat (used by leader)
	BaseElectionTime = 150 // base election timout
)

type Cluster struct {
	startOnce sync.Once
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

type ServerConfig struct {
	NodeCount     int
	NodeInboxSize int
	RequestBuffer int
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

func (c *Cluster) Start() {
	c.startOnce.Do(func() {
		for _, node := range c.Nodes {
			go node.Run()
		}
	})
}

func (c *Cluster) Shutdown() {
	for _, node := range c.Nodes {
		c.Kill(node)
	}
}

func (c *Cluster) Kill(node *Node) {
	c.mu.Lock()
	delete(c.Network, node.id)
	c.mu.Unlock()
	node.inbox <- KillSignal{}
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
		nextIndex:    make([]int, c.config.NodeCount),
		matchIndex:   make([]int, c.config.NodeCount),
		mu:           &sync.RWMutex{},
	}

	return &n
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

func (c *Cluster) AddNode(node *Node) {
	c.Network[node.id] = node.inbox
	c.Nodes[node.id] = node
	node.cluster = c
}

func (c *Cluster) CancelReq(reqId uint) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.pending, reqId)
}

func (c *Cluster) WaitForElectLeader() *Node {
	deadline := time.NewTimer(time.Duration(BaseElectionTime*10) * time.Millisecond)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline.C:
			return nil // no leader in time
		case <-ticker.C:
			for _, node := range c.Nodes {
				if node.State() == Leader {
					return node
				}
			}
		}
	}
}
