package raft

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (n *Node) becomeFollower(term int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.votedFor = -1
	n.currentTerm = term
	n.state = Follower
}

func (n *Node) becomeCandidate(term int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.votedFor = -1
	n.currentTerm = term
	n.state = Candidate
}

func (n *Node) becomeLeader(term int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.votedFor = -1
	n.currentTerm = term
	n.state = Leader

	for i := range n.cluster.config.NodeCount {
		n.nextIndex[i] = len(n.log)
		n.matchIndex[i] = -1
	}
}
