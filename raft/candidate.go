package raft

import "time"

func (n *Node) runCandidate() {
	electionTimer := time.NewTimer(n.getElectionTimeout())
	defer electionTimer.Stop()

	voteCount := n.startElection()
	electionTimer.Reset(n.getElectionTimeout())

	majority := (n.cluster.config.NodeCount / 2) + 1

	for n.State() == Candidate {
		select {
		case <-electionTimer.C:
			n.becomeCandidate(n.currentTerm + 1)
			return

		case msg := <-n.inbox:
			switch m := msg.(type) {
			case AppendEntriesRequest:
				if m.term >= n.currentTerm {
					n.leaderId = m.leaderId
					n.becomeFollower(m.term)
					return
				}
			case RequestVoteRequest:
				if m.term > n.currentTerm {
					n.becomeFollower(m.term)
					return
				}
				n.cluster.SendMessage(RequestVoteResponse{
					currTerm:    n.currentTerm,
					voteGranted: false,
				}, m.candidateId)
			case RequestVoteResponse:
				if m.currTerm > n.currentTerm {
					n.becomeFollower(m.currTerm)
					return
				}
				if m.voteGranted && m.currTerm == n.currentTerm {
					voteCount++

					if voteCount >= majority {
						n.becomeLeader(n.currentTerm)
						return
					}
				}
			case ClientRequest:
				n.cluster.SendMessage(ClientResponse{
					leaderId: n.leaderId,
					success:  false,
				}, -1)
			}
		}
	}
}
