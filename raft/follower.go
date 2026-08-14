package raft

import "time"

func (n *Node) runFollower() {
	electionTimer := time.NewTimer(n.getElectionTimeout())
	defer electionTimer.Stop()

	for n.State() == Follower {

		select {
		case <-electionTimer.C:
			n.becomeCandidate(n.currentTerm + 1)
			return

		case msg := <-n.inbox:
			switch m := msg.(type) {
			case AppendEntriesRequest:
				if m.term < n.currentTerm ||
					//check for out of bound log idx, then compare log's term
					m.prevLogIndex >= len(n.log) ||
					(m.prevLogIndex >= 0 && n.log[m.prevLogIndex].term != m.prevLogTerm) {
					n.cluster.SendMessage(AppendEntriesResponse{
						followerId: n.id,
						currTerm:   n.currentTerm,
						matchIdx:   m.prevLogIndex + len(m.entries),
						success:    false,
					}, m.leaderId)
					continue
				}

				electionTimer.Reset(n.getElectionTimeout())
				if m.term > n.currentTerm {
					n.leaderId = m.leaderId
					n.becomeFollower(m.term)
				}

				n.log = append(n.log[:m.prevLogIndex+1], m.entries...)
				if m.leaderCommit > n.commitIndex {
					n.commitIndex = min(m.leaderCommit, m.prevLogIndex+len(m.entries))
					n.applyEntries(n.log[n.lastApplied+1 : n.commitIndex+1]...)
					n.lastApplied = n.commitIndex
				}

				n.leaderId = m.leaderId
				n.cluster.SendMessage(AppendEntriesResponse{
					followerId: n.id,
					currTerm:   n.currentTerm,
					matchIdx:   m.prevLogIndex + len(m.entries),
					success:    true,
				}, m.leaderId)

			case RequestVoteRequest:
				if m.term > n.currentTerm {
					n.currentTerm = m.term
					n.votedFor = -1
				}
				if n.votedFor == -1 && n.logUptoDate(m) && m.term >= n.currentTerm {
					n.votedFor = m.candidateId
				}

				if n.votedFor == m.candidateId {
					electionTimer.Reset(n.getElectionTimeout())
					n.cluster.SendMessage(RequestVoteResponse{
						currTerm:    n.currentTerm,
						voteGranted: true,
					}, m.candidateId)
				} else {
					n.cluster.SendMessage(RequestVoteResponse{
						currTerm:    n.currentTerm,
						voteGranted: false,
					}, m.candidateId)
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
