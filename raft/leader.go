package raft

import (
	kvstore "raftconsensus/kvstore"
	"sort"
	"time"
)

func (n *Node) runLeader() {
	heartbeatTicker := time.NewTicker(time.Duration(BroadcastTime) * time.Millisecond)
	defer heartbeatTicker.Stop()

	for n.State() == Leader {
		select {
		case <-heartbeatTicker.C:
			for id := range n.cluster.config.NodeCount {
				if id != n.id {
					prevLogIdx := n.nextIndex[id] - 1
					prevLogTerm := -1
					if prevLogIdx >= 0 && len(n.log) > prevLogIdx {
						prevLogTerm = n.log[prevLogIdx].term
					}
					go n.cluster.SendMessage(AppendEntriesRequest{
						term:         n.currentTerm,
						leaderId:     n.id,
						prevLogIndex: prevLogIdx,
						prevLogTerm:  prevLogTerm,
						entries:      n.getFollowerEntries(id),
						leaderCommit: n.commitIndex,
					}, id)
				}
			}

		case msg := <-n.inbox:
			switch m := msg.(type) {
			case AppendEntriesRequest:
				if m.term > n.currentTerm {
					n.becomeFollower(m.term)
					return
				}
				go n.cluster.SendMessage(AppendEntriesResponse{
					followerId: n.id,
					currTerm:   n.currentTerm,
					success:    false,
				}, m.leaderId)
			case AppendEntriesResponse:
				if m.currTerm > n.currentTerm {
					n.becomeFollower(m.currTerm)
					return
				}

				if m.success {
					n.matchIndex[m.followerId] = m.matchIdx
					n.nextIndex[m.followerId] = m.matchIdx + 1
				} else if n.nextIndex[m.followerId] > 0 {
					n.nextIndex[m.followerId]--
				}

				// update commitIndex
				n.updateCommitIdx()

			case RequestVoteRequest:
				if m.term > n.currentTerm {
					n.becomeFollower(m.term)
					n.inbox <- m
					return
				}
				go n.cluster.SendMessage(RequestVoteResponse{
					currTerm:    n.currentTerm,
					voteGranted: false,
				}, m.candidateId)
			case ClientRequest:
				log := LogEntry{
					reqId: m.id,
					cmd:   m.cmd,
					term:  n.currentTerm,
				}
				n.log = append(n.log, log)
				n.cluster.pending[m.id] = len(n.log) - 1
				n.updateCommitIdx()
			}
		}
	}
}

func (n *Node) updateCommitIdx() {
	match := make([]int, n.cluster.config.NodeCount)
	copy(match, n.matchIndex)
	match[n.id] = len(n.log) - 1
	sort.Sort(sort.Reverse(sort.IntSlice(match)))

	majorityCommit := match[n.cluster.config.NodeCount/2]
	if majorityCommit > n.commitIndex && n.log[majorityCommit].term == n.currentTerm {
		n.commitIndex = majorityCommit
		results := n.applyEntries(n.log[n.lastApplied+1 : n.commitIndex+1]...)
		n.lastApplied = n.commitIndex

		n.respondCommitted(results)
	}
}

func (n *Node) respondCommitted(results map[int]kvstore.Result) {
	for id, logIdx := range n.cluster.pending {
		if logIdx <= n.commitIndex {
			resp := ClientResponse{
				id:      id,
				success: false,
			}
			if val, ok := results[id]; ok {
				resp.result = val.Val
				resp.found = val.Found
				resp.success = true
			}
			n.cluster.SendMessage(resp, -1)
			delete(n.cluster.pending, id)
		}
	}
}
