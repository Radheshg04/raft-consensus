package raft

import "raftconsensus/kvstore"

type RPC interface {
	isRPC()
}

type AppendEntriesRequest struct {
	term         int
	leaderId     int
	prevLogIndex int
	prevLogTerm  int
	entries      []LogEntry
	leaderCommit int
}

func (AppendEntriesRequest) isRPC() {}

type AppendEntriesResponse struct {
	followerId int
	currTerm   int
	matchIdx   int
	success    bool
}

func (AppendEntriesResponse) isRPC() {}

type RequestVoteRequest struct {
	term         int
	candidateId  int
	lastLogIndex int
	lastLogTerm  int
}

func (RequestVoteRequest) isRPC() {}

type RequestVoteResponse struct {
	currTerm    int
	voteGranted bool
}

func (RequestVoteResponse) isRPC() {}

type ClientRequest struct {
	id  int
	cmd kvstore.Command
}

func (ClientRequest) isRPC() {}

type ClientResponse struct {
	id       int
	leaderId int
	result   kvstore.Value
	found    bool
	success  bool
}

func (ClientResponse) isRPC() {}
