package raft

const (
	DefaultNodeCount      = 5
	DefaultInboxSize      = 16
	DefaultRequestBuffer  = 8
	DefaultRequestTimeout = 5000

	BroadcastTime    = 50  // timer for hearbeat (used by leader)
	BaseElectionTime = 150 // base election timout
)

type ServerConfig struct {
	NodeCount      int // Number of nodes per cluster
	NodeInboxSize  int // Size of inbox channel for nodes
	RequestBuffer  int // Max number of pending requests at any point
	RequestTimeout int // Timeout for a single request (in milliseconds)
}
