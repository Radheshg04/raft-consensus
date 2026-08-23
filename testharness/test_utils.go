package testharness

import (
	"raftconsensus/kvstore"
	"raftconsensus/raft"
	"testing"
	"time"
)

func waitForStateReplicate(t *testing.T, cluster *raft.Cluster, leaderSm *kvstore.StateMachine) {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		allReplicated := true

		for _, node := range cluster.Nodes {
			if !node.StateMachine.Equal(leaderSm) {
				allReplicated = false
				break
			}
		}

		if allReplicated {
			return
		}

		time.Sleep(raft.BroadcastTime * 10 * time.Millisecond)
	}
	t.Fatal("state machines did not converge within timeout")
}

func WaitForElectLeader(c *raft.Cluster) *raft.Node {
	deadline := time.NewTimer(time.Duration(raft.BaseElectionTime*10) * time.Millisecond)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline.C:
			return nil // no leader in time
		case <-ticker.C:
			for nodeId := range c.Network {
				if c.Nodes[nodeId].State() == raft.Leader {
					return c.Nodes[nodeId]
				}
			}
		}
	}
}
