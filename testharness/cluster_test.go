package testharness

import (
	"context"
	"raftconsensus/client"
	"raftconsensus/kvstore"
	"raftconsensus/raft"
	"testing"
	"time"
)

func GetTestCluster(t testing.TB) *raft.Cluster {
	globalConfig := &raft.ServerConfig{
		NodeCount:     5,
		RequestBuffer: 10,
	}
	c := raft.InitializeCluster(globalConfig)
	c.Start()
	// t.Cleanup(c.Shutdown)
	return c
}

func TestCluster_ElectLeader(t *testing.T) {
	c := GetTestCluster(t)
	elected := WaitForElectLeader(c)
	if elected == nil {
		t.Fatal("No elected leader in BaseElectionTime*10")
	}
	if elected.State() != raft.Leader {
		t.Fatalf("Wanted Leader, got %v", elected.State())
	}
}

func TestCluster_KillLeader(t *testing.T) {
	c := GetTestCluster(t)

	elected := WaitForElectLeader(c)
	if elected == nil {
		t.Fatalf("No elected leader in BaseElectionTime*10")
	}

	c.KillNode(elected)

	newLeader := WaitForElectLeader(c)
	if newLeader == nil {
		t.Fatalf("No new leader elected after kill")
	}
	if newLeader.Id() == elected.Id() {
		t.Errorf("Expected new leader, got same node %v re-elected", newLeader.Id())
	}
	if newLeader.State() != raft.Leader {
		t.Errorf("Wanted Leader, got %v", newLeader.State())
	}
}

func TestCluster_StateMachineReplication(t *testing.T) {
	cluster := GetTestCluster(t)
	elected := WaitForElectLeader(cluster)
	if elected == nil {
		t.Fatal("no initial leader elected")
	}
	c := client.New(cluster)
	key, value := "test1", 123
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := c.Put(ctx, kvstore.Key(key), value)
	if err != nil {
		t.Fatal(err.Error())
	}
	if !result.Found {
		t.Fatalf("Unsuccessful PUT")
	}
	if result.Val != value {
		t.Fatalf("result val does not match with val after PUT")
	}
	waitForStateReplicate(t, cluster, elected.StateMachine)
	for _, node := range cluster.Nodes {
		result := node.StateMachine.Exec(kvstore.Command{Op: kvstore.GET, Key: kvstore.Key(key)})
		if !result.Found {
			t.Fatalf("node %d: key not found", node.Id())
		}

		if result.Val != value {
			t.Fatalf(
				"node %d: expected %v, got %v",
				node.Id(),
				value,
				result.Val,
			)
		}
	}
}

func TestCluster_StateSurvivesLeaderFailure(t *testing.T) {
	cluster := GetTestCluster(t)
	elected := WaitForElectLeader(cluster)
	if elected == nil {
		t.Fatal("no initial leader elected")
	}
	c := client.New(cluster)
	key, value := "test1", 123
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := c.Put(ctx, kvstore.Key(key), value)
	if err != nil {
		t.Fatal(err.Error())
	}
	if !result.Found {
		t.Fatalf("Unsuccessful PUT")
	}
	if result.Val != value {
		t.Fatalf("result val does not match with val after PUT")
	}
	waitForStateReplicate(t, cluster, elected.StateMachine)

	cluster.KillNode(elected)
	elected = WaitForElectLeader(cluster)
	if elected == nil {
		t.Fatal("No leader elected.")
	}
	for _, node := range cluster.Nodes {
		result := node.StateMachine.Exec(kvstore.Command{Op: kvstore.GET, Key: kvstore.Key(key)})
		if !result.Found {
			t.Fatalf("node %d: key not found", node.Id())
		}

		if result.Val != value {
			t.Fatalf(
				"node %d: expected %v, got %v",
				node.Id(),
				value,
				result.Val,
			)
		}
	}
}
