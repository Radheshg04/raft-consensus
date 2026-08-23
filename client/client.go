package client

import (
	"context"
	"raftconsensus/kvstore"
	"raftconsensus/raft"
)

type Client struct {
	cluster *raft.Cluster
}

func New(c *raft.Cluster) *Client {
	return &Client{cluster: c}
}

func (c *Client) Get(ctx context.Context, key kvstore.Key) (*kvstore.Result, error) {
	return performOp(ctx, c.cluster, key, nil, kvstore.GET)
}

func (c *Client) Put(ctx context.Context, key kvstore.Key, value kvstore.Value) (*kvstore.Result, error) {
	return performOp(ctx, c.cluster, key, value, kvstore.PUT)
}

func (c *Client) Delete(ctx context.Context, key kvstore.Key) (*kvstore.Result, error) {
	return performOp(ctx, c.cluster, key, nil, kvstore.DELETE)
}

func performOp(ctx context.Context, c *raft.Cluster, key kvstore.Key, value kvstore.Value, op kvstore.Operation) (*kvstore.Result, error) {
	cmd := kvstore.Command{
		Op:    op,
		Key:   key,
		Value: value,
	}

	resultCh, errCh := c.Submit(ctx, &cmd)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()

	case result := <-resultCh:
		return &result, nil

	case err := <-errCh:
		return nil, err
	}
}
