package kvstore

import "sync"

type Key string
type Value interface{}

type Operation int

const (
	GET Operation = iota
	PUT
	DELETE
)

type StateMachine struct {
	mu    *sync.RWMutex
	store map[Key]any
}
type Command struct {
	Op    Operation
	Key   Key
	Value Value
}

type Result struct {
	Val   Value
	Found bool
}
