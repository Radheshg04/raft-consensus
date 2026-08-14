package kvstore

type Key string
type Value interface{}

type Operation int

const (
	GET Operation = iota
	PUT
	DELETE
)

type StateMachine struct {
	store map[Key]any
}
type Command struct {
	op    Operation
	Key   Key
	Value Value
}

type Result struct {
	Val   Value
	Found bool
}
