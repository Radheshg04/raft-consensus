package kvstore

import (
	"maps"
	"reflect"
	"sync"
)

func New() *StateMachine {
	return &StateMachine{
		mu:    &sync.RWMutex{},
		store: make(map[Key]Value),
	}
}

func (sm *StateMachine) Exec(cmd Command) (result Result) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	switch cmd.Op {
	case GET:
		result.Val, result.Found = sm.store[cmd.Key]

	case PUT:
		sm.store[cmd.Key] = cmd.Value
		result.Val, result.Found = cmd.Value, true

	case DELETE:
		_, result.Found = sm.store[cmd.Key]
		delete(sm.store, cmd.Key)
	}
	return
}

// This function is only used in test harness, Ok to use reflect (slow library)
func (sm *StateMachine) Equal(leaderSm *StateMachine) bool {
	sm.mu.RLock()
	leaderSm.mu.RLock()
	defer sm.mu.RUnlock()
	defer leaderSm.mu.RUnlock()

	return maps.EqualFunc(sm.store, leaderSm.store, func(v1 Value, v2 Value) bool {
		t1, t2 := reflect.TypeOf(v1), reflect.TypeOf(v2)
		if t1 != t2 || !t1.Comparable() {
			return false
		}
		return v1 == v2
	})
}
