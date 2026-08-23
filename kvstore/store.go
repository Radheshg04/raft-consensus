package kvstore

import (
	"maps"
	"sync"
)

func New() *StateMachine {
	return &StateMachine{
		mu:    &sync.RWMutex{},
		store: make(map[Key]any),
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

func (sm *StateMachine) Equal(leaderSm *StateMachine) bool {
	sm.mu.RLock()
	leaderSm.mu.RLock()
	defer sm.mu.RUnlock()
	defer leaderSm.mu.RUnlock()

	return maps.Equal(sm.store, leaderSm.store)
}
