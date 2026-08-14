package kvstore

func New() *StateMachine {
	return &StateMachine{
		store: make(map[Key]any),
	}
}

func (sm *StateMachine) Exec(cmd Command) (result Result) {
	switch cmd.op {
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
