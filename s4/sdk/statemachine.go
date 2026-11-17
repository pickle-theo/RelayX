package sdk

import (
	"fmt"
	"sync"
)

// transitions 是冻结的连接状态转换表（LLD §4.1）。
// 未列出的转换一律拒绝——状态机不允许"跳跃式"合法状态。
var transitions = map[ConnState][]ConnState{
	StateIdle:           {StateConnecting, StateClosed},
	StateConnecting:     {StateConnected, StateDisconnected, StateClosed},
	StateConnected:      {StateAuthenticating, StateDisconnected, StateClosed},
	StateAuthenticating: {StateReady, StateDisconnected, StateClosed},
	StateReady:          {StateDisconnected, StateClosed},
	StateDisconnected:   {StateReconnecting, StateClosed},
	StateReconnecting:   {StateConnecting, StateConnected, StateClosed},
	StateClosed:         {},
}

func canTransition(from, to ConnState) bool {
	for _, allowed := range transitions[from] {
		if allowed == to {
			return true
		}
	}
	return false
}

// StateMachine 是并发安全的连接状态机。
type StateMachine struct {
	mu    sync.RWMutex
	state ConnState
}

func NewStateMachine() *StateMachine {
	return &StateMachine{state: StateIdle}
}

func (s *StateMachine) State() ConnState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

// Transition 执行一次状态迁移；非法迁移返回错误且不改变状态。
func (s *StateMachine) Transition(to ConnState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == to {
		return nil
	}
	if !canTransition(s.state, to) {
		return fmt.Errorf("sdk: illegal state transition %s -> %s", s.state, to)
	}
	s.state = to
	return nil
}

// ForceTransition 用于关闭路径：任何非终态都可直接进入 CLOSED。
func (s *StateMachine) ForceTransition(to ConnState) {
	s.mu.Lock()
	s.state = to
	s.mu.Unlock()
}
