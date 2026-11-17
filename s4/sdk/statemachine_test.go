package sdk

import "testing"

func TestStateMachineHappyPath(t *testing.T) {
	sm := NewStateMachine()
	steps := []ConnState{
		StateConnecting, StateConnected, StateAuthenticating, StateReady,
		StateDisconnected, StateReconnecting, StateConnecting, StateConnected,
		StateAuthenticating, StateReady, StateDisconnected, StateClosed,
	}
	for _, to := range steps {
		if err := sm.Transition(to); err != nil {
			t.Fatalf("transition %s -> %s: %v", sm.State(), to, err)
		}
	}
	if sm.State() != StateClosed {
		t.Fatalf("state=%s", sm.State())
	}
}

func TestStateMachineRejectsIllegalTransitions(t *testing.T) {
	cases := []struct{ from, to ConnState }{
		{StateIdle, StateReady},
		{StateIdle, StateConnected},
		{StateReady, StateConnected},
		{StateClosed, StateConnecting},
		{StateConnecting, StateReady},
		{StateDisconnected, StateReady},
	}
	for _, tc := range cases {
		sm := NewStateMachine()
		sm.ForceTransition(tc.from)
		if err := sm.Transition(tc.to); err == nil {
			t.Fatalf("%s -> %s should be rejected", tc.from, tc.to)
		}
		if sm.State() != tc.from {
			t.Fatalf("rejected transition changed state to %s", sm.State())
		}
	}
	if canTransition(StateReady, StateClosed) != true {
		t.Fatal("READY -> CLOSED must be allowed")
	}
}
