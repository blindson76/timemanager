package service

import (
	"encoding/json"
	"testing"
	"time"

	"example.com/mystate/executor"
	"github.com/blindson76/gstate"
	"github.com/hashicorp/raft"
)

type stack struct {
	t     *testing.T
	x     *executor.Executor
	ctrls map[string]*executor.Ctrl[State, Event, Status]
}

// newStack builds timesync -> (kafka | mongo) -> services, with a plain
// service standing in for timesync.
func newStack(t *testing.T) *stack {
	t.Helper()
	specs := []Spec{
		{Name: "billing", DependsOn: []string{"kafka", "mongo"}},
		{Name: "audit", DependsOn: []string{"kafka"}},
		{Name: "kafka", DependsOn: []string{"timesync"}},
		{Name: "mongo", DependsOn: []string{"timesync"}},
		{Name: "timesync"},
	}
	s := &stack{t: t, ctrls: map[string]*executor.Ctrl[State, Event, Status]{}}
	var ctrls []executor.Controller
	for _, spec := range specs {
		spec.StartTimeout, spec.RetryDelay = time.Hour, time.Hour // fired by hand below
		c := New(spec)
		s.ctrls[spec.Name] = c
		ctrls = append(ctrls, c)
	}
	x, err := executor.New(ctrls...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(x.Stop)
	s.x = x
	return s
}

func (s *stack) apply(cmd executor.Command) interface{} {
	s.t.Helper()
	data, err := json.Marshal(cmd)
	if err != nil {
		s.t.Fatal(err)
	}
	return s.x.Apply(&raft.Log{Type: raft.LogCommand, Data: data})
}

func (s *stack) send(name string, e Event, args any) {
	s.t.Helper()
	cmd := executor.Command{Controller: name, Event: string(e)}
	if args != nil {
		raw, err := json.Marshal(args)
		if err != nil {
			s.t.Fatal(err)
		}
		cmd.Args = raw
	}
	s.apply(cmd)
}

// fire applies a delayed transition as the leader would propose it.
func (s *stack) fire(name string, source, target State, epoch uint64) {
	s.t.Helper()
	raw, err := json.Marshal(gstate.InternalTrigger[State]{Kind: gstate.TriggerAfter, Source: source, Target: target, Epoch: epoch})
	if err != nil {
		s.t.Fatal(err)
	}
	if fired := s.apply(executor.Command{Controller: name, Internal: raw}); fired != true {
		s.t.Fatalf("%s: trigger %s -> %s (epoch %d) not applied: %v", name, source, target, epoch, fired)
	}
}

func (s *stack) expect(want map[string]executor.Health) {
	s.t.Helper()
	for name, h := range want {
		if got := s.x.Health(name); got != h {
			s.t.Errorf("%s health = %s, want %s (states %v)", name, got, h, s.x.States(name))
		}
	}
}

func (s *stack) expectState(name string, want State) {
	s.t.Helper()
	states := s.ctrls[name].States()
	if states[len(states)-1] != want {
		s.t.Errorf("%s states = %v, want leaf %s", name, states, want)
	}
}

const (
	pending  = executor.HealthPending
	ready    = executor.HealthReady
	degraded = executor.HealthDegraded
	failed   = executor.HealthFailed
)

func TestStartupOrderFollowsDependencies(t *testing.T) {
	s := newStack(t)
	s.expectState("timesync", StateStarting)
	for _, name := range []string{"kafka", "mongo", "billing", "audit"} {
		s.expectState(name, StateWaitingDeps)
	}

	s.send("timesync", EventStarted, nil)
	s.expect(map[string]executor.Health{"timesync": ready})
	s.expectState("kafka", StateStarting)
	s.expectState("mongo", StateStarting)

	s.send("kafka", EventStarted, nil)
	s.expectState("audit", StateStarting)
	s.expectState("billing", StateWaitingDeps) // still needs mongo

	s.send("mongo", EventStarted, nil)
	s.expectState("billing", StateStarting)

	s.send("audit", EventStarted, nil)
	s.send("billing", EventStarted, nil)
	s.expect(map[string]executor.Health{"timesync": ready, "kafka": ready, "mongo": ready, "billing": ready, "audit": ready})
}

func TestDependencyFailureDegradesWithoutStopping(t *testing.T) {
	s := newStack(t)
	for _, name := range []string{"timesync", "kafka", "mongo", "audit", "billing"} {
		s.send(name, EventStarted, nil)
	}

	s.send("kafka", EventFailed, Failure{Reason: "broker crashed"})
	s.expect(map[string]executor.Health{"kafka": failed, "audit": degraded, "billing": degraded, "mongo": ready})
	s.expectState("billing", StateRunning)
	if got := s.ctrls["kafka"].Data().LastError; got != "broker crashed" {
		t.Errorf("kafka last error = %q", got)
	}

	// Retry: kafka goes back through WaitingDeps (ready) to Starting.
	s.fire("kafka", StateFailed, StateWaitingDeps, 1)
	s.expectState("kafka", StateStarting)
	s.expect(map[string]executor.Health{"kafka": pending, "billing": degraded})

	s.send("kafka", EventStarted, nil)
	s.expect(map[string]executor.Health{"kafka": ready, "audit": ready, "billing": ready})
}

func TestStartTimeoutFailsAndCountsAttempts(t *testing.T) {
	s := newStack(t)
	s.fire("timesync", StateStarting, StateFailed, 1)
	s.expect(map[string]executor.Health{"timesync": failed, "kafka": pending})

	s.fire("timesync", StateFailed, StateWaitingDeps, 1)
	s.expectState("timesync", StateStarting)
	if got := s.ctrls["timesync"].Data().Attempts; got != 2 {
		t.Errorf("attempts = %d, want 2", got)
	}
	s.send("timesync", EventStarted, nil)
	if got := s.ctrls["timesync"].Data().Attempts; got != 0 {
		t.Errorf("attempts after start = %d, want 0", got)
	}
}

func TestDependencyLossBeforeStartKeepsWaiting(t *testing.T) {
	s := newStack(t)
	s.send("timesync", EventStarted, nil)
	s.send("kafka", EventStarted, nil)
	s.send("mongo", EventStarted, nil)
	s.expectState("billing", StateStarting)

	// Losing a dependency during Starting does not abort the start.
	s.send("mongo", EventFailed, Failure{Reason: "oom"})
	s.expectState("billing", StateStarting)

	// But a service that is not yet started waits for recovery.
	s.send("billing", EventFailed, Failure{Reason: "crash"})
	s.fire("billing", StateFailed, StateWaitingDeps, 1)
	s.expectState("billing", StateWaitingDeps)
}
