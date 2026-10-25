package executor

import (
	"encoding/json"
	"fmt"

	"github.com/blindson76/gstate"
)

// Health is a controller's summary of its service, as seen by dependents.
type Health string

const (
	HealthUnknown  Health = "Unknown"
	HealthPending  Health = "Pending"
	HealthReady    Health = "Ready"
	HealthDegraded Health = "Degraded"
	HealthFailed   Health = "Failed"
)

// DependencyChange is the args of the dependency event (see
// [Ctrl.OnDependencyChange]) delivered when a dependency's health changes.
// It is delivered as this typed value, not JSON, on every node.
type DependencyChange struct {
	Name   string `json:"name"`
	Health Health `json:"health"`
}

// DecodeArgs converts event args to T. External event args arrive as
// [json.RawMessage] on every node; values already of type T pass through.
func DecodeArgs[T any](args any) (T, error) {
	var v T
	switch a := args.(type) {
	case T:
		return a, nil
	case json.RawMessage:
		err := json.Unmarshal(a, &v)
		return v, err
	case nil:
		return v, fmt.Errorf("executor: missing args, want %T", v)
	default:
		return v, fmt.Errorf("executor: args are %T, want %T", args, v)
	}
}

// Controller is a gstate machine hosted by an [Executor]. Create one with
// [NewController].
type Controller interface {
	Name() string
	Dependencies() []string

	bind(propose func(Command) error)
	hasDependencyEvent() bool
	health() Health
	states() []string
	apply(event string, args json.RawMessage)
	fireInternal(raw json.RawMessage) (bool, error)
	dependencyChanged(DependencyChange)
	setLeader(bool)
	snapshot() (json.RawMessage, error)
	restore(json.RawMessage) error
	reset()
	stop()
}

// Ctrl hosts one gstate machine. Configure it with the builder methods
// before passing it to [New]; the actor starts when the executor is built.
type Ctrl[S ~string, E ~string, D gstate.Cloner[D]] struct {
	name     string
	machine  *gstate.Machine[S, E, D]
	initial  D
	deps     []string
	depEvent E
	hasDepEv bool
	healthFn func(states []S, d D) Health
	opts     []gstate.Option[S, E, D]

	propose func(Command) error
	actor   *gstate.Actor[S, E, D]
	leader  bool
}

// NewController wraps machine m, starting from initial, under a unique name.
// initial must not share references (maps, slices) with other controllers.
func NewController[S ~string, E ~string, D gstate.Cloner[D]](name string, m *gstate.Machine[S, E, D], initial D) *Ctrl[S, E, D] {
	return &Ctrl[S, E, D]{name: name, machine: m, initial: initial}
}

// DependsOn declares controllers whose health this one needs.
func (c *Ctrl[S, E, D]) DependsOn(names ...string) *Ctrl[S, E, D] {
	c.deps = append(c.deps, names...)
	return c
}

// OnDependencyChange sets the event sent to this machine, with a
// [DependencyChange] as args, whenever a dependency's health changes. It is
// required when DependsOn is used.
func (c *Ctrl[S, E, D]) OnDependencyChange(e E) *Ctrl[S, E, D] {
	c.depEvent, c.hasDepEv = e, true
	return c
}

// WithHealth sets how the controller's health is derived from its active
// states (root to leaf) and data. It must be a pure function. Without it
// the controller always reports [HealthUnknown].
func (c *Ctrl[S, E, D]) WithHealth(fn func(states []S, d D) Health) *Ctrl[S, E, D] {
	c.healthFn = fn
	return c
}

// WithActorOptions adds gstate options, such as observers, to the actor.
func (c *Ctrl[S, E, D]) WithActorOptions(opts ...gstate.Option[S, E, D]) *Ctrl[S, E, D] {
	c.opts = append(c.opts, opts...)
	return c
}

// Name implements [Controller].
func (c *Ctrl[S, E, D]) Name() string { return c.name }

// Dependencies implements [Controller].
func (c *Ctrl[S, E, D]) Dependencies() []string { return c.deps }

// Data returns a copy of the controller's current data.
func (c *Ctrl[S, E, D]) Data() D { return c.actor.Data() }

// States returns the controller's active states, root to leaf.
func (c *Ctrl[S, E, D]) States() []S { return c.actor.States() }

func (c *Ctrl[S, E, D]) actorOptions() []gstate.Option[S, E, D] {
	return append(append([]gstate.Option[S, E, D]{}, c.opts...),
		c.machine.WithRunToCompletion(),
		c.machine.WithInternalDispatch(c.proposeInternal),
	)
}

func (c *Ctrl[S, E, D]) bind(propose func(Command) error) {
	c.propose = propose
	c.reset()
}

func (c *Ctrl[S, E, D]) reset() {
	if c.actor != nil {
		c.actor.Stop()
	}
	c.actor = gstate.Start(c.machine, c.initial.Clone(), c.leader, c.actorOptions()...)
}

func (c *Ctrl[S, E, D]) proposeInternal(t gstate.InternalTrigger[S]) {
	raw, err := json.Marshal(t)
	if err != nil {
		logf("controller %s: encoding trigger: %v", c.name, err)
		return
	}
	// Asynchronous so the timer or invoke goroutine never waits on raft.
	go func() {
		if err := c.propose(Command{Controller: c.name, Internal: raw}); err != nil {
			logProposeError(c.name, err)
		}
	}()
}

func (c *Ctrl[S, E, D]) hasDependencyEvent() bool { return c.hasDepEv }

func (c *Ctrl[S, E, D]) health() Health {
	if c.healthFn == nil {
		return HealthUnknown
	}
	return c.healthFn(c.actor.States(), c.actor.Data())
}

func (c *Ctrl[S, E, D]) states() []string {
	states := c.actor.States()
	out := make([]string, len(states))
	for i, s := range states {
		out[i] = string(s)
	}
	return out
}

func (c *Ctrl[S, E, D]) apply(event string, args json.RawMessage) {
	if args == nil {
		c.actor.Send(E(event))
		return
	}
	c.actor.SendWith(E(event), args)
}

func (c *Ctrl[S, E, D]) fireInternal(raw json.RawMessage) (bool, error) {
	var t gstate.InternalTrigger[S]
	if err := json.Unmarshal(raw, &t); err != nil {
		return false, err
	}
	return c.actor.FireInternal(t), nil
}

func (c *Ctrl[S, E, D]) dependencyChanged(dc DependencyChange) {
	c.actor.SendWith(c.depEvent, dc)
}

func (c *Ctrl[S, E, D]) setLeader(leader bool) {
	c.leader = leader
	if leader {
		c.actor.Activate()
	} else {
		c.actor.Deactivate()
	}
}

func (c *Ctrl[S, E, D]) snapshot() (json.RawMessage, error) {
	return json.Marshal(c.actor.Snapshot())
}

func (c *Ctrl[S, E, D]) restore(raw json.RawMessage) error {
	var snap gstate.Snapshot[S, D]
	if err := json.Unmarshal(raw, &snap); err != nil {
		return err
	}
	c.actor.Stop()
	c.actor = gstate.Hydrate(c.machine, snap, c.leader, c.actorOptions()...)
	return nil
}

func (c *Ctrl[S, E, D]) stop() { c.actor.Stop() }
