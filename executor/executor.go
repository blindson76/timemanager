// Package executor runs a set of gstate machines ("controllers") as one
// replicated state machine on top of hashicorp/raft.
//
// Every node holds an actor per controller. External events and internal
// transitions (delayed transitions, invoke completions) are written to the
// raft log and applied in log order on every node, so all replicas move
// through the same states with the same data. Only the leader runs timers
// and invokes; when they fire it proposes the resulting transition instead
// of taking it locally.
//
// Controllers may depend on each other. After every applied entry the
// executor recomputes each controller's [Health] in dependency order and
// sends a [DependencyChange] event to dependents of any controller whose
// health changed. This happens inside Apply, so it is deterministic and
// costs no extra log entries.
//
// Controller data must round-trip through encoding/json (snapshots are
// JSON). External event args reach actions as [json.RawMessage]; decode them
// with [DecodeArgs].
package executor

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"slices"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// DefaultApplyTimeout bounds how long a proposal waits to be committed.
const DefaultApplyTimeout = 3 * time.Second

// Command is a replicated log entry addressed to one controller: either an
// external event with its args, or an internal transition proposed by the
// leader.
type Command struct {
	Controller string          `json:"controller"`
	Event      string          `json:"event,omitempty"`
	Args       json.RawMessage `json:"args,omitempty"`
	Internal   json.RawMessage `json:"internal,omitempty"`
}

// Executor implements [raft.FSM] for a fixed set of controllers.
type Executor struct {
	applyTimeout time.Duration

	mu         sync.Mutex
	order      []Controller // dependencies before dependents
	byName     map[string]Controller
	dependents map[string][]Controller
	health     map[string]Health
	r          *raft.Raft
}

// New builds an executor for ctrls and starts their actors with services
// inactive. It fails on duplicate names, unknown dependencies, dependency
// cycles, or a controller with dependencies but no dependency event.
func New(ctrls ...Controller) (*Executor, error) {
	x := &Executor{
		applyTimeout: DefaultApplyTimeout,
		byName:       make(map[string]Controller, len(ctrls)),
		dependents:   make(map[string][]Controller),
		health:       make(map[string]Health, len(ctrls)),
	}
	for _, c := range ctrls {
		if c.Name() == "" {
			return nil, errors.New("executor: controller with empty name")
		}
		if _, dup := x.byName[c.Name()]; dup {
			return nil, fmt.Errorf("executor: duplicate controller %q", c.Name())
		}
		x.byName[c.Name()] = c
	}
	for _, c := range ctrls {
		if len(c.Dependencies()) > 0 && !c.hasDependencyEvent() {
			return nil, fmt.Errorf("executor: controller %q has dependencies but no OnDependencyChange event", c.Name())
		}
		for _, dep := range c.Dependencies() {
			if _, ok := x.byName[dep]; !ok {
				return nil, fmt.Errorf("executor: controller %q depends on unknown controller %q", c.Name(), dep)
			}
			x.dependents[dep] = append(x.dependents[dep], c)
		}
	}
	order, err := topoSort(ctrls)
	if err != nil {
		return nil, err
	}
	x.order = order

	for _, c := range x.order {
		c.bind(x.propose)
	}
	// Deliver initial dependency health. Every node does this identically
	// at startup, so it needs no log entry.
	x.propagateLocked()
	return x, nil
}

// topoSort orders controllers so dependencies come first, breaking ties by
// name so every node gets the same order.
func topoSort(ctrls []Controller) ([]Controller, error) {
	pending := make(map[string]int, len(ctrls))
	byName := make(map[string]Controller, len(ctrls))
	for _, c := range ctrls {
		pending[c.Name()] = len(c.Dependencies())
		byName[c.Name()] = c
	}
	var ready []string
	for name, n := range pending {
		if n == 0 {
			ready = append(ready, name)
		}
	}
	order := make([]Controller, 0, len(ctrls))
	for len(ready) > 0 {
		slices.Sort(ready)
		name := ready[0]
		ready = ready[1:]
		order = append(order, byName[name])
		delete(pending, name)
		for _, c := range ctrls {
			if slices.Contains(c.Dependencies(), name) {
				pending[c.Name()]--
				if pending[c.Name()] == 0 {
					ready = append(ready, c.Name())
				}
			}
		}
	}
	if len(pending) > 0 {
		cyclic := make([]string, 0, len(pending))
		for name := range pending {
			cyclic = append(cyclic, name)
		}
		slices.Sort(cyclic)
		return nil, fmt.Errorf("executor: dependency cycle among %v", cyclic)
	}
	return order, nil
}

// propagateLocked recomputes health in dependency order and notifies
// dependents of every change. Visiting in topological order means a
// controller has received all its dependencies' changes before its own
// health is computed, so one pass reaches a fixed point.
func (x *Executor) propagateLocked() {
	for _, c := range x.order {
		h := c.health()
		if h == x.health[c.Name()] {
			continue
		}
		x.health[c.Name()] = h
		for _, d := range x.dependents[c.Name()] {
			d.dependencyChanged(DependencyChange{Name: c.Name(), Health: h})
		}
	}
}

// Attach binds the raft node built with this executor. It must be called
// before the node can become leader, since proposals go through it.
func (x *Executor) Attach(r *raft.Raft) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.r = r
}

// SetLeader starts timers and invokes of every controller when this node is
// the leader and stops them otherwise. Call it on every leadership change.
func (x *Executor) SetLeader(leader bool) {
	x.mu.Lock()
	defer x.mu.Unlock()
	for _, c := range x.order {
		c.setLeader(leader)
	}
}

// Propose replicates an external event to a controller. It must be called on
// the leader and returns once the event is committed and applied locally.
func (x *Executor) Propose(controller, event string, args any) error {
	x.mu.Lock()
	_, ok := x.byName[controller]
	x.mu.Unlock()
	if !ok {
		return fmt.Errorf("executor: unknown controller %q", controller)
	}
	cmd := Command{Controller: controller, Event: event}
	if args != nil {
		raw, err := json.Marshal(args)
		if err != nil {
			return err
		}
		cmd.Args = raw
	}
	return x.propose(cmd)
}

func (x *Executor) propose(cmd Command) error {
	x.mu.Lock()
	r := x.r
	x.mu.Unlock()
	if r == nil {
		return errors.New("executor: raft not attached")
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	return r.Apply(data, x.applyTimeout).Error()
}

// ControllerInfo is a point-in-time view of one controller.
type ControllerInfo struct {
	Name      string   `json:"name"`
	Health    Health   `json:"health"`
	States    []string `json:"states"`
	DependsOn []string `json:"depends_on,omitempty"`
}

// Controllers describes every controller, dependencies before dependents.
func (x *Executor) Controllers() []ControllerInfo {
	x.mu.Lock()
	defer x.mu.Unlock()
	out := make([]ControllerInfo, len(x.order))
	for i, c := range x.order {
		out[i] = ControllerInfo{Name: c.Name(), Health: x.health[c.Name()], States: c.states(), DependsOn: c.Dependencies()}
	}
	return out
}

// Health returns a controller's last propagated health.
func (x *Executor) Health(controller string) Health {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.health[controller]
}

// States returns a controller's active states, root to leaf.
func (x *Executor) States(controller string) []string {
	x.mu.Lock()
	defer x.mu.Unlock()
	c, ok := x.byName[controller]
	if !ok {
		return nil
	}
	return c.states()
}

// Stop stops every controller's actor.
func (x *Executor) Stop() {
	x.mu.Lock()
	defer x.mu.Unlock()
	for _, c := range x.order {
		c.stop()
	}
}

// Apply implements [raft.FSM]. It returns the FireInternal result (bool) for
// internal transitions, nil for events, and an error for malformed entries.
func (x *Executor) Apply(l *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		return err
	}

	x.mu.Lock()
	defer x.mu.Unlock()
	c, ok := x.byName[cmd.Controller]
	if !ok {
		return fmt.Errorf("executor: unknown controller %q", cmd.Controller)
	}
	var result interface{}
	if cmd.Internal != nil {
		fired, err := c.fireInternal(cmd.Internal)
		if err != nil {
			return err
		}
		result = fired
	} else {
		c.apply(cmd.Event, cmd.Args)
	}
	x.propagateLocked()
	return result
}

type snapshotData struct {
	Controllers map[string]json.RawMessage `json:"controllers"`
	Health      map[string]Health          `json:"health"`
}

// Snapshot implements [raft.FSM].
func (x *Executor) Snapshot() (raft.FSMSnapshot, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	s := snapshotData{
		Controllers: make(map[string]json.RawMessage, len(x.order)),
		Health:      make(map[string]Health, len(x.health)),
	}
	for _, c := range x.order {
		raw, err := c.snapshot()
		if err != nil {
			return nil, fmt.Errorf("executor: snapshot of %q: %w", c.Name(), err)
		}
		s.Controllers[c.Name()] = raw
	}
	for k, v := range x.health {
		s.Health[k] = v
	}
	data, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	return snapshot(data), nil
}

// Restore implements [raft.FSM]. A controller missing from the snapshot
// (added since it was taken) restarts from its initial data.
func (x *Executor) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var s snapshotData
	if err := json.NewDecoder(rc).Decode(&s); err != nil {
		return err
	}

	x.mu.Lock()
	defer x.mu.Unlock()
	for name := range s.Controllers {
		if _, ok := x.byName[name]; !ok {
			return fmt.Errorf("executor: snapshot has unknown controller %q", name)
		}
	}
	var fresh []Controller
	for _, c := range x.order {
		raw, ok := s.Controllers[c.Name()]
		if !ok {
			c.reset()
			fresh = append(fresh, c)
			continue
		}
		if err := c.restore(raw); err != nil {
			return fmt.Errorf("executor: restoring %q: %w", c.Name(), err)
		}
	}
	// Restored dependents' data already reflects the snapshot's health;
	// continue from it so only changes after this point are delivered.
	x.health = make(map[string]Health, len(x.order))
	for name, h := range s.Health {
		x.health[name] = h
	}
	for _, c := range fresh {
		for _, dep := range c.Dependencies() {
			c.dependencyChanged(DependencyChange{Name: dep, Health: x.health[dep]})
		}
	}
	x.propagateLocked()
	return nil
}

type snapshot []byte

func (s snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (snapshot) Release() {}

func logf(format string, args ...any) { log.Printf("executor: "+format, args...) }

func logProposeError(controller string, err error) {
	if !errors.Is(err, raft.ErrNotLeader) {
		logf("controller %s: replicating internal transition: %v", controller, err)
	}
}
