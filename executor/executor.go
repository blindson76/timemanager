// Package executor runs any gstate machine as a replicated state machine on
// top of hashicorp/raft.
//
// Every node holds an actor for the same machine. External events and
// internal transitions (delayed transitions, invoke completions) are both
// written to the raft log and applied in log order on every node, so all
// replicas move through the same states with the same data. Only the leader
// runs timers and invokes; when they fire, the leader proposes the resulting
// transition instead of taking it locally.
//
// The data type D must round-trip through encoding/json, since snapshots are
// JSON-encoded gstate snapshots. Event args likewise arrive at actions as
// their JSON-decoded form (for example map[string]any for structs) on every
// node, including the one that proposed them.
package executor

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"sync"
	"time"

	"github.com/blindson76/gstate"
	"github.com/hashicorp/raft"
)

// DefaultApplyTimeout bounds how long a proposal waits to be committed.
const DefaultApplyTimeout = 3 * time.Second

// Command is a replicated log entry: either an external event with its args,
// or an internal transition proposed by the leader.
type Command[S ~string, E ~string] struct {
	Event    E                          `json:"event,omitempty"`
	Args     any                        `json:"args,omitempty"`
	Internal *gstate.InternalTrigger[S] `json:"internal,omitempty"`
}

// FSM adapts a gstate machine to [raft.FSM].
type FSM[S ~string, E ~string, D gstate.Cloner[D]] struct {
	machine      *gstate.Machine[S, E, D]
	opts         []gstate.Option[S, E, D]
	applyTimeout time.Duration

	mu     sync.Mutex
	actor  *gstate.Actor[S, E, D]
	leader bool
	r      *raft.Raft
}

// New creates an FSM whose actor starts from initial with services inactive.
// opts are passed to every actor the FSM creates (at start and on snapshot
// restore); run-to-completion mode and internal dispatch are always added and
// must not be overridden.
func New[S ~string, E ~string, D gstate.Cloner[D]](m *gstate.Machine[S, E, D], initial D, opts ...gstate.Option[S, E, D]) *FSM[S, E, D] {
	f := &FSM[S, E, D]{machine: m, applyTimeout: DefaultApplyTimeout}
	f.opts = append(append([]gstate.Option[S, E, D]{}, opts...),
		m.WithRunToCompletion(),
		m.WithInternalDispatch(f.proposeInternal),
	)
	f.actor = gstate.Start(m, initial, false, f.opts...)
	return f
}

// Attach binds the raft node built with this FSM. It must be called before
// the node can become leader, since proposals go through it.
func (f *FSM[S, E, D]) Attach(r *raft.Raft) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.r = r
}

// SetLeader starts the actor's timers and invokes when this node is the
// leader and stops them otherwise. Call it on every leadership change.
func (f *FSM[S, E, D]) SetLeader(leader bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leader = leader
	if leader {
		f.actor.Activate()
	} else {
		f.actor.Deactivate()
	}
}

// Propose replicates an external event. It must be called on the leader and
// returns once the event is committed and applied locally.
func (f *FSM[S, E, D]) Propose(event E, args any) error {
	return f.propose(Command[S, E]{Event: event, Args: args})
}

func (f *FSM[S, E, D]) propose(cmd Command[S, E]) error {
	f.mu.Lock()
	r := f.r
	f.mu.Unlock()
	if r == nil {
		return errors.New("executor: raft not attached")
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	return r.Apply(data, f.applyTimeout).Error()
}

// proposeInternal is the actor's internal dispatcher. It runs asynchronously
// so the timer or invoke goroutine never waits on raft. A trigger that turns
// out stale, or is proposed twice across a leader change, is rejected by
// FireInternal on every node.
func (f *FSM[S, E, D]) proposeInternal(t gstate.InternalTrigger[S]) {
	go func() {
		if err := f.propose(Command[S, E]{Internal: &t}); err != nil && !errors.Is(err, raft.ErrNotLeader) {
			log.Printf("executor: replicating %s %s -> %s: %v", t.Kind, t.Source, t.Target, err)
		}
	}()
}

// States returns the active states, root to leaf.
func (f *FSM[S, E, D]) States() []S {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.actor.States()
}

// Data returns a copy of the current data.
func (f *FSM[S, E, D]) Data() D {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.actor.Data()
}

// Apply implements [raft.FSM]. It returns the [gstate.Actor.FireInternal]
// result for internal transitions and nil for events.
func (f *FSM[S, E, D]) Apply(l *raft.Log) interface{} {
	var cmd Command[S, E]
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if cmd.Internal != nil {
		return f.actor.FireInternal(*cmd.Internal)
	}
	f.actor.SendWith(cmd.Event, cmd.Args)
	return nil
}

// Snapshot implements [raft.FSM].
func (f *FSM[S, E, D]) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, err := json.Marshal(f.actor.Snapshot())
	if err != nil {
		return nil, err
	}
	return snapshot(data), nil
}

// Restore implements [raft.FSM]. The restored actor keeps services running
// if this node is the leader.
func (f *FSM[S, E, D]) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var snap gstate.Snapshot[S, D]
	if err := json.NewDecoder(rc).Decode(&snap); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.actor.Stop()
	f.actor = gstate.Hydrate(f.machine, snap, f.leader, f.opts...)
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
