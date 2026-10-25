package executor

import (
	"bytes"
	"encoding/json"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/blindson76/gstate"
	"github.com/hashicorp/raft"
)

type state string
type event string

type svcData struct {
	Version string            `json:"version"`
	Deps    map[string]Health `json:"deps"`
}

func (d svcData) Clone() svcData {
	c := d
	c.Deps = make(map[string]Health, len(d.Deps))
	for k, v := range d.Deps {
		c.Deps[k] = v
	}
	return c
}

// db: down --start--> starting --after 20ms--> ready --fail--> down
func dbController() *Ctrl[state, event, svcData] {
	m := gstate.New[state, event, svcData]("db").
		Initial("down").
		State("down", func(s *gstate.StateBuilder[state, event, svcData]) {
			s.On("start").GoTo("starting")
		}).
		State("starting", func(s *gstate.StateBuilder[state, event, svcData]) {
			s.After(20 * time.Millisecond).GoTo("ready")
		}).
		State("ready", func(s *gstate.StateBuilder[state, event, svcData]) {
			s.On("fail").GoTo("down")
			s.On("configure").AssignWith(func(d svcData, a any) svcData {
				if args, err := DecodeArgs[struct{ Version string }](a); err == nil {
					d.Version = args.Version
				}
				return d
			})
		}).
		Build()
	return NewController("db", m, svcData{}).WithHealth(func(states []state, _ svcData) Health {
		switch {
		case slices.Contains(states, "ready"):
			return HealthReady
		case slices.Contains(states, "starting"):
			return HealthPending
		default:
			return HealthFailed
		}
	})
}

// dependent runs while every dependency is Ready and waits otherwise.
func dependent(name string, deps ...string) *Ctrl[state, event, svcData] {
	allReady := func(d svcData) bool {
		for _, dep := range deps {
			if d.Deps[dep] != HealthReady {
				return false
			}
		}
		return true
	}
	m := gstate.New[state, event, svcData](name).
		Initial("root").
		State("root", func(s *gstate.StateBuilder[state, event, svcData]) {
			s.Initial("waiting")
			s.On("dep").AssignWith(func(d svcData, a any) svcData {
				dc := a.(DependencyChange)
				d.Deps[dc.Name] = dc.Health
				return d
			})
			s.State("waiting", func(s *gstate.StateBuilder[state, event, svcData]) {
				s.Always().Guard(allReady).GoTo("running")
			})
			s.State("running", func(s *gstate.StateBuilder[state, event, svcData]) {
				s.Always().Guard(func(d svcData) bool { return !allReady(d) }).GoTo("waiting")
			})
		}).
		Build()
	return NewController(name, m, svcData{Deps: map[string]Health{}}).
		DependsOn(deps...).
		OnDependencyChange("dep").
		WithHealth(func(states []state, _ svcData) Health {
			if slices.Contains(states, "running") {
				return HealthReady
			}
			return HealthPending
		})
}

func newStack(t *testing.T) *Executor {
	t.Helper()
	// Registration order is deliberately not dependency order.
	x, err := New(dependent("web", "app"), dependent("app", "db"), dbController())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(x.Stop)
	return x
}

func apply(t *testing.T, x *Executor, cmd Command) interface{} {
	t.Helper()
	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatal(err)
	}
	return x.Apply(&raft.Log{Type: raft.LogCommand, Data: data})
}

func trigger(t *testing.T, source, target string, epoch uint64) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(gstate.InternalTrigger[state]{Kind: gstate.TriggerAfter, Source: state(source), Target: state(target), Epoch: epoch})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func assertHealth(t *testing.T, x *Executor, want map[string]Health) {
	t.Helper()
	for name, h := range want {
		if got := x.Health(name); got != h {
			t.Errorf("%s health = %s, want %s", name, got, h)
		}
	}
}

func TestDependencyCascade(t *testing.T) {
	x := newStack(t)
	assertHealth(t, x, map[string]Health{"db": HealthFailed, "app": HealthPending, "web": HealthPending})

	apply(t, x, Command{Controller: "db", Event: "start"})
	assertHealth(t, x, map[string]Health{"db": HealthPending, "app": HealthPending})

	// One applied entry makes db ready, which must cascade through app to web.
	if fired := apply(t, x, Command{Controller: "db", Internal: trigger(t, "starting", "ready", 1)}); fired != true {
		t.Fatalf("FireInternal = %v, want true", fired)
	}
	assertHealth(t, x, map[string]Health{"db": HealthReady, "app": HealthReady, "web": HealthReady})

	apply(t, x, Command{Controller: "db", Event: "fail"})
	assertHealth(t, x, map[string]Health{"db": HealthFailed, "app": HealthPending, "web": HealthPending})
	if got := x.States("web"); !slices.Contains(got, "waiting") {
		t.Fatalf("web states = %v, want waiting", got)
	}
}

func TestEventArgsArriveAsJSON(t *testing.T) {
	db := dbController()
	x, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	defer x.Stop()
	apply(t, x, Command{Controller: "db", Event: "start"})
	apply(t, x, Command{Controller: "db", Internal: trigger(t, "starting", "ready", 1)})
	apply(t, x, Command{Controller: "db", Event: "configure", Args: json.RawMessage(`{"Version":"7.0"}`)})
	if got := db.Data().Version; got != "7.0" {
		t.Fatalf("version = %q, want 7.0", got)
	}
}

func TestApplyRejectsUnknownController(t *testing.T) {
	x := newStack(t)
	if _, ok := apply(t, x, Command{Controller: "nope", Event: "start"}).(error); !ok {
		t.Fatal("expected error for unknown controller")
	}
}

func TestNewValidates(t *testing.T) {
	for name, tc := range map[string]struct {
		ctrls []Controller
		want  string
	}{
		"duplicate":    {[]Controller{dbController(), dbController()}, "duplicate"},
		"unknown dep":  {[]Controller{dependent("app", "db")}, "unknown controller"},
		"cycle":        {[]Controller{dependent("a", "b"), dependent("b", "a")}, "cycle"},
		"no dep event": {[]Controller{dbController(), NewController("app", dbController().machine, svcData{}).DependsOn("db")}, "OnDependencyChange"},
		"self cycle":   {[]Controller{dependent("a", "a")}, "cycle"},
		"empty name":   {[]Controller{NewController("", dbController().machine, svcData{})}, "empty name"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := New(tc.ctrls...)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestSnapshotRestore(t *testing.T) {
	x := newStack(t)
	apply(t, x, Command{Controller: "db", Event: "start"})
	apply(t, x, Command{Controller: "db", Internal: trigger(t, "starting", "ready", 1)})

	snap, err := x.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&sink{Buffer: &buf}); err != nil {
		t.Fatal(err)
	}

	y := newStack(t)
	if err := y.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatal(err)
	}
	assertHealth(t, y, map[string]Health{"db": HealthReady, "app": HealthReady, "web": HealthReady})

	// Propagation continues correctly from the restored state.
	apply(t, y, Command{Controller: "db", Event: "fail"})
	assertHealth(t, y, map[string]Health{"db": HealthFailed, "app": HealthPending, "web": HealthPending})
}

func TestRestoreResetsControllerMissingFromSnapshot(t *testing.T) {
	db := dbController()
	x, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	defer x.Stop()
	apply(t, x, Command{Controller: "db", Event: "start"})
	apply(t, x, Command{Controller: "db", Internal: trigger(t, "starting", "ready", 1)})
	snap, err := x.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&sink{Buffer: &buf}); err != nil {
		t.Fatal(err)
	}

	// A newer build adds "app"; it must learn db's health from the snapshot.
	y, err := New(dbController(), dependent("app", "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer y.Stop()
	if err := y.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatal(err)
	}
	assertHealth(t, y, map[string]Health{"db": HealthReady, "app": HealthReady})
}

func TestDecodeArgs(t *testing.T) {
	type v struct{ N int }
	if got, err := DecodeArgs[v](json.RawMessage(`{"N":3}`)); err != nil || got.N != 3 {
		t.Fatalf("raw: %+v, %v", got, err)
	}
	if got, err := DecodeArgs[v](v{N: 4}); err != nil || got.N != 4 {
		t.Fatalf("typed: %+v, %v", got, err)
	}
	if _, err := DecodeArgs[v](nil); err == nil {
		t.Fatal("expected error for nil args")
	}
	if _, err := DecodeArgs[v]("x"); err == nil {
		t.Fatal("expected error for wrong type")
	}
}

// TestTimerReplicatesThroughRaft runs a real single-node raft: db's After
// timer fires on the leader, is proposed, and a follower that only replays
// the log (services never active) reaches the same state and health.
func TestTimerReplicatesThroughRaft(t *testing.T) {
	x := newStack(t)
	r, logs := startSingleNode(t, x)

	if err := x.Propose("db", "start", nil); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return x.Health("web") == HealthReady })

	follower := newStack(t)
	for i := uint64(1); i <= r.LastIndex(); i++ {
		var l raft.Log
		if err := logs.GetLog(i, &l); err != nil {
			continue
		}
		if l.Type == raft.LogCommand {
			follower.Apply(&l)
		}
	}
	assertHealth(t, follower, map[string]Health{"db": HealthReady, "app": HealthReady, "web": HealthReady})

	if err := x.Propose("nope", "start", nil); err == nil {
		t.Fatal("expected error proposing to unknown controller")
	}
}

func TestProposeWithoutRaft(t *testing.T) {
	if err := newStack(t).Propose("db", "start", nil); err == nil {
		t.Fatal("expected error without attached raft")
	}
}

func startSingleNode(t *testing.T, x *Executor) (*raft.Raft, raft.LogStore) {
	t.Helper()
	cfg := raft.DefaultConfig()
	cfg.LocalID = "n1"
	cfg.LogOutput = io.Discard
	cfg.HeartbeatTimeout = 50 * time.Millisecond
	cfg.ElectionTimeout = 50 * time.Millisecond
	cfg.LeaderLeaseTimeout = 50 * time.Millisecond

	addr, transport := raft.NewInmemTransport("")
	store := raft.NewInmemStore()
	r, err := raft.NewRaft(cfg, x, store, store, raft.NewInmemSnapshotStore(), transport)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Shutdown().Error() })
	x.Attach(r)

	if err := r.BootstrapCluster(raft.Configuration{Servers: []raft.Server{{ID: cfg.LocalID, Address: addr}}}).Error(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-r.LeaderCh():
	case <-time.After(5 * time.Second):
		t.Fatal("no leader elected")
	}
	x.SetLeader(true)
	return r, store
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

type sink struct{ *bytes.Buffer }

func (sink) ID() string    { return "test" }
func (sink) Cancel() error { return nil }
func (sink) Close() error  { return nil }
