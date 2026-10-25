package executor

import (
	"bytes"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/blindson76/gstate"
	"github.com/hashicorp/raft"
)

type state string
type event string

type counter struct {
	Hits int `json:"hits"`
}

func (c counter) Clone() counter { return c }

func toyMachine() *gstate.Machine[state, event, counter] {
	return gstate.New[state, event, counter]("toy").
		Initial("idle").
		State("idle", func(s *gstate.StateBuilder[state, event, counter]) {
			s.On("start").GoTo("running")
		}).
		State("running", func(s *gstate.StateBuilder[state, event, counter]) {
			s.Entry(func(c counter) counter { c.Hits++; return c })
			s.After(20 * time.Millisecond).GoTo("done")
		}).
		State("done", func(s *gstate.StateBuilder[state, event, counter]) {}).
		Build()
}

func startSingleNode(t *testing.T, f *FSM[state, event, counter]) (*raft.Raft, raft.LogStore) {
	t.Helper()
	cfg := raft.DefaultConfig()
	cfg.LocalID = "n1"
	cfg.LogOutput = io.Discard
	cfg.HeartbeatTimeout = 50 * time.Millisecond
	cfg.ElectionTimeout = 50 * time.Millisecond
	cfg.LeaderLeaseTimeout = 50 * time.Millisecond

	addr, transport := raft.NewInmemTransport("")
	store := raft.NewInmemStore()
	r, err := raft.NewRaft(cfg, f, store, store, raft.NewInmemSnapshotStore(), transport)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Shutdown().Error() })
	f.Attach(r)

	if err := r.BootstrapCluster(raft.Configuration{Servers: []raft.Server{{ID: cfg.LocalID, Address: addr}}}).Error(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-r.LeaderCh():
	case <-time.After(5 * time.Second):
		t.Fatal("no leader elected")
	}
	f.SetLeader(true)
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

func TestDelayedTransitionGoesThroughLog(t *testing.T) {
	f := New(toyMachine(), counter{})
	r, logs := startSingleNode(t, f)

	if err := f.Propose("start", nil); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return slices.Contains(f.States(), "done") })

	// A follower replaying the same log, with services never activated,
	// reaches the same state only because the timer was logged.
	follower := New(toyMachine(), counter{})
	for i := uint64(1); i <= r.LastIndex(); i++ {
		var l raft.Log
		if err := logs.GetLog(i, &l); err != nil {
			continue
		}
		if l.Type == raft.LogCommand {
			follower.Apply(&l)
		}
	}
	if got := follower.States(); !slices.Contains(got, "done") {
		t.Fatalf("follower states = %v, want done", got)
	}
	if follower.Data().Hits != 1 {
		t.Fatalf("follower hits = %d, want 1", follower.Data().Hits)
	}
}

func TestSnapshotRestore(t *testing.T) {
	f := New(toyMachine(), counter{})
	startSingleNode(t, f)
	if err := f.Propose("start", nil); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return slices.Contains(f.States(), "done") })

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&sink{Buffer: &buf}); err != nil {
		t.Fatal(err)
	}

	g := New(toyMachine(), counter{})
	if err := g.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(g.States(), "done") || g.Data().Hits != 1 {
		t.Fatalf("restored states=%v data=%+v", g.States(), g.Data())
	}
}

func TestProposeWithoutRaft(t *testing.T) {
	if err := New(toyMachine(), counter{}).Propose("start", nil); err == nil {
		t.Fatal("expected error without attached raft")
	}
}

type sink struct{ *bytes.Buffer }

func (sink) ID() string    { return "test" }
func (sink) Cancel() error { return nil }
func (sink) Close() error  { return nil }
