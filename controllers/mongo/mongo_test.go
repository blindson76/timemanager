package mongo

import (
	"encoding/json"
	"slices"
	"testing"
	"time"

	"example.com/mystate/controllers/service"
	"example.com/mystate/executor"
	"github.com/blindson76/gstate"
	"github.com/hashicorp/raft"
)

type harness struct {
	t    *testing.T
	x    *executor.Executor
	ctrl *executor.Ctrl[State, Event, Status]
}

func newHarness(t *testing.T, extra ...executor.Controller) *harness {
	t.Helper()
	sp := spec3()
	sp.ReportWindow, sp.OrderTimeout, sp.RetryDelay = time.Hour, time.Hour, time.Hour // fired by hand
	if len(extra) > 0 {
		sp.DependsOn = []string{"timesync"}
	}
	h := &harness{t: t, ctrl: New(sp)}
	x, err := executor.New(append(extra, h.ctrl)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(x.Stop)
	h.x = x
	return h
}

func (h *harness) apply(cmd executor.Command) interface{} {
	h.t.Helper()
	data, err := json.Marshal(cmd)
	if err != nil {
		h.t.Fatal(err)
	}
	return h.x.Apply(&raft.Log{Type: raft.LogCommand, Data: data})
}

func (h *harness) send(controller string, e string, args any) {
	h.t.Helper()
	cmd := executor.Command{Controller: controller, Event: e}
	if args != nil {
		raw, err := json.Marshal(args)
		if err != nil {
			h.t.Fatal(err)
		}
		cmd.Args = raw
	}
	h.apply(cmd)
}

func (h *harness) report(rs ...NodeReport) {
	h.t.Helper()
	for _, r := range rs {
		h.send("mongo", string(EventReport), r)
	}
}

func (h *harness) done(id int, errText string) {
	h.t.Helper()
	h.send("mongo", string(EventOrderDone), OrderResult{ID: id, Error: errText})
}

func (h *harness) fire(source, target State, epoch uint64) {
	h.t.Helper()
	raw, _ := json.Marshal(gstate.InternalTrigger[State]{Kind: gstate.TriggerAfter, Source: source, Target: target, Epoch: epoch})
	if fired := h.apply(executor.Command{Controller: "mongo", Internal: raw}); fired != true {
		h.t.Fatalf("trigger %s -> %s (epoch %d) not applied: %v", source, target, epoch, fired)
	}
}

func (h *harness) expectState(want State) {
	h.t.Helper()
	states := h.ctrl.States()
	if states[len(states)-1] != want {
		h.t.Fatalf("states = %v, want %s (plan %+v, error %q)", states, want, h.ctrl.Data().Plan, h.ctrl.Data().LastError)
	}
}

func (h *harness) expectHealth(want executor.Health) {
	h.t.Helper()
	if got := h.x.Health("mongo"); got != want {
		h.t.Fatalf("health = %s, want %s", got, want)
	}
}

// onlyOrder returns the single pending order, which must be for node and action.
func (h *harness) onlyOrder(node, action string) Order {
	h.t.Helper()
	orders := h.ctrl.Data().Orders
	if len(orders) != 1 || orders[0].Node != node || orders[0].Action != action {
		h.t.Fatalf("orders = %+v, want one %s for %s", orders, action, node)
	}
	return orders[0]
}

func TestDiscoveryInspectsEveryMember(t *testing.T) {
	h := newHarness(t)
	h.expectState(StateDiscovering)
	h.expectHealth(executor.HealthPending)
	var got []string
	for _, o := range h.ctrl.Data().Orders {
		if o.Action == ActionInspect {
			got = append(got, o.Node)
		}
	}
	if !slices.Equal(got, []string{"n1", "n2", "n3"}) {
		t.Fatalf("inspect orders for %v", got)
	}
	if orders := h.ctrl.Data().OrdersFor("n2"); len(orders) != 1 || orders[0].ID != 2 {
		t.Fatalf("OrdersFor(n2) = %+v", orders)
	}
}

func TestFreshClusterIsInitiated(t *testing.T) {
	h := newHarness(t)
	h.report(empty("n1"), empty("n2"))
	h.expectState(StateDiscovering)
	h.report(empty("n3"))
	h.expectState(StateConfiguring)

	o := h.onlyOrder("n1", ActionInitiate)
	if !slices.Equal(o.Config.nodes(), []string{"n1", "n2", "n3"}) {
		t.Fatalf("initiate config = %+v", o.Config)
	}
	h.done(o.ID, "")
	h.expectState(StateRunning)
	h.expectHealth(executor.HealthFailed) // no primary reported yet

	c := o.Config
	h.report(data("n1", 1, 2, RSPrimary, c), data("n2", 1, 2, RSSecondary, c))
	h.expectHealth(executor.HealthDegraded)
	h.report(data("n3", 1, 2, RSSecondary, c))
	h.expectHealth(executor.HealthReady)
	h.expectState(StateRunning)
}

func TestTooStaleMemberIsWipedAndRejoins(t *testing.T) {
	h := newHarness(t)
	c := cfg(3, "n1", "n2", "n3")
	h.report(data("n1", 100, 500, RSPrimary, c), data("n2", 120, 500, RSSecondary, c), data("n3", 10, 50, RSRecovering, c))
	h.expectState(StateWiping)
	if r := h.ctrl.Data().Plan.Reasons["n3"]; r == "" {
		t.Fatal("wipe has no reason")
	}

	o := h.onlyOrder("n3", ActionWipe)
	h.done(o.ID, "")
	// n3 stays a member; no reconfig is needed for it to resync.
	h.expectState(StateRunning)
	h.expectHealth(executor.HealthDegraded)
	if len(h.ctrl.Data().Orders) != 0 {
		t.Fatalf("unexpected orders %+v", h.ctrl.Data().Orders)
	}

	h.report(NodeReport{Node: "n3", Host: "n3:27017", Running: true, Empty: true, RSState: RSStartup2, Config: c})
	h.expectState(StateRunning) // initial sync is not "stale"
	h.report(data("n3", 480, 500, RSSecondary, c))
	h.expectHealth(executor.HealthReady)
}

func TestLateStaleNodeIsWipedWhileRunning(t *testing.T) {
	h := newHarness(t)
	c := cfg(3, "n1", "n2", "n3")
	h.report(data("n1", 100, 500, RSPrimary, c), data("n2", 100, 500, RSSecondary, c))
	h.fire(StateDiscovering, StateDiscoveringQuorum, 1)
	h.expectState(StateRunning) // majority reported; n3 is a kept, absent member

	h.report(data("n3", 10, 50, RSRecovering, c))
	h.expectState(StateWiping)
	h.onlyOrder("n3", ActionWipe)
}

func TestDiscoveryWithoutMajorityFails(t *testing.T) {
	h := newHarness(t)
	h.report(empty("n1"))
	h.fire(StateDiscovering, StateDiscoveringQuorum, 1)
	h.expectState(StateDiscoveringQuorum)
	h.fire(StateDiscoveringQuorum, StateFailed, 1)
	h.expectState(StateFailed)
	h.expectHealth(executor.HealthFailed)

	// Retry rediscovers from scratch with new order IDs.
	h.fire(StateFailed, StateDiscovering, 1)
	h.expectState(StateDiscovering)
	d := h.ctrl.Data()
	if len(d.Nodes) != 0 || len(d.Orders) != 3 || d.Orders[0].ID != 4 {
		t.Fatalf("rediscovery data: nodes %v, orders %+v", d.Nodes, d.Orders)
	}
}

func TestOrderFailureFailsStep(t *testing.T) {
	h := newHarness(t)
	h.report(empty("n1"), empty("n2"), empty("n3"))
	o := h.onlyOrder("n1", ActionInitiate)
	h.done(o.ID+100, "") // unknown order: ignored
	h.expectState(StateConfiguring)
	h.done(o.ID, "replSetInitiate: quorum check failed")
	h.expectState(StateFailed)
	if d := h.ctrl.Data(); d.LastError != "replSetInitiate: quorum check failed" || len(d.Orders) != 0 || d.Applied != nil {
		t.Fatalf("after failure: %+v", d)
	}
}

func TestBlockedUntilOperatorFixesForeignData(t *testing.T) {
	h := newHarness(t)
	foreign := cfg(1, "n1")
	foreign.SetName = "other"
	h.report(data("n1", 1, 9, RSSecondary, foreign), empty("n2"), empty("n3"))
	h.expectState(StateBlocked)
	h.expectHealth(executor.HealthFailed)

	// The operator wipes n1 by hand; its next report unblocks the plan.
	h.report(empty("n1"))
	h.expectState(StateConfiguring)
}

func TestWaitsForTimesync(t *testing.T) {
	ts := service.New(service.Spec{Name: "timesync", StartTimeout: time.Hour, RetryDelay: time.Hour})
	h := newHarness(t, ts)
	h.expectState(StateWaitingDeps)
	if len(h.ctrl.Data().Orders) != 0 {
		t.Fatal("orders issued before dependencies are ready")
	}
	h.send("timesync", string(service.EventStarted), nil)
	h.expectState(StateDiscovering)
}

func TestReplicasAgreeOnEverything(t *testing.T) {
	c := cfg(3, "n1", "n2", "n3")
	run := func() Status {
		h := newHarness(t)
		h.report(data("n3", 10, 50, RSRecovering, c), data("n2", 120, 500, RSSecondary, c), data("n1", 100, 500, RSPrimary, c))
		h.done(h.onlyOrder("n3", ActionWipe).ID, "")
		h.report(data("n3", 480, 500, RSSecondary, c))
		return h.ctrl.Data()
	}
	want, _ := json.Marshal(run())
	for range 20 {
		if got, _ := json.Marshal(run()); string(got) != string(want) {
			t.Fatalf("replicas diverged:\n%s\n%s", want, got)
		}
	}
}
