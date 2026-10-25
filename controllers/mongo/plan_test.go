package mongo

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

func spec3() Spec {
	return Spec{Name: "mongo", ReplicaSet: "rs0", Members: []string{"n1", "n2", "n3"}, Replicas: 3}
}

func ts(t uint32) Timestamp { return Timestamp{T: t} }
func op(t uint32) OpTime    { return OpTime{TS: ts(t), Term: 1} }

func cfg(version int, nodes ...string) *ReplConfig {
	c := &ReplConfig{SetName: "rs0", Version: version, Term: 1}
	for i, n := range nodes {
		c.Members = append(c.Members, Member{ID: i, Node: n, Host: n + ":27017"})
	}
	return c
}

// data is a running node holding oplog entries [oldest, newest].
func data(node string, oldest, newest uint32, rs string, c *ReplConfig) NodeReport {
	return NodeReport{Node: node, Host: node + ":27017", Running: true, OplogOldest: ts(oldest), OplogNewest: op(newest), Config: c, RSState: rs}
}

func empty(node string) NodeReport {
	return NodeReport{Node: node, Host: node + ":27017", Running: true, Empty: true}
}

func status(reports ...NodeReport) Status {
	s := Status{Nodes: map[string]NodeReport{}}
	for _, r := range reports {
		s.Nodes[r.Node] = r
	}
	return s
}

func TestPlanFreshClusterInitiates(t *testing.T) {
	p := ComputePlan(spec3(), status(empty("n2"), empty("n1"), empty("n3")))
	if p.Action != ActionInitiate || p.Target != "n1" {
		t.Fatalf("plan = %+v, want initiate on n1", p)
	}
	if got := p.Config.nodes(); !slices.Equal(got, []string{"n1", "n2", "n3"}) || p.Config.Members[1].Host != "n2:27017" || p.Config.Version != 1 {
		t.Fatalf("config = %+v", p.Config)
	}
}

func TestPlanInitiatesFromTheOnlyDataNode(t *testing.T) {
	p := ComputePlan(spec3(), status(empty("n1"), data("n2", 1, 9, "", nil), empty("n3")))
	if p.Action != ActionInitiate || p.Target != "n2" {
		t.Fatalf("plan = %+v, want initiate on n2", p)
	}
}

func TestPlanRefusesToMergeUnrelatedData(t *testing.T) {
	p := ComputePlan(spec3(), status(data("n1", 1, 5, "", nil), data("n2", 1, 9, "", nil), empty("n3")))
	if !strings.Contains(p.Blocked, "unrelated") {
		t.Fatalf("plan = %+v, want blocked", p)
	}
}

func TestPlanBlocksOnForeignReplicaSet(t *testing.T) {
	foreign := cfg(1, "n1")
	foreign.SetName = "other"
	p := ComputePlan(spec3(), status(data("n1", 1, 9, RSSecondary, foreign), empty("n2"), empty("n3")))
	if !strings.Contains(p.Blocked, `"other"`) || len(p.Wipe) > 0 {
		t.Fatalf("plan = %+v, want blocked without wipe", p)
	}
}

func TestPlanHealthySetIsNoop(t *testing.T) {
	c := cfg(3, "n1", "n2", "n3")
	p := ComputePlan(spec3(), status(data("n1", 100, 500, RSPrimary, c), data("n2", 100, 498, RSSecondary, c), data("n3", 90, 499, RSSecondary, c)))
	if !p.Noop() {
		t.Fatalf("plan = %+v, want no-op", p)
	}
}

func TestPlanWipesTooStaleMember(t *testing.T) {
	c := cfg(3, "n1", "n2", "n3")
	p := ComputePlan(spec3(), status(
		data("n1", 100, 500, RSPrimary, c),
		data("n2", 120, 500, RSSecondary, c),
		data("n3", 10, 50, RSRecovering, c), // newest 50 < every other oldest
	))
	if !slices.Equal(p.Wipe, []string{"n3"}) || p.Action != "" || !strings.Contains(p.Reasons["n3"], "too stale") {
		t.Fatalf("plan = %+v, want wipe n3 only", p)
	}
}

func TestPlanLaggingMemberThatCanCatchUpIsKept(t *testing.T) {
	c := cfg(3, "n1", "n2", "n3")
	p := ComputePlan(spec3(), status(
		data("n1", 100, 500, RSPrimary, c),
		data("n2", 40, 500, RSSecondary, c), // n2's longer oplog still covers n3's newest
		data("n3", 10, 50, RSRecovering, c),
	))
	if !p.Noop() {
		t.Fatalf("plan = %+v, want no-op", p)
	}
}

func TestPlanNoWipeWithoutCompleteEvidence(t *testing.T) {
	c := cfg(3, "n1", "n2", "n3")
	// No primary and n3 has not reported: n3 might hold a covering oplog.
	p := ComputePlan(spec3(), status(data("n1", 100, 500, RSSecondary, c), data("n2", 10, 50, RSSecondary, c)))
	if len(p.Wipe) > 0 {
		t.Fatalf("plan = %+v, want no wipe", p)
	}
}

func TestPlanNodeAheadOfPrimaryIsNotWiped(t *testing.T) {
	c := cfg(3, "n1", "n2", "n3")
	// n3 has writes the primary lacks; mongod rolls them back itself.
	p := ComputePlan(spec3(), status(data("n1", 100, 500, RSPrimary, c), data("n2", 100, 500, RSSecondary, c), data("n3", 300, 520, RSSecondary, c)))
	if len(p.Wipe) > 0 {
		t.Fatalf("plan = %+v, want no wipe", p)
	}
}

func TestPlanAddsMostRecentCandidate(t *testing.T) {
	sp := spec3()
	sp.Members = []string{"n1", "n2", "n3", "n4"}
	c := cfg(3, "n1", "n2")
	p := ComputePlan(sp, status(data("n1", 100, 500, RSPrimary, c), data("n2", 100, 500, RSSecondary, c), empty("n3"), data("n4", 100, 450, RSSecondary, c)))
	if p.Action != ActionReconfig || p.Target != "n1" || !slices.Equal(p.Config.nodes(), []string{"n1", "n2", "n4"}) || p.Config.Version != 4 {
		t.Fatalf("plan = %+v, want reconfig adding n4 (more recent than empty n3)", p)
	}
	if p.Config.Members[2].ID != 2 || p.Config.Members[2].Host != "n4:27017" {
		t.Fatalf("new member = %+v", p.Config.Members[2])
	}
}

func TestPlanChangesOneMemberPerStep(t *testing.T) {
	sp := spec3()
	sp.Members = []string{"n1", "n4", "n5"} // n2, n3 no longer eligible
	c := cfg(7, "n1", "n2", "n3")
	s := status(data("n1", 1, 9, RSPrimary, c), empty("n4"), empty("n5"))
	p := ComputePlan(sp, s)
	if p.Action != ActionReconfig || !slices.Equal(p.Config.nodes(), []string{"n1", "n3"}) || p.Config.Version != 8 {
		t.Fatalf("step 1 = %+v, want removal of n2 only", p)
	}
	// Feed each applied step back, as the controller does via Applied.
	for range 10 {
		if p.Action == "" {
			break
		}
		s.Applied = p.Config
		p = ComputePlan(sp, s)
	}
	if !p.Noop() || !slices.Equal(s.Applied.nodes(), []string{"n1", "n4", "n5"}) {
		t.Fatalf("did not converge: applied %v, plan %+v", s.Applied.nodes(), p)
	}
}

func TestPlanKeepsDownMembers(t *testing.T) {
	c := cfg(3, "n1", "n2", "n3")
	p := ComputePlan(spec3(), status(data("n1", 1, 9, RSPrimary, c), data("n2", 1, 9, RSSecondary, c), NodeReport{Node: "n3"}))
	if !p.Noop() {
		t.Fatalf("plan = %+v, want down member kept without reconfig", p)
	}
}

func TestPlanShrinksKeepingSource(t *testing.T) {
	sp := spec3()
	sp.Replicas = 1
	c := cfg(3, "n1", "n2", "n3")
	p := ComputePlan(sp, status(data("n1", 1, 8, RSSecondary, c), data("n2", 1, 9, RSPrimary, c), data("n3", 1, 7, RSSecondary, c)))
	if p.Action != ActionReconfig || !slices.Contains(p.Config.nodes(), "n2") {
		t.Fatalf("plan = %+v, want a removal that keeps primary n2", p)
	}
}

func TestPlanWaitsForElectionWithMajority(t *testing.T) {
	sp := spec3()
	sp.Members = []string{"n1", "n2", "n3", "n4"}
	sp.Replicas = 4 // n4 wants in, which needs a primary to reconfig
	c := cfg(3, "n1", "n2", "n3")
	p := ComputePlan(sp, status(data("n1", 1, 9, RSSecondary, c), data("n2", 1, 9, RSSecondary, c), empty("n4")))
	if p.Blocked != "waiting for primary election" {
		t.Fatalf("plan = %+v", p)
	}
}

func TestPlanForceReconfigOnlyWhenAllowed(t *testing.T) {
	sp := spec3()
	c := cfg(3, "n1", "n2", "n3")
	s := status(data("n1", 1, 9, RSSecondary, c))
	if p := ComputePlan(sp, s); !strings.Contains(p.Blocked, "force reconfig not allowed") {
		t.Fatalf("plan = %+v, want blocked", p)
	}
	sp.AllowForceReconfig = true
	p := ComputePlan(sp, s)
	if p.Action != ActionReconfig || !p.Force || p.Target != "n1" || !slices.Equal(p.Config.nodes(), []string{"n1"}) {
		t.Fatalf("plan = %+v, want forced reconfig to n1 alone", p)
	}
}

func TestPlanAppliedBridgesUntilReported(t *testing.T) {
	s := status(empty("n1"), empty("n2"), empty("n3"))
	s.Applied = cfg(1, "n1", "n2", "n3")
	if p := ComputePlan(spec3(), s); !p.Noop() {
		t.Fatalf("plan = %+v, want no-op right after initiate", p)
	}
}

func TestPlanIgnoresIneligibleAndFailedNodes(t *testing.T) {
	s := status(empty("n1"), empty("n2"), data("x9", 1, 9, "", nil))
	s.Nodes["n3"] = NodeReport{Node: "n3", Running: true, Error: "disk full"}
	p := ComputePlan(spec3(), s)
	if p.Action != ActionInitiate || !slices.Equal(p.Config.nodes(), []string{"n1", "n2"}) {
		t.Fatalf("plan = %+v, want initiate with n1, n2", p)
	}
}

func TestPlanIsDeterministic(t *testing.T) {
	sp := spec3()
	sp.Members = []string{"n5", "n1", "n4", "n2", "n3"}
	c := cfg(3, "n1", "n2")
	s := status(data("n1", 100, 500, RSPrimary, c), data("n2", 100, 500, RSSecondary, c), empty("n3"), empty("n4"), empty("n5"))
	want, _ := json.Marshal(ComputePlan(sp, s))
	for range 200 {
		if got, _ := json.Marshal(ComputePlan(sp, s.Clone())); string(got) != string(want) {
			t.Fatalf("plan changed between runs:\n%s\n%s", want, got)
		}
	}
}
