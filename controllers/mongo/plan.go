package mongo

import (
	"fmt"
	"slices"
)

// ComputePlan decides the next reconciliation step from the replicated
// status. It is a pure function: every node computes the same plan.
//
// Rules, in order:
//
//  1. Only eligible nodes that report a running mongod are considered.
//  2. A node holding data of another replica set blocks the plan: that is
//     an operator decision, never an automatic wipe.
//  3. The current config is the newest (term, version) config reported by
//     any node, or the one this controller last applied if newer.
//  4. The source of truth is the reported primary, else the node with the
//     newest oplog entry. The source is never wiped.
//  5. A node is too stale when it is behind the source and no other node's
//     oplog still covers its newest entry, so it cannot catch up through
//     replication. Too-stale nodes are wiped, but only when the evidence is
//     complete: a primary is reported or every eligible node has reported.
//     Otherwise they are just kept out of new membership.
//  6. Without a current config, one node with data (plus empty nodes) is
//     initiated from; several nodes with unrelated data block the plan.
//  7. Membership is sticky: current members stay, even while down, unless
//     no longer eligible or above Replicas. Free slots go to the most recent
//     candidates (data nodes by newest oplog entry, then empty nodes).
//  8. With a majority of current members lost and no primary, the plan
//     forces a reconfig to the reachable members if AllowForceReconfig,
//     else blocks. This applies even when membership is as desired.
//  9. Reconfigs change one voting member per step (removals first), sent to
//     the primary. Without a primary but with a majority running, the plan
//     waits for an election.
func ComputePlan(spec Spec, s Status) Plan {
	live, allReported := liveReports(spec, s)
	if len(live) == 0 {
		return Plan{Blocked: "no running members reported"}
	}

	for _, r := range live {
		if r.Config != nil && r.Config.SetName != spec.ReplicaSet {
			return Plan{Blocked: fmt.Sprintf("node %s holds data of replica set %q, expected %q", r.Node, r.Config.SetName, spec.ReplicaSet)}
		}
	}

	current := currentConfig(spec, s, live)
	source := pickSource(live)

	if current == nil {
		for _, r := range live {
			if r.hasData() && r.Node != source.Node {
				return Plan{Blocked: fmt.Sprintf("nodes %s and %s both hold data without a replica set config; refusing to merge unrelated data sets", source.Node, r.Node)}
			}
		}
	}

	p := Plan{}
	excluded := map[string]bool{}
	if source != nil && current != nil {
		authoritative := allReported || source.RSState == RSPrimary
		for _, r := range live {
			if r.Node == source.Node || !r.hasData() || !r.OplogNewest.Less(source.OplogNewest) || canCatchUp(r, live) {
				continue
			}
			excluded[r.Node] = true
			if authoritative {
				p.Wipe = append(p.Wipe, r.Node)
				if p.Reasons == nil {
					p.Reasons = map[string]string{}
				}
				p.Reasons[r.Node] = fmt.Sprintf("too stale: newest op %v is older than every oplog that could feed it (source %s oldest %v)",
					r.OplogNewest, source.Node, source.OplogOldest)
			}
		}
	}
	if len(p.Wipe) > 0 {
		return p
	}

	desired := desiredMembers(spec, current, live, source, excluded)
	if len(desired) == 0 {
		return Plan{Blocked: "no eligible candidates"}
	}

	if current == nil {
		target := desired[0]
		cfg := &ReplConfig{SetName: spec.ReplicaSet, Version: 1}
		for i, n := range slices.Sorted(slices.Values(desired)) {
			cfg.Members = append(cfg.Members, Member{ID: i, Node: n, Host: hostOf(n, live, nil)})
		}
		return Plan{Action: ActionInitiate, Target: target, Config: cfg}
	}

	hasPrimary := source != nil && source.RSState == RSPrimary
	running := 0
	for _, m := range current.Members {
		if slices.ContainsFunc(live, func(r NodeReport) bool { return r.Node == m.Node }) {
			running++
		}
	}
	majority := 2*running > len(current.Members)

	// Losing the majority needs action even when membership is as desired:
	// without it no primary can ever be elected.
	if !hasPrimary && !majority {
		if !spec.AllowForceReconfig || source == nil {
			return Plan{Blocked: fmt.Sprintf("majority of replica set unreachable (%d/%d running); force reconfig not allowed", running, len(current.Members))}
		}
		// Keep only reachable members; later steps add the others back.
		forced := current.clone()
		forced.Version++
		forced.Members = slices.DeleteFunc(forced.Members, func(m Member) bool {
			return !slices.ContainsFunc(live, func(r NodeReport) bool { return r.Node == m.Node })
		})
		return Plan{Action: ActionReconfig, Target: source.Node, Config: forced, Force: true}
	}

	next, changed := stepToward(current, desired, live)
	if !changed {
		return Plan{}
	}
	if !hasPrimary {
		return Plan{Blocked: "waiting for primary election"}
	}
	return Plan{Action: ActionReconfig, Target: source.Node, Config: next}
}

// liveReports returns reports of eligible nodes running mongod without
// error, sorted by node, and whether every eligible node has reported.
func liveReports(spec Spec, s Status) ([]NodeReport, bool) {
	var live []NodeReport
	allReported := true
	for _, n := range slices.Sorted(slices.Values(spec.Members)) {
		r, ok := s.Nodes[n]
		if !ok {
			allReported = false
			continue
		}
		if r.Running && r.Error == "" {
			live = append(live, r)
		}
	}
	return live, allReported
}

// currentConfig is rule 3: the newest config reported by a live node, or the
// last applied one if newer. Applied bridges the gap between a successful
// initiate/reconfig and the nodes reporting it; discovery clears it so a
// fresh discovery trusts only what nodes report.
func currentConfig(spec Spec, s Status, live []NodeReport) *ReplConfig {
	var current *ReplConfig
	for _, r := range live {
		if r.Config != nil && r.Config.SetName == spec.ReplicaSet && r.Config.newer(current) {
			current = r.Config
		}
	}
	if s.Applied != nil && s.Applied.SetName == spec.ReplicaSet && s.Applied.newer(current) {
		current = s.Applied
	}
	return current
}

// pickSource returns the reported primary (highest term if several reports
// claim it), else the data node with the newest oplog entry, else nil.
// live is sorted by node, so ties go to the lowest node ID.
func pickSource(live []NodeReport) *NodeReport {
	var src *NodeReport
	for i := range live {
		r := &live[i]
		if r.RSState == RSPrimary && (src == nil || src.OplogNewest.Term < r.OplogNewest.Term) {
			src = r
		}
	}
	if src != nil {
		return src
	}
	for i := range live {
		r := &live[i]
		if r.hasData() && (src == nil || src.OplogNewest.Less(r.OplogNewest)) {
			src = r
		}
	}
	return src
}

// canCatchUp reports whether some other data node's oplog window still
// contains r's newest entry, so r can resume replication from it.
func canCatchUp(r NodeReport, live []NodeReport) bool {
	for _, o := range live {
		if o.Node == r.Node || !o.hasData() {
			continue
		}
		if !o.OplogNewest.Less(r.OplogNewest) && !r.OplogNewest.TS.Less(o.OplogOldest) {
			return true
		}
	}
	return false
}

// desiredMembers applies rule 7 and returns members in rank order, the
// source first.
func desiredMembers(spec Spec, current *ReplConfig, live []NodeReport, source *NodeReport, excluded map[string]bool) []string {
	rank := rankCandidates(live, excluded)
	pos := func(n string) int {
		if i := slices.Index(rank, n); i >= 0 {
			return i
		}
		return len(rank) // unreported members rank last
	}

	var desired []string
	for _, n := range current.nodes() {
		if slices.Contains(spec.Members, n) {
			desired = append(desired, n)
		}
	}
	slices.SortStableFunc(desired, func(a, b string) int { return pos(a) - pos(b) })
	for len(desired) > spec.Replicas {
		// Drop the lowest-ranked member, never the source.
		drop := len(desired) - 1
		if source != nil && desired[drop] == source.Node {
			drop--
		}
		desired = slices.Delete(desired, drop, drop+1)
	}
	for _, n := range rank {
		if len(desired) >= spec.Replicas {
			break
		}
		if !slices.Contains(desired, n) {
			desired = append(desired, n)
		}
	}
	return desired
}

// rankCandidates orders live, non-excluded nodes by recency: nodes with data
// by newest oplog entry, then empty nodes, ties by node ID.
func rankCandidates(live []NodeReport, excluded map[string]bool) []string {
	var cands []NodeReport
	for _, r := range live {
		if !excluded[r.Node] {
			cands = append(cands, r)
		}
	}
	slices.SortStableFunc(cands, func(a, b NodeReport) int {
		switch {
		case a.hasData() != b.hasData():
			if a.hasData() {
				return -1
			}
			return 1
		case b.OplogNewest.Less(a.OplogNewest):
			return -1
		case a.OplogNewest.Less(b.OplogNewest):
			return 1
		}
		return 0 // stable: live is sorted by node
	})
	out := make([]string, len(cands))
	for i, r := range cands {
		out[i] = r.Node
	}
	return out
}

// stepToward returns current with one membership change toward desired:
// the first removal in config order, else the first addition by rank.
func stepToward(current *ReplConfig, desired []string, live []NodeReport) (*ReplConfig, bool) {
	next := current.clone()
	next.Version++
	for i, m := range next.Members {
		if !slices.Contains(desired, m.Node) {
			next.Members = slices.Delete(next.Members, i, i+1)
			return next, true
		}
	}
	for _, n := range desired {
		if slices.ContainsFunc(next.Members, func(m Member) bool { return m.Node == n }) {
			continue
		}
		id := 0
		for _, m := range next.Members {
			id = max(id, m.ID+1)
		}
		next.Members = append(next.Members, Member{ID: id, Node: n, Host: hostOf(n, live, current)})
		return next, true
	}
	return current, false
}

func hostOf(node string, live []NodeReport, cfg *ReplConfig) string {
	for _, r := range live {
		if r.Node == node && r.Host != "" {
			return r.Host
		}
	}
	if cfg != nil {
		for _, m := range cfg.Members {
			if m.Node == node {
				return m.Host
			}
		}
	}
	return ""
}
