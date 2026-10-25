// Package mongo controls a MongoDB replica set spread over cluster nodes.
//
// The controller never touches mongod. It decides; node agents act. Agents
// read their pending orders from the replicated [Status] (see
// [Status.OrdersFor]), execute them idempotently, acknowledge each with
// [EventOrderDone] and send a fresh [NodeReport] with [EventReport].
//
//	WaitingDeps ─deps Ready─► Discovering ─all reported─► Planning
//	                            │ after ReportWindow          ▲ │
//	                            ▼                             │ ├─blocked─► Blocked ─plan unblocked─► Planning
//	                     DiscoveringQuorum ─majority──────────┘ ├─wipes───► Wiping ─all done──► Planning
//	                                                            ├─config──► Configuring ─done─► Planning
//	                                                            └─no-op───► Running ─plan not no-op─► Planning
//	Discovering, Wiping, Configuring ─timeout or order error─► Failed ─after RetryDelay─► Discovering
//
// Discovery orders every eligible node to inspect itself: start mongod with
// --replSet and report its oplog window, newest optime, local replica set
// config and member state. Planning is [ComputePlan], a pure function of the
// replicated status, so every node takes the same decision. In Running the
// plan is re-evaluated on every report, which is how late or recovered
// nodes are wiped or added.
package mongo

import (
	"slices"

	"example.com/mystate/executor"
	"github.com/blindson76/gstate"
)

type State string

const (
	StateRoot              State = "Root"
	StateWaitingDeps       State = "WaitingDeps"
	StateDiscovering       State = "Discovering"
	StateDiscoveringQuorum State = "DiscoveringQuorum"
	StatePlanning          State = "Planning"
	StateWiping            State = "Wiping"
	StateConfiguring       State = "Configuring"
	StateRunning           State = "Running"
	StateBlocked           State = "Blocked"
	StateFailed            State = "Failed"
)

type Event string

const (
	// EventDependency carries an [executor.DependencyChange].
	EventDependency Event = "Dependency"
	// EventReport carries a [NodeReport].
	EventReport Event = "Report"
	// EventOrderDone carries an [OrderResult].
	EventOrderDone Event = "OrderDone"
)

// New builds the mongo controller. Its health is Ready when every member is
// PRIMARY or SECONDARY, Degraded when a majority is and a primary exists,
// Failed without a primary, and Pending while discovering or changing.
func New(spec Spec, observers ...gstate.Observer[State, Event, Status]) *executor.Ctrl[State, Event, Status] {
	observers = append([]gstate.Observer[State, Event, Status]{&executor.TransitionLogger[State, Event, Status]{Name: spec.Name}}, observers...)

	depsReady := func(s Status) bool {
		for _, dep := range spec.DependsOn {
			if s.Deps[dep] != executor.HealthReady {
				return false
			}
		}
		return true
	}
	plan := func(s Status) Plan { return ComputePlan(spec, s) }
	running := func(s Status) int {
		live, _ := liveReports(spec, s)
		return len(live)
	}
	allReported := func(s Status) bool {
		_, all := liveReports(spec, s)
		return all
	}
	allDone := func(s Status) bool {
		for _, id := range s.PlanOrders {
			if _, ok := s.Results[id]; !ok {
				return false
			}
		}
		return true
	}
	anyFailed := func(s Status) bool {
		for _, id := range s.PlanOrders {
			if s.Results[id] != "" {
				return true
			}
		}
		return false
	}
	addOrder := func(s Status, o Order) Status {
		s.NextOrderID++
		o.ID = s.NextOrderID
		s.Orders = append(s.Orders, o)
		s.PlanOrders = append(s.PlanOrders, o.ID)
		return s
	}

	m := gstate.New[State, Event, Status](spec.Name).
		Initial(StateRoot).
		State(StateRoot, func(sb *gstate.StateBuilder[State, Event, Status]) {
			sb.Initial(StateWaitingDeps)
			sb.On(EventDependency).AssignWith(func(s Status, a any) Status {
				if dc, ok := a.(executor.DependencyChange); ok {
					s.Deps[dc.Name] = dc.Health
				}
				return s
			})
			sb.On(EventReport).AssignWith(func(s Status, a any) Status {
				r, err := executor.DecodeArgs[NodeReport](a)
				if err != nil || !slices.Contains(spec.Members, r.Node) {
					return s
				}
				s.Nodes[r.Node] = r
				return s
			})
			sb.On(EventOrderDone).AssignWith(func(s Status, a any) Status {
				res, err := executor.DecodeArgs[OrderResult](a)
				if err != nil {
					return s
				}
				i := slices.IndexFunc(s.Orders, func(o Order) bool { return o.ID == res.ID })
				if i < 0 {
					return s // duplicate or cancelled
				}
				o := s.Orders[i]
				s.Orders = slices.Delete(s.Orders, i, i+1)
				if slices.Contains(s.PlanOrders, o.ID) {
					s.Results[o.ID] = res.Error
				}
				if res.Error != "" {
					return s
				}
				switch o.Action {
				case ActionWipe:
					// Forget the stale report; the node rejoins once it reports again.
					delete(s.Nodes, o.Node)
				case ActionInitiate, ActionReconfig:
					s.Applied = o.Config.clone()
				}
				return s
			})

			sb.State(StateWaitingDeps, func(sb *gstate.StateBuilder[State, Event, Status]) {
				sb.Always().Guard(depsReady).GoTo(StateDiscovering)
			})

			sb.State(StateDiscovering, func(sb *gstate.StateBuilder[State, Event, Status]) {
				sb.Entry(func(s Status) Status {
					s.Nodes = map[string]NodeReport{}
					s.Applied = nil
					s.Orders, s.PlanOrders, s.Results = nil, nil, map[int]string{}
					for _, n := range slices.Sorted(slices.Values(spec.Members)) {
						s = addOrder(s, Order{Node: n, Action: ActionInspect})
					}
					return s
				})
				sb.Always().Guard(allReported).GoTo(StatePlanning)
				sb.After(spec.ReportWindow).GoTo(StateDiscoveringQuorum)
			})
			sb.State(StateDiscoveringQuorum, func(sb *gstate.StateBuilder[State, Event, Status]) {
				sb.Always().Guard(func(s Status) bool { return 2*running(s) > len(spec.Members) }).GoTo(StatePlanning)
				sb.After(spec.OrderTimeout).GoTo(StateFailed)
			})

			sb.State(StatePlanning, func(sb *gstate.StateBuilder[State, Event, Status]) {
				sb.Entry(func(s Status) Status {
					s.Plan = plan(s)
					// Orders of the previous step are finished or obsolete.
					s.Orders, s.PlanOrders, s.Results = nil, nil, map[int]string{}
					s.LastError = ""
					return s
				})
				sb.Always().Guard(func(s Status) bool { return s.Plan.Blocked != "" }).GoTo(StateBlocked)
				sb.Always().Guard(func(s Status) bool { return len(s.Plan.Wipe) > 0 }).GoTo(StateWiping)
				sb.Always().Guard(func(s Status) bool { return s.Plan.Action != "" }).GoTo(StateConfiguring)
				sb.Always().GoTo(StateRunning)
			})

			sb.State(StateWiping, func(sb *gstate.StateBuilder[State, Event, Status]) {
				sb.Entry(func(s Status) Status {
					for _, n := range s.Plan.Wipe {
						s = addOrder(s, Order{Node: n, Action: ActionWipe})
					}
					return s
				})
				sb.Always().Guard(anyFailed).GoTo(StateFailed)
				sb.Always().Guard(allDone).GoTo(StatePlanning)
				sb.After(spec.OrderTimeout).GoTo(StateFailed)
			})

			sb.State(StateConfiguring, func(sb *gstate.StateBuilder[State, Event, Status]) {
				sb.Entry(func(s Status) Status {
					return addOrder(s, Order{Node: s.Plan.Target, Action: s.Plan.Action, Config: s.Plan.Config.clone(), Force: s.Plan.Force})
				})
				sb.Always().Guard(anyFailed).GoTo(StateFailed)
				sb.Always().Guard(allDone).GoTo(StatePlanning)
				sb.After(spec.OrderTimeout).GoTo(StateFailed)
			})

			sb.State(StateRunning, func(sb *gstate.StateBuilder[State, Event, Status]) {
				sb.Always().Guard(func(s Status) bool { return !plan(s).Noop() }).GoTo(StatePlanning)
			})
			sb.State(StateBlocked, func(sb *gstate.StateBuilder[State, Event, Status]) {
				sb.Always().Guard(func(s Status) bool { return plan(s).Blocked == "" }).GoTo(StatePlanning)
			})

			sb.State(StateFailed, func(sb *gstate.StateBuilder[State, Event, Status]) {
				sb.Entry(func(s Status) Status {
					s.LastError = firstError(s)
					s.Orders = nil
					return s
				})
				sb.After(spec.RetryDelay).GoTo(StateDiscovering)
			})
		}).
		Build()

	c := executor.NewController(spec.Name, m, Status{
		Deps:    map[string]executor.Health{},
		Nodes:   map[string]NodeReport{},
		Results: map[int]string{},
	}).
		WithHealth(func(states []State, s Status) executor.Health {
			switch {
			case slices.Contains(states, StateFailed):
				return executor.HealthFailed
			case slices.Contains(states, StateRunning), slices.Contains(states, StateBlocked):
				return replicaSetHealth(spec, s)
			default:
				return executor.HealthPending
			}
		}).
		WithActorOptions(m.WithObservers(observers...))
	if len(spec.DependsOn) > 0 {
		c.DependsOn(spec.DependsOn...).OnDependencyChange(EventDependency)
	}
	return c
}

// firstError returns the error of the lowest-numbered failed order of the
// current step, or a timeout message when none failed.
func firstError(s Status) string {
	for _, id := range slices.Sorted(slices.Values(s.PlanOrders)) {
		if err := s.Results[id]; err != "" {
			return err
		}
	}
	return "step timed out"
}

func replicaSetHealth(spec Spec, s Status) executor.Health {
	live, _ := liveReports(spec, s)
	cfg := currentConfig(spec, s, live)
	if cfg == nil {
		return executor.HealthFailed
	}
	healthy, primary := 0, false
	for _, m := range cfg.Members {
		i := slices.IndexFunc(live, func(r NodeReport) bool { return r.Node == m.Node })
		if i < 0 {
			continue
		}
		switch live[i].RSState {
		case RSPrimary:
			primary = true
			healthy++
		case RSSecondary:
			healthy++
		}
	}
	switch {
	case primary && healthy == len(cfg.Members):
		return executor.HealthReady
	case primary && 2*healthy > len(cfg.Members):
		return executor.HealthDegraded
	default:
		return executor.HealthFailed
	}
}
