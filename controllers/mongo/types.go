package mongo

import (
	"fmt"
	"slices"
	"time"

	"example.com/mystate/executor"
)

// Timestamp is a MongoDB BSON timestamp (seconds, increment).
type Timestamp struct {
	T uint32 `json:"t"`
	I uint32 `json:"i"`
}

func (ts Timestamp) String() string { return fmt.Sprintf("Timestamp(%d, %d)", ts.T, ts.I) }

// Less reports whether ts is before o.
func (ts Timestamp) Less(o Timestamp) bool {
	return ts.T < o.T || (ts.T == o.T && ts.I < o.I)
}

// OpTime identifies an oplog entry. OpTimes order by term, then timestamp,
// as in replication protocol version 1.
type OpTime struct {
	TS   Timestamp `json:"ts"`
	Term int64     `json:"t"`
}

func (ot OpTime) String() string { return fmt.Sprintf("{ts: %v, t: %d}", ot.TS, ot.Term) }

// Less reports whether ot is before o.
func (ot OpTime) Less(o OpTime) bool {
	if ot.Term != o.Term {
		return ot.Term < o.Term
	}
	return ot.TS.Less(o.TS)
}

// Member is one replica set member. ID is the member's _id in the replica
// set config and is kept stable across reconfigs.
type Member struct {
	ID   int    `json:"_id"`
	Node string `json:"node"`
	Host string `json:"host"`
}

// ReplConfig is a replica set configuration, as read from
// local.system.replset or as ordered by the controller.
type ReplConfig struct {
	SetName string   `json:"set_name"`
	Version int      `json:"version"`
	Term    int64    `json:"term"`
	Members []Member `json:"members"`
}

// newer reports whether c supersedes o.
func (c *ReplConfig) newer(o *ReplConfig) bool {
	if o == nil {
		return c != nil
	}
	if c == nil {
		return false
	}
	if c.Term != o.Term {
		return c.Term > o.Term
	}
	return c.Version > o.Version
}

func (c *ReplConfig) nodes() []string {
	if c == nil {
		return nil
	}
	out := make([]string, len(c.Members))
	for i, m := range c.Members {
		out[i] = m.Node
	}
	slices.Sort(out)
	return out
}

func (c *ReplConfig) clone() *ReplConfig {
	if c == nil {
		return nil
	}
	cp := *c
	cp.Members = slices.Clone(c.Members)
	return &cp
}

// Member states as reported by replSetGetStatus.
const (
	RSPrimary    = "PRIMARY"
	RSSecondary  = "SECONDARY"
	RSStartup2   = "STARTUP2" // initial sync in progress
	RSRecovering = "RECOVERING"
)

// NodeReport is what a node's agent observes about its local mongod. Agents
// send it as the args of [EventReport] in reply to an inspect order, after
// executing any order, and whenever it changes.
type NodeReport struct {
	Node    string `json:"node"`
	Host    string `json:"host"`    // host:port other members use to reach this mongod
	Running bool   `json:"running"` // mongod is up with --replSet
	// Empty is true when the data directory holds no oplog and no user data.
	Empty       bool      `json:"empty"`
	OplogOldest Timestamp `json:"oplog_oldest"`
	OplogNewest OpTime    `json:"oplog_newest"`
	// Config is the node's local replica set config, nil if it has none.
	Config  *ReplConfig `json:"config,omitempty"`
	RSState string      `json:"rs_state,omitempty"`
	Error   string      `json:"error,omitempty"`
}

func (r NodeReport) clone() NodeReport {
	r.Config = r.Config.clone()
	return r
}

// hasData reports whether the node holds data that replication must account for.
func (r NodeReport) hasData() bool { return !r.Empty && r.RSState != RSStartup2 }

// Order actions carried out by node agents.
const (
	// ActionInspect: ensure mongod runs with --replSet, then send a report.
	ActionInspect = "inspect"
	// ActionWipe: stop mongod, delete the data directory, start it empty.
	ActionWipe = "wipe"
	// ActionInitiate: run replSetInitiate with Config.
	ActionInitiate = "initiate"
	// ActionReconfig: run replSetReconfig with Config (force when Force).
	ActionReconfig = "reconfig"
)

// Order is an instruction to one node's agent. Agents execute their pending
// orders (idempotently) and acknowledge each with [EventOrderDone].
type Order struct {
	ID     int         `json:"id"`
	Node   string      `json:"node"`
	Action string      `json:"action"`
	Config *ReplConfig `json:"config,omitempty"`
	Force  bool        `json:"force,omitempty"`
}

// OrderResult is the args of [EventOrderDone].
type OrderResult struct {
	ID    int    `json:"id"`
	Error string `json:"error,omitempty"`
}

// Plan is one reconciliation step, computed deterministically from the
// replicated status by [ComputePlan].
type Plan struct {
	// Wipe lists nodes whose data must be deleted before they can rejoin.
	// Wipes are executed before any config change.
	Wipe []string `json:"wipe,omitempty"`
	// Action is ActionInitiate, ActionReconfig, or "" for none.
	Action string      `json:"action,omitempty"`
	Target string      `json:"target,omitempty"`
	Config *ReplConfig `json:"config,omitempty"`
	Force  bool        `json:"force,omitempty"`
	// Blocked explains why no safe step exists; the controller waits.
	Blocked string `json:"blocked,omitempty"`
	// Reasons explains each wipe, by node.
	Reasons map[string]string `json:"reasons,omitempty"`
}

// Noop reports whether the plan requires no action.
func (p Plan) Noop() bool { return p.Blocked == "" && len(p.Wipe) == 0 && p.Action == "" }

func (p Plan) clone() Plan {
	p.Wipe = slices.Clone(p.Wipe)
	p.Config = p.Config.clone()
	if p.Reasons != nil {
		r := make(map[string]string, len(p.Reasons))
		for k, v := range p.Reasons {
			r[k] = v
		}
		p.Reasons = r
	}
	return p
}

// Status is the replicated data of the mongo controller.
type Status struct {
	Deps  map[string]executor.Health `json:"deps"`
	Nodes map[string]NodeReport      `json:"nodes"`
	// Applied is the last config this controller successfully ordered.
	Applied     *ReplConfig `json:"applied,omitempty"`
	Orders      []Order     `json:"orders"`
	NextOrderID int         `json:"next_order_id"`
	// Plan is the step being executed; PlanOrders are the orders issued for
	// it and Results their outcomes (error text, "" for success).
	Plan       Plan           `json:"plan"`
	PlanOrders []int          `json:"plan_orders,omitempty"`
	Results    map[int]string `json:"results,omitempty"`
	LastError  string         `json:"last_error,omitempty"`
}

func (s Status) Clone() Status {
	c := s
	c.Deps = make(map[string]executor.Health, len(s.Deps))
	for k, v := range s.Deps {
		c.Deps[k] = v
	}
	c.Nodes = make(map[string]NodeReport, len(s.Nodes))
	for k, v := range s.Nodes {
		c.Nodes[k] = v.clone()
	}
	c.Applied = s.Applied.clone()
	c.Orders = make([]Order, len(s.Orders))
	for i, o := range s.Orders {
		o.Config = o.Config.clone()
		c.Orders[i] = o
	}
	c.Plan = s.Plan.clone()
	c.PlanOrders = slices.Clone(s.PlanOrders)
	c.Results = make(map[int]string, len(s.Results))
	for k, v := range s.Results {
		c.Results[k] = v
	}
	return c
}

// OrdersFor returns the pending orders addressed to node, oldest first.
func (s Status) OrdersFor(node string) []Order {
	var out []Order
	for _, o := range s.Orders {
		if o.Node == node {
			out = append(out, o)
		}
	}
	return out
}

// Spec configures the mongo controller.
type Spec struct {
	Name       string
	DependsOn  []string
	ReplicaSet string
	// Members are the cluster nodes eligible to host a replica set member.
	Members []string
	// Replicas is the desired number of voting members (1..7).
	Replicas int
	// ReportWindow is how long discovery waits for every eligible node to
	// report before deciding with a majority.
	ReportWindow time.Duration
	// OrderTimeout bounds discovery and each wipe/config step.
	OrderTimeout time.Duration
	RetryDelay   time.Duration
	// AllowForceReconfig permits a forced reconfig when a majority of the
	// current members is unreachable. It can cause rollback of writes that
	// only reached the lost members; leave it off unless that is acceptable.
	AllowForceReconfig bool
}
