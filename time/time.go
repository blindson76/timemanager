package time

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/blindson76/gstate"
)

type TimeMode string

const (
	TimeModeAuto  TimeMode = "Auto"
	TimeModeLocal TimeMode = "Local"
)

type ClusterState string

const (
	ClusterStateRoot           ClusterState = "Root"
	ClusterStateInitializing   ClusterState = "Initializing"
	ClusterStateWindowing      ClusterState = "Windowing"
	ClusterStateConfiguring    ClusterState = "Configuring"
	ClusterStateConstructing   ClusterState = "Constructing"
	ClusterStateManualStarting ClusterState = "ManualStarting"
	ClusterStateServerStarting ClusterState = "ServerStarting"
	ClusterStateServerRunning  ClusterState = "ServerRunning"
)

type Event string

const (
	EventTypeReport Event = "Report"
)

type PeerState string

const (
	PeerStateIdle   PeerState = "Reporting"
	PeerStateActive PeerState = "Active"
)

type TimeReport struct {
	PeerID            string   `json:"peer_id"`
	OrderNumber       int      `json:"order_number"`
	TimeMode          TimeMode `json:"time_mode"`
	TimeDelta         int      `json:"time_delta"`
	AvailableServsers []string `json:"available_servers"`
	OrderApplied      bool     `json:"order_applied"`
}
type PeerStatus struct {
	PeerID string `json:"peer_id"`
}
type Order struct {
	OrderNumber int    `json:"order_number"`
	IssuedTime  int    `json:"issued_time"`
	Action      string `json:"action"`
	Payload     any    `json:"payload"`
}
type Decision struct {
	TimeMode     TimeMode `json:"time_mode"`
	TimeDelta    int      `json:"time_delta"`
	ManualServer string   `json:"manual_server,omitempty"`
}
type StateTimers struct {
	Duration int64 `json:"duration"`
}
type ClusterStatus struct {
	ClusterState       ClusterState           `json:"cluster_state"`
	TimeMode           TimeMode               `json:"time_mode"`
	Reports            map[string]TimeReport  `json:"reports"`
	Orders             map[string][]Order     `json:"orders"`
	Decisions          *Decision              `json:"decisions,omitempty"`
	WindowingStartTime int64                  `json:"windowing_start_time"`
	Timers             map[ClusterState]int64 `json:"timers"`
}

func (cs ClusterStatus) Clone() ClusterStatus {
	bytes, _ := json.Marshal(cs)
	var clone ClusterStatus
	json.Unmarshal(bytes, &clone)
	return clone
}
func (cs ClusterStatus) buildDecision() ClusterStatus {
	// Implement the logic to build a decision based on the current cluster status.
	// This is a placeholder implementation.
	cs.Decisions = &Decision{
		TimeMode:  cs.TimeMode,
		TimeDelta: 0,
	}
	return cs
}
func BuildMachine() {
	fmt.Println("Building state machine...")
	builder := gstate.New[ClusterState, Event, ClusterStatus]("ClusterStateMachine").
		Initial(ClusterStateRoot).
		State(ClusterStateRoot, func(sb *gstate.StateBuilder[ClusterState, Event, ClusterStatus]) {
			sb.Initial(ClusterStateInitializing)
			sb.On(EventTypeReport).AssignWith(func(cs ClusterStatus, a any) ClusterStatus {
				report := a.(TimeReport)
				cs.Reports[report.PeerID] = report
				return cs
			})
			sb.State(ClusterStateInitializing, func(sb *gstate.StateBuilder[ClusterState, Event, ClusterStatus]) {
				sb.Always().Guard(func(cs ClusterStatus) bool {
					return len(cs.Reports) >= 3
				}).GoTo(ClusterStateWindowing)
				sb.After(3 * time.Second).GoTo(ClusterStateWindowing)
			})

			sb.State(ClusterStateWindowing, func(sb *gstate.StateBuilder[ClusterState, Event, ClusterStatus]) {
				sb.Entry(func(cs ClusterStatus) ClusterStatus {
					cs.WindowingStartTime = int64(time.Now().Unix())
					return cs
				})
				sb.Exit(func(cs ClusterStatus) ClusterStatus {
					cs.WindowingStartTime = 0
					return cs
				})
				sb.Always().Guard(func(cs ClusterStatus) bool {
					return len(cs.Reports) >= 6
				}).GoTo(ClusterStateConfiguring)
			})
			sb.State(ClusterStateConfiguring, func(sb *gstate.StateBuilder[ClusterState, Event, ClusterStatus]) {
				sb.Entry(func(cs ClusterStatus) ClusterStatus {
					return cs.buildDecision()
				})
				sb.Always().Guard(func(cs ClusterStatus) bool {
					return cs.Decisions.TimeMode == TimeModeAuto
				}).GoTo(ClusterStateManualStarting)
			})
			sb.State(ClusterStateManualStarting, func(sb *gstate.StateBuilder[ClusterState, Event, ClusterStatus]) {
				sb.Entry(func(cs ClusterStatus) ClusterStatus {
					cs.Orders[cs.Decisions.ManualServer] = append(cs.Orders[a.(TimeReport).PeerID], Order{
						OrderNumber:  a.(TimeReport).OrderNumber,
						TimeMode:     a.(TimeReport).TimeMode,
						TimeDelta:    a.(TimeReport).TimeDelta,
						OrderApplied: a.(TimeReport).OrderApplied,
					})
					return cs
				})

			})
		}).
		Build()
	statedef := builder.StateDefMap()
	for id, def := range statedef {
		for event, transitions := range def.Delayed {
			fmt.Printf("  Event: %v, Transitions: %+v\n", event, transitions)
		}
		for event, transitions := range def.Transitions {
			fmt.Printf("  Event: %v, Transitions: %+v\n", event, transitions)
		}
		fmt.Printf("State ID: %v, Definition: %+v\n", id, def)
	}

	actor := gstate.Start(builder, ClusterStatus{
		ClusterState: ClusterStateInitializing,
		TimeMode:     TimeModeAuto,
		Reports:      make(map[string]TimeReport),
		Orders:       make(map[string]Order),
	}, builder.WithRunToCompletion())
	fmt.Printf("Actor started: %v\n", actor)
	actor.SendWith(EventTypeReport, TimeReport{
		PeerID:            "peer1",
		OrderNumber:       1,
		TimeMode:          TimeModeAuto,
		TimeDelta:         0,
		AvailableServsers: []string{"server1", "server2"},
		OrderApplied:      false,
	})
	fmt.Printf("Actor state after first report: %v\n", actor.States())
	actor.SendWith(EventTypeReport, TimeReport{
		PeerID:            "peer2",
		OrderNumber:       1,
		TimeMode:          TimeModeAuto,
		TimeDelta:         0,
		AvailableServsers: []string{"server1", "server2"},
		OrderApplied:      false,
	})
	fmt.Printf("Actor state after second report: %v\n", actor.States())

	fmt.Printf("%+v", actor.Snapshot())
	time.Sleep(1 * time.Minute)

}
