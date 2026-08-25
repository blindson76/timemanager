package time

import (
	"encoding/json"
	"fmt"

	"github.com/blindson76/gstate"
)

type TimeMode string

const (
	TimeModeAuto  TimeMode = "Auto"
	TimeModeLocal TimeMode = "Local"
)

type ClusterState string

const (
	ClusterStateRoot         ClusterState = "Root"
	ClusterStateInitializing ClusterState = "Initializing"
	ClusterStateWindowing    ClusterState = "Windowing"
	ClusterStateConfiguring  ClusterState = "Configuring"
	ClusterStateConstructing ClusterState = "Constructing"
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
	TimeMode  TimeMode `json:"time_mode"`
	TimeDelta int      `json:"time_delta"`
}

type ClusterStatus struct {
	ClusterState ClusterState          `json:"cluster_state"`
	TimeMode     TimeMode              `json:"time_mode"`
	Reports      map[string]TimeReport `json:"reports"`
	Orders       map[string]Order      `json:"orders"`
	Decisions    Decision              `json:"decisions"`
}

func (cs ClusterStatus) Clone() ClusterStatus {
	bytes, _ := json.Marshal(cs)
	var clone ClusterStatus
	json.Unmarshal(bytes, &clone)
	return clone
}

func BuildMachine() {
	fmt.Println("Building state machine...")
	builder := gstate.New[ClusterState, Event, ClusterStatus]("ClusterStateMachine").
		Initial(ClusterStateRoot).
		State(ClusterStateRoot, func(sb *gstate.StateBuilder[ClusterState, Event, ClusterStatus]) {
			sb.Initial(ClusterStateInitializing)
			sb.On(EventTypeReport).AssignWith(func(cs ClusterStatus, a any) ClusterStatus {
				fmt.Printf("Received report: %v\n", a)
				report := a.(TimeReport)
				cs.Reports[report.PeerID] = report
				fmt.Printf("Updated cluster status: %v\n", cs)
				return cs
			})
			sb.State(ClusterStateInitializing, func(sb *gstate.StateBuilder[ClusterState, Event, ClusterStatus]) {

			})

			sb.State(ClusterStateWindowing, func(sb *gstate.StateBuilder[ClusterState, Event, ClusterStatus]) {})
			sb.State(ClusterStateConfiguring, func(sb *gstate.StateBuilder[ClusterState, Event, ClusterStatus]) {})
			sb.State(ClusterStateConstructing, func(sb *gstate.StateBuilder[ClusterState, Event, ClusterStatus]) {})
		}).
		Build()

	actor := gstate.Start(builder, ClusterStatus{
		ClusterState: ClusterStateInitializing,
		TimeMode:     TimeModeAuto,
		Reports:      make(map[string]TimeReport),
		Orders:       make(map[string]Order),
		Decisions:    Decision{TimeMode: TimeModeAuto, TimeDelta: 0},
	})
	actor.SendWith(EventTypeReport, TimeReport{
		PeerID:            "peer1",
		OrderNumber:       1,
		TimeMode:          TimeModeAuto,
		TimeDelta:         0,
		AvailableServsers: []string{"server1", "server2"},
		OrderApplied:      false,
	})
	actor.SendWith(EventTypeReport, TimeReport{
		PeerID:            "peer2",
		OrderNumber:       1,
		TimeMode:          TimeModeAuto,
		TimeDelta:         0,
		AvailableServsers: []string{"server1", "server2"},
		OrderApplied:      false,
	})

	fmt.Printf("%v", actor.States())
	select {}

}
