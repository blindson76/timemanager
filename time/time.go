package time

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"example.com/mystate/executor"
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

	ClusterStateManualStarting       ClusterState = "ManualStarting"
	ClusterStateManualServerStarting ClusterState = "ManualServerStarting"
	ClusterStateManualPeersStarting  ClusterState = "ManualPeersStarting"
	ClusterStateServerStarting       ClusterState = "ServerStarting"
	ClusterStateServerRunning        ClusterState = "ServerRunning"

	ClusterStateAutoStarting ClusterState = "AutoStarting"
)

type Event string

const (
	EventReport      Event = "Report"
	EventOrderReport Event = "OrderReport"
)

type PeerState string

const (
	PeerStateIdle   PeerState = "Reporting"
	PeerStateActive PeerState = "Active"
)

type Action string

const (
	ActionManualStart Action = "ManualStart"
	ActionServerStart Action = "ServerStart"
	ActionServerStop  Action = "ServerStop"
	ActionClientStart Action = "ClientStart"
	ActionClientStop  Action = "ClientStop"
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
type OrderStatus string

const (
	OrderStatusPending  OrderStatus = "Pending"
	OrderStatusApplied  OrderStatus = "Applied"
	OrderStatusRejected OrderStatus = "Rejected"
	OrderStatusFailed   OrderStatus = "Failed"
)

type Order struct {
	OrderNumber int    `json:"order_number"`
	IssuedTime  int64  `json:"issued_time"`
	Action      Action `json:"action"`
	Payload     any    `json:"payload"`
}
type OrderReport struct {
	PeerID      string      `json:"peer_id"`
	OrderNumber int         `json:"order_number"`
	Status      OrderStatus `json:"status"`
	Message     string      `json:"message,omitempty"`
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
	for peerID := range cs.Reports {
		cs.Decisions = &Decision{
			TimeMode:     cs.TimeMode,
			TimeDelta:    0,
			ManualServer: peerID,
		}
		break
	}
	return cs
}
func (cs ClusterStatus) buildRepors(ctx context.Context) error {

	return nil
}

type LogObserver struct {
	gstate.BaseObserver[ClusterState, Event, ClusterStatus]
} // <-- Eksik olan struct kapatma parantezi eklendi

func (*LogObserver) OnTransition(ctx context.Context, e *gstate.TransitionEvent[ClusterState, Event, ClusterStatus]) {
	log.Printf("[%s] %s --%s--> %s", e.ActorID, e.From, e.Event, e.To)
}
func (*LogObserver) OnInvokeCompleted(ctx context.Context, e *gstate.InvokeEvent[ClusterState, Event, ClusterStatus]) {
	log.Printf("[%s] Invoke Completed Event %+v", e.ActorID, e)
}
func (*LogObserver) OnInvokeStarted(ctx context.Context, e *gstate.InvokeEvent[ClusterState, Event, ClusterStatus]) {
	log.Printf("[%s] Invoke Started Event %+v", e.ActorID, e)
}
func (*LogObserver) OnEventDropped(ctx context.Context, e *gstate.EventNotice[ClusterState, Event, ClusterStatus]) {
	log.Printf("[%s] Event Dropped in state %+v", e.ActorID, e)
}
func (*LogObserver) OnStateEntered(ctx context.Context, e *gstate.StateEvent[ClusterState, Event, ClusterStatus]) {
	log.Printf("[%s] Entered state %s", e.ActorID, e.State)
}
func (*LogObserver) OnStateExited(ctx context.Context, e *gstate.StateEvent[ClusterState, Event, ClusterStatus]) {
	log.Printf("[%s] Exited state %s", e.ActorID, e.State)
}
func (*LogObserver) OnEventReceived(ctx context.Context, e *gstate.EventNotice[ClusterState, Event, ClusterStatus]) {
	log.Printf("[%s] Event received: %+v", e.ActorID, e)
}
// Health reports Ready once a time source has been decided and the peers
// have been ordered to follow it.
//
// TODO: tighten to "orders acknowledged" once agents report OrderReport.
func Health(states []ClusterState, _ ClusterStatus) executor.Health {
	for _, s := range states {
		switch s {
		case ClusterStateServerRunning, ClusterStateAutoStarting, ClusterStateManualPeersStarting:
			return executor.HealthReady
		}
	}
	return executor.HealthPending
}

// InitialStatus is the data a fresh cluster machine starts from.
func InitialStatus() ClusterStatus {
	return ClusterStatus{
		ClusterState: ClusterStateInitializing,
		TimeMode:     TimeModeLocal,
		Reports:      make(map[string]TimeReport),
		Orders:       make(map[string][]Order),
	}
}

func BuildMachine() *gstate.Machine[ClusterState, Event, ClusterStatus] {
	builder := gstate.New[ClusterState, Event, ClusterStatus]("ClusterStateMachine").
		Initial(ClusterStateRoot).
		State(ClusterStateRoot, func(sb *gstate.StateBuilder[ClusterState, Event, ClusterStatus]) {
			sb.Initial(ClusterStateInitializing)
			sb.Invoke(func(ctx context.Context, snap ClusterStatus, mutate func(func(ClusterStatus) ClusterStatus)) error {

				for {
					fmt.Printf("Inside invoke loop\n")
					time.Sleep(time.Second * 2)
				}
				return nil
			}, ClusterStateRoot, ClusterStateRoot)
			sb.On(EventReport).AssignWith(func(cs ClusterStatus, a any) ClusterStatus {
				fmt.Printf("Raw report data: %+v\n", a)
				var report TimeReport
				data, err := json.Marshal(a)
				if err != nil {
					return cs
				}
				err = json.Unmarshal(data, &report)
				if err != nil {
					return cs
				}
				cs.Reports[report.PeerID] = report
				return cs
			})
			sb.On(EventOrderReport).AssignWith(func(cs ClusterStatus, a any) ClusterStatus {
				e, ok := a.(OrderReport)
				if ok {
					if e.Status == OrderStatusApplied {
						for i, order := range cs.Orders[e.PeerID] {
							if order.OrderNumber == e.OrderNumber {
								cs.Orders[e.PeerID] = append(cs.Orders[e.PeerID][:i], cs.Orders[e.PeerID][i+1:]...)
								break
							}
						}
					}
				}
				return cs
			})
			sb.State(ClusterStateInitializing, func(sb *gstate.StateBuilder[ClusterState, Event, ClusterStatus]) {
				sb.Always().Guard(func(cs ClusterStatus) bool {
					return len(cs.Reports) >= 3
				}).GoTo(ClusterStateWindowing)
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
					return len(cs.Reports) >= 4
				}).GoTo(ClusterStateConfiguring)
				sb.After(3 * time.Second).GoTo(ClusterStateConfiguring)
			})
			sb.State(ClusterStateConfiguring, func(sb *gstate.StateBuilder[ClusterState, Event, ClusterStatus]) {
				sb.Entry(func(cs ClusterStatus) ClusterStatus {
					ncs := cs.buildDecision()
					fmt.Printf("Built decision: %+v\n", ncs.Decisions)
					return ncs
				})
				sb.Always().Guard(func(cs ClusterStatus) bool {
					return cs.Decisions.TimeMode == TimeModeLocal
				}).GoTo(ClusterStateManualPeersStarting)
				sb.Always().Guard(func(cs ClusterStatus) bool {
					return cs.Decisions.TimeMode == TimeModeAuto
				}).GoTo(ClusterStateAutoStarting)
			})
			sb.State(ClusterStateManualStarting, func(sb *gstate.StateBuilder[ClusterState, Event, ClusterStatus]) {
				sb.Initial(ClusterStateManualServerStarting)
				sb.Entry(func(cs ClusterStatus) ClusterStatus {
					fmt.Printf("Entered ManualStart\n")
					return cs
				})
				sb.State(ClusterStateManualServerStarting, func(sb *gstate.StateBuilder[ClusterState, Event, ClusterStatus]) {

					sb.Entry(func(cs ClusterStatus) ClusterStatus {
						cs.Orders[cs.Decisions.ManualServer] = append(cs.Orders[cs.Decisions.ManualServer], Order{
							OrderNumber: 0,
							IssuedTime:  time.Now().UnixMilli(),
							Action:      ActionManualStart,
						})
						return cs
					})
					sb.Always().Guard(func(cs ClusterStatus) bool {
						return len(cs.Orders[cs.Decisions.ManualServer]) == 0
					}).GoTo(ClusterStateManualPeersStarting)
				})
				sb.State(ClusterStateManualPeersStarting, func(sb *gstate.StateBuilder[ClusterState, Event, ClusterStatus]) {
					sb.Entry(func(cs ClusterStatus) ClusterStatus {
						for peerID := range cs.Reports {
							if peerID == cs.Decisions.ManualServer {
								continue
							}
							cs.Orders[peerID] = append(cs.Orders[peerID], Order{
								OrderNumber: 0,
								IssuedTime:  time.Now().UnixMilli(),
								Action:      ActionManualStart,
							})
						}
						return cs
					})
				})

			})

			sb.State(ClusterStateAutoStarting, func(sb *gstate.StateBuilder[ClusterState, Event, ClusterStatus]) {
				sb.Entry(func(cs ClusterStatus) ClusterStatus {
					fmt.Printf("Entered AutoStarted\n")
					return cs
				})

			})
		}).
		Build()
	return builder
}
func RunSM() {
	builder := BuildMachine()
	// statedef := builder.StateDefMap()
	// for id, def := range statedef {
	// 	for event, transitions := range def.Delayed {
	// 		fmt.Printf("  Event: %v, Transitions: %+v\n", event, transitions)
	// 	}
	// 	for event, transitions := range def.Transitions {
	// 		fmt.Printf("  Event: %v, Transitions: %+v\n", event, transitions)
	// 	}
	// 	fmt.Printf("State ID: %v, Definition: %+v\n", id, def)
	// }
	actor := gstate.Start(builder, ClusterStatus{
		ClusterState: ClusterStateInitializing,
		TimeMode:     TimeModeLocal,
		Reports:      make(map[string]TimeReport),
		Orders:       make(map[string][]Order),
	}, true, builder.WithRunToCompletion(), builder.WithObservers(&LogObserver{}))
	fmt.Printf("Actor started: %v\n", actor.States())
	actor.SendWith(EventReport, TimeReport{
		PeerID:            "peer1",
		OrderNumber:       1,
		TimeMode:          TimeModeAuto,
		TimeDelta:         0,
		AvailableServsers: []string{"server1", "server2"},
		OrderApplied:      false,
	})
	// fmt.Printf("Actor state after first report: %v\n", actor.States())
	actor.SendWith(EventReport, TimeReport{
		PeerID:            "peer2",
		OrderNumber:       1,
		TimeMode:          TimeModeAuto,
		TimeDelta:         0,
		AvailableServsers: []string{"server1", "server2"},
		OrderApplied:      false,
	})
	actor.SendWith(EventReport, TimeReport{
		PeerID:            "peer3",
		OrderNumber:       1,
		TimeMode:          TimeModeAuto,
		TimeDelta:         0,
		AvailableServsers: []string{"server1", "server2"},
		OrderApplied:      false,
	})

	time.Sleep(5 * time.Second)
	actor.SendWith(EventOrderReport, OrderReport{
		PeerID: "peer1",
		Status: OrderStatusApplied,
	})
	fmt.Printf("Actor state after second report: %v\n", actor.States())

	// fmt.Printf("%+v", actor.Snapshot())
	time.Sleep(10 * time.Second)
	fmt.Printf("Actor state after second report: %v\n", actor.States())

}
