// Package service is a generic lifecycle controller for a service that must
// not start before its dependencies are ready.
//
//	WaitingDeps --all deps Ready--> Starting --Started--> Running
//	                                   |                    |
//	                     Failed / start timeout          Failed
//	                                   v                    |
//	                                 Failed <---------------+
//	                                   |
//	                        after retry delay --> WaitingDeps
//
// Dependencies gate starting only. Losing a dependency while Running does
// not stop the service; it reports [executor.HealthDegraded] until the
// dependency recovers. Started and Failed are reported by the node agents.
package service

import (
	"context"
	"log"
	"slices"
	"time"

	"example.com/mystate/executor"
	"github.com/blindson76/gstate"
)

type State string

const (
	StateRoot        State = "Root"
	StateWaitingDeps State = "WaitingDeps"
	StateStarting    State = "Starting"
	StateRunning     State = "Running"
	StateFailed      State = "Failed"
)

type Event string

const (
	// EventDependency carries an [executor.DependencyChange].
	EventDependency Event = "Dependency"
	// EventStarted is reported once the service is up.
	EventStarted Event = "Started"
	// EventFailed is reported when the service fails; args are [Failure].
	EventFailed Event = "Failed"
)

// Failure is the args of [EventFailed].
type Failure struct {
	Reason string `json:"reason"`
}

// Status is the replicated data of one service.
type Status struct {
	Deps      map[string]executor.Health `json:"deps"`
	Attempts  int                        `json:"attempts"`
	LastError string                     `json:"last_error,omitempty"`
}

func (s Status) Clone() Status {
	c := s
	c.Deps = make(map[string]executor.Health, len(s.Deps))
	for k, v := range s.Deps {
		c.Deps[k] = v
	}
	return c
}

// LogObserver logs state changes of a service, skipping data-only updates
// such as dependency events.
type LogObserver struct {
	gstate.BaseObserver[State, Event, Status]
	Name string
}

func (o *LogObserver) OnTransition(_ context.Context, e *gstate.TransitionEvent[State, Event, Status]) {
	if e.To != "" {
		log.Printf("[%s] %s --%s--> %s", o.Name, e.From, e.Event, e.To)
	}
}

// Spec configures one service controller.
type Spec struct {
	Name         string
	DependsOn    []string
	StartTimeout time.Duration
	RetryDelay   time.Duration
}

// New builds the controller for spec, with optional observers on its actor.
func New(spec Spec, observers ...gstate.Observer[State, Event, Status]) *executor.Ctrl[State, Event, Status] {
	depsReady := func(s Status) bool {
		for _, dep := range spec.DependsOn {
			if s.Deps[dep] != executor.HealthReady {
				return false
			}
		}
		return true
	}
	onFailed := func(s Status, a any) Status {
		if f, err := executor.DecodeArgs[Failure](a); err == nil {
			s.LastError = f.Reason
		}
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

			sb.State(StateWaitingDeps, func(sb *gstate.StateBuilder[State, Event, Status]) {
				sb.Always().Guard(depsReady).GoTo(StateStarting)
			})
			sb.State(StateStarting, func(sb *gstate.StateBuilder[State, Event, Status]) {
				sb.Entry(func(s Status) Status {
					s.Attempts++
					s.LastError = ""
					return s
				})
				sb.On(EventStarted).GoTo(StateRunning)
				sb.On(EventFailed).AssignWith(onFailed).GoTo(StateFailed)
				sb.After(spec.StartTimeout).GoTo(StateFailed)
			})
			sb.State(StateRunning, func(sb *gstate.StateBuilder[State, Event, Status]) {
				sb.Entry(func(s Status) Status {
					s.Attempts = 0
					return s
				})
				sb.On(EventFailed).AssignWith(onFailed).GoTo(StateFailed)
			})
			sb.State(StateFailed, func(sb *gstate.StateBuilder[State, Event, Status]) {
				sb.After(spec.RetryDelay).GoTo(StateWaitingDeps)
			})
		}).
		Build()

	c := executor.NewController(spec.Name, m, Status{Deps: map[string]executor.Health{}}).
		WithHealth(func(states []State, s Status) executor.Health {
			switch {
			case slices.Contains(states, StateRunning):
				if depsReady(s) {
					return executor.HealthReady
				}
				return executor.HealthDegraded
			case slices.Contains(states, StateFailed):
				return executor.HealthFailed
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
