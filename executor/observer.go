package executor

import (
	"context"
	"log"

	"github.com/blindson76/gstate"
)

// TransitionLogger logs a controller's state changes, skipping data-only
// updates (transitions without a target).
type TransitionLogger[S ~string, E ~string, D gstate.Cloner[D]] struct {
	gstate.BaseObserver[S, E, D]
	Name string
}

func (o *TransitionLogger[S, E, D]) OnTransition(_ context.Context, e *gstate.TransitionEvent[S, E, D]) {
	if e.To != "" {
		log.Printf("[%s] %s --%s--> %s", o.Name, e.From, e.Event, e.To)
	}
}
