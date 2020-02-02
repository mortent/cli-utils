package collector

import (
	"sort"
	"sync"

	"sigs.k8s.io/cli-utils/pkg/kstatus/observe"
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe/common"
	"sigs.k8s.io/cli-utils/pkg/kstatus/status"
	"sigs.k8s.io/cli-utils/pkg/kstatus/wait"
)

func NewObservedStatusCollector(identifiers []wait.ResourceIdentifier) *ObservedStatusCollector {
	observations := make(map[wait.ResourceIdentifier]*common.ObservedResource)
	for _, id := range identifiers {
		observations[id] = &common.ObservedResource{
			Identifier: id,
			Status:     status.UnknownStatus,
		}
	}
	return &ObservedStatusCollector{
		aggregateStatus: status.UnknownStatus,
		observations:    observations,
	}
}

// ObservedStatusCollector is for use by clients of the observe library and provides
// a way to keep track of the latest status/state for all the observed resources. The collector
// is set up to listen to the eventChannel and keep the latest event for each resource. It also
// provides a way to fetch the latest state for all resources and the aggregated status at any point.
// The functions already handles synchronization so it can be used by multiple goroutines.
type ObservedStatusCollector struct {
	mux sync.RWMutex

	lastEventType observe.EventType

	aggregateStatus status.Status

	observations map[wait.ResourceIdentifier]*common.ObservedResource

	error error
}

// Observe kicks off the goroutine that will listen for the events on the eventChannel. It is also
// provided a stop channel that can be used by the caller to stop the collector before the
// eventChannel is closed. It returns a channel that will be closed the collector stops listening to
// the eventChannel.
func (o *ObservedStatusCollector) Observe(eventChannel <-chan observe.Event, stop <-chan struct{}) <-chan struct{} {
	completed := make(chan struct{})
	go func() {
		defer close(completed)
		for {
			select {
			case <-stop:
				return
			case event, more := <-eventChannel:
				if !more {
					return
				}
				o.processEvent(event)
			}
		}
	}()
	return completed
}

func (o *ObservedStatusCollector) processEvent(event observe.Event) {
	o.mux.Lock()
	defer o.mux.Unlock()
	o.lastEventType = event.EventType
	if event.EventType == observe.ErrorEvent {
		o.error = event.Error
		return
	}
	o.aggregateStatus = event.AggregateStatus
	if event.EventType == observe.ResourceUpdateEvent {
		observedResource := event.Resource
		o.observations[observedResource.Identifier] = observedResource
	}
}

// Observation contains the latest state known by the collector as returned
// by a call to the LatestObservation function.
type Observation struct {
	LastEventType observe.EventType

	AggregateStatus status.Status

	ObservedResources []*common.ObservedResource

	Error error
}

// LatestObservation returns an Observation instance, which contains the
// latest information about the resources known by the collector.
func (o *ObservedStatusCollector) LatestObservation() *Observation {
	o.mux.RLock()
	defer o.mux.RUnlock()

	var observedResources common.ObservedResources
	for _, observedResource := range o.observations {
		observedResources = append(observedResources, observedResource)
	}
	sort.Sort(observedResources)

	return &Observation{
		LastEventType:     o.lastEventType,
		AggregateStatus:   o.aggregateStatus,
		ObservedResources: observedResources,
		Error:             o.error,
	}
}
