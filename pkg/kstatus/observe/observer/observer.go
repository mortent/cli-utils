package observer

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe/event"
	"sigs.k8s.io/cli-utils/pkg/kstatus/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type AggregatorFactoryFunc func(identifiers []wait.ResourceIdentifier) StatusAggregator
type ReaderFactoryFunc func(reader client.Reader, mapper meta.RESTMapper, identifiers []wait.ResourceIdentifier) (ObserverReader, error)
type ObserversFactoryFunc func(reader ObserverReader, mapper meta.RESTMapper) (resourceObservers map[schema.GroupKind]ResourceObserver, defaultObserver ResourceObserver)

// Observer provides functionality for polling a cluster for status for a set of resources.
type Observer struct {
	Reader client.Reader
	Mapper meta.RESTMapper

	AggregatorFactoryFunc AggregatorFactoryFunc
	ReaderFactoryFunc     ReaderFactoryFunc
	ObserversFactoryFunc  ObserversFactoryFunc
}

// Observe will create a new StatusObserverRunner that will poll all the resources provided and report their status
// back on the event channel returned. The StatusObserverRunner can be cancelled at any time by cancelling the
// context passed in.
// If stopOnCompleted is set to true, then the runner will stop observing the resources when the StatusAggregator
// determines that all resources has been fully reconciled. If this is set to false, the observer will keep running
// until cancelled.
// If useCache is set to true, the observer will fetch all resources needed using LIST calls before each polling
// cycle. The observers responsible for computing status will rely on the cached data. This can dramatically reduce
// the number of calls against the API server.
func (s *Observer) Observe(ctx context.Context, identifiers []wait.ResourceIdentifier, pollInterval time.Duration, stopOnCompleted bool) <-chan event.Event {
	eventChannel := make(chan event.Event)

	go func() {
		defer close(eventChannel)

		observerReader, err := s.ReaderFactoryFunc(s.Reader, s.Mapper, identifiers)
		if err != nil {
			eventChannel <- event.Event{
				EventType: event.ErrorEvent,
				Error:     err,
			}
			return
		}
		observers, defaultObserver := s.ObserversFactoryFunc(observerReader, s.Mapper)
		aggregator := s.AggregatorFactoryFunc(identifiers)

		runner := &StatusObserverRunner{
			ctx:                       ctx,
			reader:                    observerReader,
			observers:                 observers,
			defaultObserver:           defaultObserver,
			identifiers:               identifiers,
			previousObservedResources: make(map[wait.ResourceIdentifier]*event.ObservedResource),
			eventChannel:              eventChannel,
			statusAggregator:          aggregator,
			stopOnCompleted:           stopOnCompleted,
			pollingInterval:           pollInterval,
		}
		runner.Run()
	}()

	return eventChannel
}

// StatusObserverRunner is responsible for polling of a set of resources. Each call to Observe will create
// a new StatusObserverRunner, which means we can keep state in the runner and all data will only be accessed
// by a single goroutine, meaning we don't need synchronization.
// The StatusObserverRunner uses an implementation of the ObserverReader interface to talk to the
// kubernetes cluster. Currently this can be either the cached ObserverReader that syncs all needed resources
// with LIST calls before each polling loop, or the normal ObserverReader that just forwards each call
// to the client.Reader from controller-runtime.
type StatusObserverRunner struct {
	ctx context.Context

	reader ObserverReader

	observers map[schema.GroupKind]ResourceObserver

	defaultObserver ResourceObserver

	identifiers []wait.ResourceIdentifier

	previousObservedResources map[wait.ResourceIdentifier]*event.ObservedResource

	eventChannel chan event.Event

	statusAggregator StatusAggregator

	stopOnCompleted bool

	pollingInterval time.Duration
}

// Run starts the polling loop of the observers. This function is meant to be running in its
// own goroutine.
func (r *StatusObserverRunner) Run() {
	// Sets up ticker that will trigger the regular polling loop at a regular interval.
	ticker := time.NewTicker(r.pollingInterval)
	defer func() {
		ticker.Stop()
	}()

	for {
		select {
		case <-r.ctx.Done():
			// If the context has been cancelled, just send an AbortedEvent
			// and pass along the most up-to-date aggregate status. Then return
			// from this function, which will stop the ticker and close the event channel.
			aggregatedStatus := r.statusAggregator.AggregateStatus()
			r.eventChannel <- event.Event{
				EventType:       event.AbortedEvent,
				AggregateStatus: aggregatedStatus,
			}
			return
		case <-ticker.C:
			// First trigger a sync of the ObserverReader. This may or may not actually
			// result in calls to the cluster, depending on the implementation.
			// If this call fails, there is no clean way to recover, so we just return an ErrorEvent
			// and shut down.
			err := r.reader.Sync(r.ctx)
			if err != nil {
				r.eventChannel <- event.Event{
					EventType: event.ErrorEvent,
					Error:     err,
				}
				return
			}
			// Poll all resources and compute status. If the polling of resources has completed (based
			// on information from the StatusAggregator and the value of stopOnCompleted), we send
			// a CompletedEvent and return.
			completed := r.observeStatusForAllResources()
			if completed {
				aggregatedStatus := r.statusAggregator.AggregateStatus()
				r.eventChannel <- event.Event{
					EventType:       event.CompletedEvent,
					AggregateStatus: aggregatedStatus,
				}
				return
			}
		}
	}
}

// observeStatusForAllResources iterates over all the resources in the set and delegates
// to the appropriate observer to compute the status.
func (r *StatusObserverRunner) observeStatusForAllResources() bool {
	for _, id := range r.identifiers {
		gk := id.GroupKind
		observer := r.observerForGroupKind(gk)
		observedResource := observer.Observe(r.ctx, id)
		r.statusAggregator.ResourceObserved(observedResource)
		if r.isUpdatedObservedResource(observedResource) {
			r.previousObservedResources[id] = observedResource
			aggregatedStatus := r.statusAggregator.AggregateStatus()
			r.eventChannel <- event.Event{
				EventType:       event.ResourceUpdateEvent,
				AggregateStatus: aggregatedStatus,
				Resource:        observedResource,
			}
			if r.statusAggregator.Completed() && r.stopOnCompleted {
				return true
			}
		}
	}
	if r.statusAggregator.Completed() && r.stopOnCompleted {
		return true
	}
	return false
}

func (r *StatusObserverRunner) observerForGroupKind(gk schema.GroupKind) ResourceObserver {
	observer, ok := r.observers[gk]
	if !ok {
		return r.defaultObserver
	}
	return observer
}

func (r *StatusObserverRunner) isUpdatedObservedResource(observedResource *event.ObservedResource) bool {
	oldObservedResource, found := r.previousObservedResources[observedResource.Identifier]
	if !found {
		return true
	}
	return !event.DeepEqual(observedResource, oldObservedResource)
}
