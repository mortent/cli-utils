package observe

import (
	"context"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe/aggregator"
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe/common"
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe/observers"
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe/reader"
	"sigs.k8s.io/cli-utils/pkg/kstatus/status"
	"sigs.k8s.io/cli-utils/pkg/kstatus/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// StatusAggregator provides the interface the observer uses to compute the aggregate status.
// It also include a function that will be used by the observer to determine if all resources
// should be considered fully reconciled.
type StatusAggregator interface {
	// ResourceObserved notifies the aggregator of a new observation. Called after status has been
	// computed.
	ResourceObserved(resource *common.ObservedResource)
	// AggregateStatus computes the aggregate status for all the resources at the given
	// point in time.
	AggregateStatus() status.Status
	// Completed returns true if all resources should be considered reconciled and false otherwise.
	Completed() bool
}

// NewStatusObserver creates a new StatusObserver using the given reader and mapper. The StatusObserver
// will use the client for all calls to the cluster.
func NewStatusObserver(reader client.Reader, mapper meta.RESTMapper) *StatusObserver {
	return &StatusObserver{
		reader: reader,
		mapper: mapper,
	}
}

// StatusObserver provides functionality for polling a cluster for status for a set of resources.
type StatusObserver struct {
	reader client.Reader
	mapper meta.RESTMapper
}

// EventType is the type that describes the type of an Event that is passed back to the caller
// as resources in the cluster are being observed.
type EventType string

const (
	// ResourceUpdateEvent describes events related to a change in the status of one of the observed resources.
	ResourceUpdateEvent EventType = "ResourceUpdated"
	// CompletedEvent signals that all resources have been reconciled and the observer has completed its work. The
	// event channel will be closed after this event.
	CompletedEvent EventType = "Completed"
	// AbortedEvent signals that the observer is shutting down because it has been cancelled. All resources might
	// not have been reconciled. The event channel will be closed after this event.
	AbortedEvent EventType = "Aborted"
	// ErrorEvent signals that the observer has encountered an error that it can not recover from. The observer
	// is shutting down and the event channel will be closed after this event.
	ErrorEvent EventType = "Error"
)

// Event defines that type that is passed back through the event channel to notify the caller of changes
// as resources are being observed.
type Event struct {
	// EventType defines the type of event.
	EventType EventType

	// AggregateStatus is the collective status for all the resources. It is computed by the
	// StatusAggregator
	AggregateStatus status.Status

	// Resource is only available for ResourceUpdateEvents. It includes information about the observed resource,
	// including the resource status, any errors and the resource itself (as an unstructured).
	Resource *common.ObservedResource

	// Error is only available for ErrorEvents. It contains the error that caused the observer to
	// give up.
	Error error
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
func (s *StatusObserver) Observe(ctx context.Context, resources []wait.ResourceIdentifier, stopOnCompleted bool, useCache bool) <-chan Event {
	eventChannel := make(chan Event)

	var observerReader reader.ObserverReader
	if useCache {
		var err error
		observerReader, err = reader.NewCachedObserverReader(s.reader, s.mapper, resources)
		if err != nil {
			panic(err)
		}
	} else {
		observerReader = &reader.NoCacheObserverReader{
			Reader: s.reader,
		}
	}

	runner := newStatusObserverRunner(ctx, observerReader, s.mapper, resources, eventChannel, stopOnCompleted)
	go runner.Run()

	return eventChannel
}

// newStatusObserverRunner creates a new StatusObserverRunner.
func newStatusObserverRunner(ctx context.Context, reader reader.ObserverReader, mapper meta.RESTMapper, identifiers []wait.ResourceIdentifier,
	eventChannel chan Event, stopOnCompleted bool) *StatusObserverRunner {
	resourceObservers, defaultObserver := createObservers(reader, mapper)
	return &StatusObserverRunner{
		ctx:                       ctx,
		reader:                    reader,
		identifiers:               identifiers,
		observers:                 resourceObservers,
		defaultObserver:           defaultObserver,
		eventChannel:              eventChannel,
		previousObservedResources: make(map[wait.ResourceIdentifier]*common.ObservedResource),
		statusAggregator:          aggregator.NewAllCurrentOrNotFoundStatusAggregator(identifiers),
		stopOnCompleted:           stopOnCompleted,
	}
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

	reader reader.ObserverReader

	observers map[schema.GroupKind]observers.ResourceObserver

	defaultObserver observers.ResourceObserver

	identifiers []wait.ResourceIdentifier

	previousObservedResources map[wait.ResourceIdentifier]*common.ObservedResource

	eventChannel chan Event

	statusAggregator StatusAggregator

	stopOnCompleted bool
}

// Run starts the polling loop of the observers. This function is meant to be running in its
// own goroutine.
func (r *StatusObserverRunner) Run() {
	// Sets up ticker that will trigger the regular polling loop at a regular interval.
	ticker := time.NewTicker(2 * time.Second) // TODO(mortent): This should be configurable.
	defer func() {
		ticker.Stop()
		// This closes the eventChannel that was created in the Observe function of the
		// StatusObserver and included in the StatusObserverRunner that was created.
		close(r.eventChannel)
	}()

	for {
		select {
		case <-r.ctx.Done():
			// If the context has been cancelled, just send an AbortedEvent
			// and pass along the most up-to-date aggregate status. Then return
			// from this function, which will stop the ticker and close the event channel.
			aggregatedStatus := r.statusAggregator.AggregateStatus()
			r.eventChannel <- Event{
				EventType:       AbortedEvent,
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
				r.eventChannel <- Event{
					EventType: ErrorEvent,
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
				r.eventChannel <- Event{
					EventType:       CompletedEvent,
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
			r.eventChannel <- Event{
				EventType:       ResourceUpdateEvent,
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

func (r *StatusObserverRunner) observerForGroupKind(gk schema.GroupKind) observers.ResourceObserver {
	observer, ok := r.observers[gk]
	if !ok {
		return r.defaultObserver
	}
	return observer
}

func (r *StatusObserverRunner) isUpdatedObservedResource(observedResource *common.ObservedResource) bool {
	oldObservedResource, found := r.previousObservedResources[observedResource.Identifier]
	if !found {
		return true
	}
	return !DeepEqual(observedResource, oldObservedResource)
}

// createObservers creates an instance of all the observers. This includes a set of observers for
// a particular GroupKind, and a default observer used for all resource types that does not have
// a specific observers.
// TODO: We should consider making the registration more automatic instead of having to create each of them
// here. Also, it might be worth creating them on demand.
func createObservers(reader reader.ObserverReader, mapper meta.RESTMapper) (map[schema.GroupKind]observers.ResourceObserver, observers.ResourceObserver) {
	podObserver := observers.NewPodObserver(reader, mapper)
	replicaSetObserver := observers.NewReplicaSetObserver(reader, mapper, podObserver)
	deploymentObserver := observers.NewDeploymentObserver(reader, mapper, replicaSetObserver)
	statefulSetObserver := observers.NewStatefulSetObserver(reader, mapper, podObserver)
	jobObserver := observers.NewJobObserver(reader, mapper, podObserver)
	serviceObserver := observers.NewServiceObserver(reader, mapper)

	resourceObservers := map[schema.GroupKind]observers.ResourceObserver{
		appsv1.SchemeGroupVersion.WithKind("Deployment").GroupKind():  deploymentObserver,
		appsv1.SchemeGroupVersion.WithKind("StatefulSet").GroupKind(): statefulSetObserver,
		appsv1.SchemeGroupVersion.WithKind("ReplicaSet").GroupKind():  replicaSetObserver,
		v1.SchemeGroupVersion.WithKind("Pod").GroupKind():             podObserver,
		batchv1.SchemeGroupVersion.WithKind("Job").GroupKind():        jobObserver,
		v1.SchemeGroupVersion.WithKind("Service").GroupKind():         serviceObserver,
	}
	defaultObserver := observers.NewDefaultObserver(reader, mapper)
	return resourceObservers, defaultObserver
}

// DeepEqual checks if two instances of ObservedResource are identical. This is used
// to determine whether status has changed for a particular resource.
func DeepEqual(or1, or2 *common.ObservedResource) bool {
	if or1.Identifier != or2.Identifier ||
		or1.Status != or2.Status ||
		or1.Message != or2.Message {
		return false
	}

	if or1.Error != nil && or2.Error != nil && or1.Error.Error() != or2.Error.Error() {
		return false
	}
	if (or1.Error == nil && or2.Error != nil) || (or1.Error != nil && or2.Error == nil) {
		return false
	}

	if len(or1.GeneratedResources) != len(or2.GeneratedResources) {
		return false
	}

	for i := range or1.GeneratedResources {
		if !DeepEqual(or1.GeneratedResources[i], or2.GeneratedResources[i]) {
			return false
		}
	}
	return true
}
