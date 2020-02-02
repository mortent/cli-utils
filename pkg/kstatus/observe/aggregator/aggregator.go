package aggregator

import (
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe/common"
	"sigs.k8s.io/cli-utils/pkg/kstatus/status"
	"sigs.k8s.io/cli-utils/pkg/kstatus/wait"
)

// AllCurrentOrNotFoundStatusAggregator is an implementation of the StatusAggregator interface
// that considers the aggregate status as Current whenever all resources have either the Current
// or the NotFound status.
type AllCurrentOrNotFoundStatusAggregator struct {
	resourceCurrentStatus map[wait.ResourceIdentifier]status.Status
}

// NewAllCurrentOrNotFoundStatusAggregator returns a new StatusAggregator that will track
// the status of all resources given by the provided identifiers.
func NewAllCurrentOrNotFoundStatusAggregator(identifiers []wait.ResourceIdentifier) *AllCurrentOrNotFoundStatusAggregator {
	aggregator := &AllCurrentOrNotFoundStatusAggregator{
		resourceCurrentStatus: make(map[wait.ResourceIdentifier]status.Status),
	}
	for _, id := range identifiers {
		aggregator.resourceCurrentStatus[id] = status.UnknownStatus
	}
	return aggregator
}

func (d *AllCurrentOrNotFoundStatusAggregator) ResourceObserved(observedResource *common.ObservedResource) {
	d.resourceCurrentStatus[observedResource.Identifier] = observedResource.Status
}

func (d *AllCurrentOrNotFoundStatusAggregator) AggregateStatus() status.Status {
	// if we are not observing any resources, we consider status be Current.
	if len(d.resourceCurrentStatus) == 0 {
		return status.CurrentStatus
	}

	allCurrentOrNotFound := true
	anyUnknown := false
	for _, latestStatus := range d.resourceCurrentStatus {
		if latestStatus == status.FailedStatus {
			return status.FailedStatus
		}
		if latestStatus == status.UnknownStatus {
			anyUnknown = true
		}
		if !(latestStatus == status.CurrentStatus || latestStatus == status.NotFoundStatus) {
			allCurrentOrNotFound = false
		}
	}
	if anyUnknown {
		return status.UnknownStatus
	}
	if allCurrentOrNotFound {
		return status.CurrentStatus
	}
	return status.InProgressStatus
}

func (d *AllCurrentOrNotFoundStatusAggregator) Completed() bool {
	return d.AggregateStatus() == status.CurrentStatus
}
