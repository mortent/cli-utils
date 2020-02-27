// Copyright 2020 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package aggregator

import (
	"sigs.k8s.io/cli-utils/pkg/kstatus/polling/event"
	"sigs.k8s.io/cli-utils/pkg/kstatus/status"
	"sigs.k8s.io/cli-utils/pkg/kstatus/wait"
)

type GenericAggregator struct {
	resourceCurrentStatus map[wait.ResourceIdentifier]status.Status
	desiredStatus         status.Status
}

func NewGenericAggregator(identifiers []wait.ResourceIdentifier, desiredStatus status.Status) *GenericAggregator {
	aggregator := &GenericAggregator{
		resourceCurrentStatus: make(map[wait.ResourceIdentifier]status.Status),
		desiredStatus:         desiredStatus,
	}
	for _, id := range identifiers {
		aggregator.resourceCurrentStatus[id] = status.UnknownStatus
	}
	return aggregator
}

func (g *GenericAggregator) ResourceStatus(r *event.ResourceStatus) {
	g.resourceCurrentStatus[r.Identifier] = r.Status
}

func (g *GenericAggregator) AggregateStatus() status.Status {
	if len(g.resourceCurrentStatus) == 0 {
		return g.desiredStatus
	}

	allDesired := true
	anyUnknown := false
	for _, s := range g.resourceCurrentStatus {
		if s == status.FailedStatus {
			return status.FailedStatus
		}
		if s == status.UnknownStatus {
			anyUnknown = true
		}
		if s != g.desiredStatus {
			allDesired = false
		}
	}
	if anyUnknown {
		return status.UnknownStatus
	}
	if allDesired {
		return g.desiredStatus
	}
	return status.InProgressStatus
}

func (g *GenericAggregator) Completed() bool {
	return g.AggregateStatus() == g.desiredStatus
}
