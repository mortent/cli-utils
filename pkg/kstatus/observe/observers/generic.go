// Copyright 2020 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package observers

import (
	"context"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe/event"
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe/observer"
	"sigs.k8s.io/cli-utils/pkg/kstatus/status"
)

func NewGenericObserver(reader observer.ClusterReader, mapper meta.RESTMapper) observer.ResourceObserver {
	return observerFactory(reader, mapper, &genericObserver{
		statusFunc: status.Compute,
	})
}

// genericObserver is an observer that will be used for any resource that
// doesn't have a specific observer. It will just delegate computation of
// status to the status library.
// This should work pretty well for resources that doesn't have any
// generated resources and where status can be computed only based on the
// resource itself.
type genericObserver struct {
	statusFunc computeStatusFunc
}

var _ resourceTypeObserver = &genericObserver{}

func (d *genericObserver) ObserveObject(_ context.Context, resource *unstructured.Unstructured) *event.ObservedResource {
	identifier := toIdentifier(resource)

	res, err := d.statusFunc(resource)
	if err != nil {
		return &event.ObservedResource{
			Identifier: identifier,
			Status:     status.UnknownStatus,
			Error:      err,
		}
	}

	return &event.ObservedResource{
		Identifier: identifier,
		Status:     res.Status,
		Resource:   resource,
		Message:    res.Message,
	}
}
