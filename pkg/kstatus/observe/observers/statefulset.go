// Copyright 2020 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package observers

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe/event"
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe/observer"
	"sigs.k8s.io/cli-utils/pkg/kstatus/status"
)

func NewStatefulSetObserver(reader observer.ClusterReader, mapper meta.RESTMapper, podObserver observer.ResourceObserver) observer.ResourceObserver {
	return observerFactory(reader, mapper, &statefulSetObserver{
		reader:      reader,
		mapper:      mapper,
		podObserver: podObserver,
	})
}

// StatefulObserver is an observer that can fetch StatefulSet resources
// from the cluster, knows how to find any Pods belonging to the
// StatefulSet, and compute status for the StatefulSet.
type statefulSetObserver struct {
	reader observer.ClusterReader

	mapper meta.RESTMapper

	podObserver observer.ResourceObserver
}

var _ resourceTypeObserver = &statefulSetObserver{}

func (s *statefulSetObserver) ObserveObject(ctx context.Context, statefulSet *unstructured.Unstructured) *event.ObservedResource {
	identifier := toIdentifier(statefulSet)

	observedPods, err := observeGeneratedResources(ctx, s.reader, s.mapper, s.podObserver, statefulSet,
		v1.SchemeGroupVersion.WithKind("Pod").GroupKind(), "spec", "selector")
	if err != nil {
		return &event.ObservedResource{
			Identifier: identifier,
			Status:     status.UnknownStatus,
			Resource:   statefulSet,
			Error:      err,
		}
	}

	res, err := status.Compute(statefulSet)
	if err != nil {
		return &event.ObservedResource{
			Identifier:         identifier,
			Status:             status.UnknownStatus,
			Error:              err,
			GeneratedResources: observedPods,
		}
	}

	return &event.ObservedResource{
		Identifier:         identifier,
		Status:             res.Status,
		Resource:           statefulSet,
		Message:            res.Message,
		GeneratedResources: observedPods,
	}
}
