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

func NewReplicaSetObserver(reader observer.ClusterReader, mapper meta.RESTMapper, podObserver observer.ResourceObserver) observer.ResourceObserver {
	return observerFactory(reader, mapper, &replicaSetObserver{
		reader:      reader,
		mapper:      mapper,
		podObserver: podObserver,
	})
}

// replicaSetObserver is an observer that can fetch ReplicaSet resources
// from the cluster, knows how to find any Pods belonging to the ReplicaSet,
// and compute status for the ReplicaSet.
type replicaSetObserver struct {
	reader observer.ClusterReader

	mapper meta.RESTMapper

	podObserver observer.ResourceObserver
}

var _ resourceTypeObserver = &replicaSetObserver{}

func (r *replicaSetObserver) ObserveObject(ctx context.Context, rs *unstructured.Unstructured) *event.ObservedResource {
	identifier := toIdentifier(rs)

	observedPods, err := observeGeneratedResources(ctx, r.reader, r.mapper, r.podObserver, rs,
		v1.SchemeGroupVersion.WithKind("Pod").GroupKind(), "spec", "selector")
	if err != nil {
		return &event.ObservedResource{
			Identifier: identifier,
			Status:     status.UnknownStatus,
			Resource:   rs,
			Error:      err,
		}
	}

	res, err := status.Compute(rs)
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
		Resource:           rs,
		Message:            res.Message,
		GeneratedResources: observedPods,
	}
}
