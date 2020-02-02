package observers

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe/common"
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe/reader"
	"sigs.k8s.io/cli-utils/pkg/kstatus/status"
	"sigs.k8s.io/cli-utils/pkg/kstatus/wait"
)

func NewReplicaSetObserver(reader reader.ObserverReader, mapper meta.RESTMapper, podObserver ResourceObserver) ResourceObserver {
	return &replicaSetObserver{
		BaseObserver: BaseObserver{
			Reader:            reader,
			Mapper:            mapper,
			computeStatusFunc: status.Compute,
		},
		PodObserver: podObserver,
	}
}

// replicaSetObserver is an observer that can fetch ReplicaSet resources
// from the cluster, knows how to find any Pods belonging to the ReplicaSet,
// and compute status for the ReplicaSet.
type replicaSetObserver struct {
	BaseObserver

	PodObserver ResourceObserver
}

func (r *replicaSetObserver) Observe(ctx context.Context, identifier wait.ResourceIdentifier) *common.ObservedResource {
	rs, observedResource := r.LookupResource(ctx, identifier)
	if observedResource != nil {
		return observedResource
	}
	return r.ObserveObject(ctx, rs)
}

func (r *replicaSetObserver) ObserveObject(ctx context.Context, rs *unstructured.Unstructured) *common.ObservedResource {
	identifier := toIdentifier(rs)

	observedPods, err := r.ObserveGeneratedResources(ctx, r.PodObserver, rs,
		v1.SchemeGroupVersion.WithKind("Pod").GroupKind(), "spec", "selector")
	if err != nil {
		return &common.ObservedResource{
			Identifier: identifier,
			Status:     status.UnknownStatus,
			Resource:   rs,
			Error:      err,
		}
	}

	res, err := r.computeStatusFunc(rs)
	if err != nil {
		return &common.ObservedResource{
			Identifier:         identifier,
			Status:             status.UnknownStatus,
			Error:              err,
			GeneratedResources: observedPods,
		}
	}

	return &common.ObservedResource{
		Identifier:         identifier,
		Status:             res.Status,
		Resource:           rs,
		Message:            res.Message,
		GeneratedResources: observedPods,
	}
}
