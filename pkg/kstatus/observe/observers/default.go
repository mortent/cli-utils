package observers

import (
	"context"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe/common"
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe/reader"
	"sigs.k8s.io/cli-utils/pkg/kstatus/status"
	"sigs.k8s.io/cli-utils/pkg/kstatus/wait"
)

func NewDefaultObserver(reader reader.ObserverReader, mapper meta.RESTMapper) ResourceObserver {
	return &DefaultObserver{
		BaseObserver: BaseObserver{
			Reader:            reader,
			Mapper:            mapper,
			computeStatusFunc: status.Compute,
		},
	}
}

// DefaultObserver is an observer that will be used for any resource that
// doesn't have a specific observer. It will just delegate computation of
// status to the status library.
// This should work pretty well for resources that doesn't have any
// generated resources and where status can be computed only based on the
// resource itself.
type DefaultObserver struct {
	BaseObserver
}

func (d *DefaultObserver) Observe(ctx context.Context, identifier wait.ResourceIdentifier) *common.ObservedResource {
	u, observedResource := d.LookupResource(ctx, identifier)
	if observedResource != nil {
		return observedResource
	}
	return d.ObserveObject(ctx, u)
}

func (d *DefaultObserver) ObserveObject(_ context.Context, resource *unstructured.Unstructured) *common.ObservedResource {
	identifier := toIdentifier(resource)

	res, err := d.computeStatusFunc(resource)
	if err != nil {
		return &common.ObservedResource{
			Identifier: identifier,
			Status:     status.UnknownStatus,
			Error:      err,
		}
	}

	return &common.ObservedResource{
		Identifier: identifier,
		Status:     res.Status,
		Resource:   resource,
		Message:    res.Message,
	}
}
