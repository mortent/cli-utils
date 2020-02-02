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

func NewServiceObserver(reader reader.ObserverReader, mapper meta.RESTMapper) ResourceObserver {
	return &ServiceObserver{
		BaseObserver: BaseObserver{
			Reader:            reader,
			Mapper:            mapper,
			computeStatusFunc: status.Compute,
		},
	}
}

// ServiceObserver is an observer that can fetch Service resources
// from the cluster and compute status. Currently it does not look at
// Endpoint resources or targeted pods when computing status, but both
// can be added.
type ServiceObserver struct {
	BaseObserver
}

func (s *ServiceObserver) Observe(ctx context.Context, identifier wait.ResourceIdentifier) *common.ObservedResource {
	service, observedResource := s.LookupResource(ctx, identifier)
	if observedResource != nil {
		return observedResource
	}
	return s.ObserveObject(ctx, service)
}

func (s *ServiceObserver) ObserveObject(_ context.Context, service *unstructured.Unstructured) *common.ObservedResource {
	identifier := toIdentifier(service)

	res, err := s.computeStatusFunc(service)
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
		Resource:   service,
		Message:    res.Message,
	}
}
