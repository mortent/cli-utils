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

func NewJobObserver(reader reader.ObserverReader, mapper meta.RESTMapper, podObserver ResourceObserver) ResourceObserver {
	return &JobObserver{
		BaseObserver: BaseObserver{
			Reader:            reader,
			Mapper:            mapper,
			computeStatusFunc: status.Compute,
		},
		PodObserver: podObserver,
	}
}

// JobObserver is an observer that can fetch Job resources and compute
// status. It currently does not implement functionality for fetching
// Pods that belong to a Job.
// TODO: Update observer to look up pods.
type JobObserver struct {
	BaseObserver

	PodObserver ResourceObserver
}

func (j *JobObserver) Observe(ctx context.Context, identifier wait.ResourceIdentifier) *common.ObservedResource {
	job, observedResource := j.LookupResource(ctx, identifier)
	if observedResource != nil {
		return observedResource
	}
	return j.ObserveObject(ctx, job)
}

func (j *JobObserver) ObserveObject(ctx context.Context, job *unstructured.Unstructured) *common.ObservedResource {
	identifier := toIdentifier(job)

	// Add status rules for jobs that looks at the pods belonging to the job.
	// For now, it only delegates to the basic status computation.
	res, err := j.computeStatusFunc(job)
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
		Resource:   job,
		Message:    res.Message,
	}
}
