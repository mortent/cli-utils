package common

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/cli-utils/pkg/kstatus/status"
	"sigs.k8s.io/cli-utils/pkg/kstatus/wait"
)

// ObservedResource contains information about a resource after we have
// fetched it from the cluster and computed status.
type ObservedResource struct {
	// Identifier contains the information necessary to locate the
	// resource within a cluster.
	Identifier wait.ResourceIdentifier

	// Status is the computed status for this resource.
	Status status.Status

	// Resource contains the actual manifest for the resource that
	// was fetched from the cluster and used to compute status.
	Resource *unstructured.Unstructured

	// Errors contains the error if something went wrong during the
	// process of fetching the resource and computing the status.
	Error error

	// Message is text describing the status of the resource.
	Message string

	// GeneratedResources is a slice of ObservedResource that
	// contains information and status for any generated resources
	// of the current resource.
	GeneratedResources ObservedResources
}

type ObservedResources []*ObservedResource

func (g ObservedResources) Len() int {
	return len(g)
}

func (g ObservedResources) Less(i, j int) bool {
	idI := g[i].Identifier
	idJ := g[j].Identifier

	if idI.Namespace != idJ.Namespace {
		return idI.Namespace < idJ.Namespace
	}
	if idI.GroupKind.Group != idJ.GroupKind.Group {
		return idI.GroupKind.Group < idJ.GroupKind.Group
	}
	if idI.GroupKind.Kind != idJ.GroupKind.Kind {
		return idI.GroupKind.Kind < idJ.GroupKind.Kind
	}
	return idI.Name < idJ.Name
}

func (g ObservedResources) Swap(i, j int) {
	g[i], g[j] = g[j], g[i]
}
