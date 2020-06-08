package resourceset

import (
	"k8s.io/cli-runtime/pkg/resource"
	"sigs.k8s.io/cli-utils/pkg/apply/prune"
	"sigs.k8s.io/cli-utils/pkg/apply/solver"
)

type ResourceSet struct {
	inventories []*resource.Info
	crds []*resource.Info
	namespaces []*resource.Info
	resources []*resource.Info
}

func NewResourceSet(objects []*resource.Info) *ResourceSet {
	var inventories []*resource.Info
	var crds []*resource.Info
	var namespaces []*resource.Info
	var resources []*resource.Info

	for _, info := range objects {
		switch {
		case prune.IsInventoryObject(info.Object):
			inventories = append(inventories, info)
		case solver.IsCRD(info):
			crds = append(crds, info)
		case isNamespace(info):
			namespaces = append(namespaces, info)
		default:
			resources = append(resources, info)
		}
	}
	return &ResourceSet{
		inventories: inventories,
		crds: crds,
		namespaces: namespaces,
		resources: resources,
	}
}

func isNamespace(object *resource.Info) bool {
	gvk := object.Object.GetObjectKind().GroupVersionKind()
	if gvk.Group == "" && gvk.Kind == "Namespace" {
		return true
	}
	return false
}
