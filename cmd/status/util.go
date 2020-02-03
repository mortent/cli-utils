package status

import (
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/cli-utils/pkg/kstatus/wait"
	"sigs.k8s.io/kustomize/kyaml/kio"
	"sigs.k8s.io/kustomize/kyaml/yaml"
)

// CaptureIdentifiersFilter implements the Filter interface in the kio package. It
// captures the identifiers for all resources passed through the pipeline.
type CaptureIdentifiersFilter struct {
	Identifiers []wait.ResourceIdentifier
	Mapper      meta.RESTMapper
}

var _ kio.Filter = &CaptureIdentifiersFilter{}

func (f *CaptureIdentifiersFilter) Filter(slice []*yaml.RNode) ([]*yaml.RNode, error) {
	for i := range slice {
		objectMeta, err := slice[i].GetMeta()
		if err != nil {
			return nil, err
		}
		// TODO(mortent): Update kyaml library
		id := objectMeta.GetIdentifier()
		gv, err := schema.ParseGroupVersion(id.APIVersion)
		if err != nil {
			return nil, err
		}
		gk := schema.GroupKind{
			Group: gv.Group,
			Kind:  id.Kind,
		}
		mapping, err := f.Mapper.RESTMapping(gk)
		if err != nil {
			return nil, err
		}
		var namespace string
		if mapping.Scope.Name() == meta.RESTScopeNameNamespace && id.Namespace == "" {
			namespace = "default"
		} else {
			namespace = id.Namespace
		}
		if IsValidKubernetesResource(id) {
			f.Identifiers = append(f.Identifiers, wait.ResourceIdentifier{
				Name:      id.Name,
				Namespace: namespace,
				GroupKind: schema.GroupKind{
					Group: gv.Group,
					Kind:  id.Kind,
				},
			})
		}
	}
	return slice, nil
}

func IsValidKubernetesResource(id yaml.ResourceIdentifier) bool {
	return id.GetKind() != "" && id.GetAPIVersion() != "" && id.GetName() != ""
}
