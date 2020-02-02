package observers

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe/common"
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe/reader"
	"sigs.k8s.io/cli-utils/pkg/kstatus/status"
	"sigs.k8s.io/cli-utils/pkg/kstatus/wait"
)

// BaseObserver provides some basic functionality needed by the observers.
type BaseObserver struct {
	Reader reader.ObserverReader

	Mapper meta.RESTMapper

	computeStatusFunc ComputeStatusFunc
}

// SetComputeStatusFunc allows for setting the function used by the observer for computing status. The default
// value here is to use the status package. This is provided for testing purposes.
func (b *BaseObserver) SetComputeStatusFunc(statusFunc ComputeStatusFunc) {
	b.computeStatusFunc = statusFunc
}

// LookupResource looks up a resource with the given identifier. It will use the rest mapper to resolve
// the version of the GroupKind given in the identifier.
// If the resource is found, it will be returned and the observedResource will be nil. If there is an error
// or the resource is not found, the observedResource will be returned containing information about the resource
// and the problem.
func (b *BaseObserver) LookupResource(ctx context.Context, identifier wait.ResourceIdentifier) (*unstructured.Unstructured, *common.ObservedResource) {
	GVK, err := b.GVK(identifier.GroupKind)
	if err != nil {
		return nil, &common.ObservedResource{
			Identifier: identifier,
			Status:     status.UnknownStatus,
			Error:      err,
		}
	}

	var u unstructured.Unstructured
	u.SetGroupVersionKind(GVK)
	key := common.KeyForNamespacedResource(identifier)
	err = b.Reader.Get(ctx, key, &u)
	if err != nil && errors.IsNotFound(err) {
		return nil, &common.ObservedResource{
			Identifier: identifier,
			Status:     status.NotFoundStatus,
			Message:    "Resource not found",
		}
	}
	if err != nil {
		return nil, &common.ObservedResource{
			Identifier: identifier,
			Status:     status.UnknownStatus,
			Error:      err,
		}
	}
	u.SetNamespace(identifier.Namespace)
	return &u, nil
}

// ObservedGeneratedResources provides a way to fetch the statuses for all resources of a given GroupKind
// that match the selector in the provided resource. Typically, this is used to fetch the status of generated
// resources.
func (b *BaseObserver) ObserveGeneratedResources(ctx context.Context, observer ResourceObserver, object *unstructured.Unstructured,
	gk schema.GroupKind, selectorPath ...string) (common.ObservedResources, error) {
	namespace := common.GetNamespaceForNamespacedResource(object)
	selector, err := toSelector(object, selectorPath...)
	if err != nil {
		return common.ObservedResources{}, err
	}

	var objectList unstructured.UnstructuredList
	gvk, err := b.GVK(gk)
	if err != nil {
		return common.ObservedResources{}, err
	}
	objectList.SetGroupVersionKind(gvk)
	err = b.Reader.ListNamespaceScoped(ctx, &objectList, namespace, selector)
	if err != nil {
		return common.ObservedResources{}, err
	}

	var observedObjects common.ObservedResources
	for i := range objectList.Items {
		generatedObject := objectList.Items[i]
		observedObject := observer.ObserveObject(ctx, &generatedObject)
		observedObjects = append(observedObjects, observedObject)
	}
	sort.Sort(observedObjects)
	return observedObjects, nil
}

// GVK looks up the GVK from a GroupKind using the rest mapper.
func (b *BaseObserver) GVK(gk schema.GroupKind) (schema.GroupVersionKind, error) {
	mapping, err := b.Mapper.RESTMapping(gk)
	if err != nil {
		return schema.GroupVersionKind{}, err
	}
	return mapping.GroupVersionKind, nil
}

func toSelector(resource *unstructured.Unstructured, path ...string) (labels.Selector, error) {
	selector, found, err := unstructured.NestedMap(resource.Object, path...)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("no selector found")
	}
	bytes, err := json.Marshal(selector)
	if err != nil {
		return nil, err
	}
	var s metav1.LabelSelector
	err = json.Unmarshal(bytes, &s)
	if err != nil {
		return nil, err
	}
	return metav1.LabelSelectorAsSelector(&s)
}

func toIdentifier(u *unstructured.Unstructured) wait.ResourceIdentifier {
	return wait.ResourceIdentifier{
		GroupKind: u.GroupVersionKind().GroupKind(),
		Name:      u.GetName(),
		Namespace: u.GetNamespace(),
	}
}
