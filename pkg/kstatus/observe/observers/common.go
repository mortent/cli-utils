// Copyright 2020 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe/event"
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe/observer"
	"sigs.k8s.io/cli-utils/pkg/kstatus/status"
	"sigs.k8s.io/cli-utils/pkg/kstatus/wait"
)

type computeStatusFunc func(u *unstructured.Unstructured) (*status.Result, error)

func observerFactory(reader observer.ClusterReader, mapper meta.RESTMapper, resourceTypeObserver resourceTypeObserver) observer.ResourceObserver {
	return &baseObserver{
		reader:               reader,
		mapper:               mapper,
		resourceTypeObserver: resourceTypeObserver,
	}
}

type resourceTypeObserver interface {
	ObserveObject(ctx context.Context, resource *unstructured.Unstructured) *event.ObservedResource
}

type baseObserver struct {
	reader observer.ClusterReader
	mapper meta.RESTMapper

	resourceTypeObserver
}

func (o *baseObserver) Observe(ctx context.Context, identifier wait.ResourceIdentifier) *event.ObservedResource {
	deployment, err := o.lookupResource(ctx, identifier)
	if err != nil {
		return handleObservedResourceError(identifier, err)
	}
	return o.ObserveObject(ctx, deployment)
}

func (o *baseObserver) lookupResource(ctx context.Context, identifier wait.ResourceIdentifier) (*unstructured.Unstructured, error) {
	GVK, err := toGVK(o.mapper, identifier.GroupKind)
	if err != nil {
		return nil, err
	}

	var u unstructured.Unstructured
	u.SetGroupVersionKind(GVK)
	key := keyForNamespacedResource(identifier)
	err = o.reader.Get(ctx, key, &u)
	if err != nil {
		return nil, err
	}
	u.SetNamespace(identifier.Namespace)
	return &u, nil
}

// ObservedGeneratedResources provides a way to fetch the statuses for all resources of a given GroupKind
// that match the selector in the provided resource. Typically, this is used to fetch the status of generated
// resources.
func observeGeneratedResources(ctx context.Context, reader observer.ClusterReader, mapper meta.RESTMapper,
	observer observer.ResourceObserver, object *unstructured.Unstructured, gk schema.GroupKind, selectorPath ...string) (event.ObservedResources, error) {
	namespace := getNamespaceForNamespacedResource(object)
	selector, err := toSelector(object, selectorPath...)
	if err != nil {
		return event.ObservedResources{}, err
	}

	var objectList unstructured.UnstructuredList
	gvk, err := toGVK(mapper, gk)
	if err != nil {
		return event.ObservedResources{}, err
	}
	objectList.SetGroupVersionKind(gvk)
	err = reader.ListNamespaceScoped(ctx, &objectList, namespace, selector)
	if err != nil {
		return event.ObservedResources{}, err
	}

	var observedObjects event.ObservedResources
	for i := range objectList.Items {
		generatedObject := objectList.Items[i]
		observedObject := observer.ObserveObject(ctx, &generatedObject)
		observedObjects = append(observedObjects, observedObject)
	}
	sort.Sort(observedObjects)
	return observedObjects, nil
}

func handleObservedResourceError(identifier wait.ResourceIdentifier, err error) *event.ObservedResource {
	if errors.IsNotFound(err) {
		return &event.ObservedResource{
			Identifier: identifier,
			Status:     status.NotFoundStatus,
			Message:    "Resource not found",
		}
	}
	return &event.ObservedResource{
		Identifier: identifier,
		Status:     status.UnknownStatus,
		Error:      err,
	}
}

func toGVK(mapper meta.RESTMapper, gk schema.GroupKind) (schema.GroupVersionKind, error) {
	mapping, err := mapper.RESTMapping(gk)
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

// getNamespaceForNamespacedResource returns the namespace for the given object,
// but includes logic for returning the default namespace if it is not set.
func getNamespaceForNamespacedResource(object runtime.Object) string {
	acc, err := meta.Accessor(object)
	if err != nil {
		panic(err)
	}
	ns := acc.GetNamespace()
	if ns == "" {
		return "default"
	}
	return ns
}

// keyForNamespacedResource returns the object key for the given identifier. It makes
// sure to set the namespace to default if it is not provided.
func keyForNamespacedResource(identifier wait.ResourceIdentifier) types.NamespacedName {
	namespace := "default"
	if identifier.Namespace != "" {
		namespace = identifier.Namespace
	}
	return types.NamespacedName{
		Name:      identifier.Name,
		Namespace: namespace,
	}
}
