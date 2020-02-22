// Copyright 2020 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package observers

import (
	"context"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe/event"
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe/testutil"
	"sigs.k8s.io/cli-utils/pkg/kstatus/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type fakeClusterReader struct {
	testutil.NoopObserverReader

	getResource *unstructured.Unstructured
	getErr      error

	listResources *unstructured.UnstructuredList
	listErr       error
}

func (f *fakeClusterReader) Get(_ context.Context, key client.ObjectKey, u *unstructured.Unstructured) error {
	if f.getResource != nil {
		u.Object = f.getResource.Object
	}
	return f.getErr
}

func (f *fakeClusterReader) ListNamespaceScoped(_ context.Context, list *unstructured.UnstructuredList, ns string, selector labels.Selector) error {
	if f.listResources != nil {
		list.Items = f.listResources.Items
	}
	return f.listErr
}

type fakeResourceObserver struct{}

func (f *fakeResourceObserver) Observe(ctx context.Context, resource wait.ResourceIdentifier) *event.ObservedResource {
	return nil
}

func (f *fakeResourceObserver) ObserveObject(ctx context.Context, object *unstructured.Unstructured) *event.ObservedResource {
	identifier := toIdentifier(object)
	return &event.ObservedResource{
		Identifier: identifier,
	}
}
