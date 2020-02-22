// Copyright 2020 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package observers

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"gotest.tools/assert"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe/event"
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe/testutil"
	"sigs.k8s.io/cli-utils/pkg/kstatus/status"
	"sigs.k8s.io/cli-utils/pkg/kstatus/wait"
)

var (
	deploymentGVK = appsv1.SchemeGroupVersion.WithKind("Deployment")
	deploymentGVR = appsv1.SchemeGroupVersion.WithResource("deployments")

	rsGVK = appsv1.SchemeGroupVersion.WithKind("ReplicaSet")
)

func TestLookupResource(t *testing.T) {
	name := "Foo"
	namespace := "Bar"
	deploymentIdentifier := wait.ResourceIdentifier{
		GroupKind: deploymentGVK.GroupKind(),
		Name:      name,
		Namespace: namespace,
	}
	deploymentResource := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
			},
		},
	}
	deploymentResource.SetGroupVersionKind(deploymentGVK)

	testCases := map[string]struct {
		identifier         wait.ResourceIdentifier
		readerErr          error
		readerResource     *unstructured.Unstructured
		expectedStatus     status.Status
		expectErr          bool
		expectedErrMessage string
	}{
		"unknown GVK": {
			identifier: wait.ResourceIdentifier{
				GroupKind: schema.GroupKind{
					Group: "custom.io",
					Kind:  "Custom",
				},
				Name:      "Bar",
				Namespace: "default",
			},
			expectedStatus:     status.UnknownStatus,
			expectErr:          true,
			expectedErrMessage: "",
		},
		"resource does not exist": {
			identifier:     deploymentIdentifier,
			readerErr:      errors.NewNotFound(deploymentGVR.GroupResource(), "Foo"),
			expectedStatus: status.NotFoundStatus,
			expectErr:      false,
		},
		"getting resource fails": {
			identifier:         deploymentIdentifier,
			readerErr:          errors.NewInternalError(fmt.Errorf("this is a test")),
			expectedStatus:     status.UnknownStatus,
			expectErr:          true,
			expectedErrMessage: "",
		},
		"getting resource succeeds": {
			identifier:     deploymentIdentifier,
			readerResource: deploymentResource,
			expectedStatus: status.CurrentStatus,
		},
	}

	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {
			fakeReader := &fakeClusterReader{
				getResource: tc.readerResource,
				getErr:      tc.readerErr,
			}
			fakeMapper := testutil.NewFakeRESTMapper(deploymentGVK)

			observer := observerFactory(fakeReader, fakeMapper, &fakeResourceTypeObserver{})

			observedResource := observer.Observe(context.Background(), tc.identifier)

			assert.Equal(t, tc.identifier, observedResource.Identifier)
			assert.Equal(t, tc.expectedStatus, observedResource.Status)

			if tc.expectErr {
				if observedResource.Error == nil {
					t.Errorf("expected error, but didn't get one")
				} else {
					assert.ErrorContains(t, observedResource.Error, tc.expectedErrMessage)
				}
				return
			}

			assert.NilError(t, observedResource.Error)

			if observedResource.Resource != nil {
				assert.Equal(t, deploymentGVK, observedResource.Resource.GroupVersionKind())
			}
		})
	}
}

func TestObserveGeneratedResources(t *testing.T) {
	testCases := map[string]struct {
		manifest    string
		listObjects []unstructured.Unstructured
		listErr     error
		gk          schema.GroupKind
		path        []string
		expectError bool
		errMessage  string
	}{
		"invalid selector": {
			manifest: `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: Foo
spec:
  replicas: 1
`,
			gk:          appsv1.SchemeGroupVersion.WithKind("ReplicaSet").GroupKind(),
			path:        []string{"spec", "selector"},
			expectError: true,
			errMessage:  "no selector found",
		},
		"Invalid GVK": {
			manifest: `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: Foo
spec:
  replicas: 1
  selector:
    matchLabels:
      app: nginx
`,
			gk: schema.GroupKind{
				Group: "custom.io",
				Kind:  "Custom",
			},
			path:        []string{"spec", "selector"},
			expectError: true,
			errMessage:  "no matches for kind",
		},
		"error listing replicasets": {
			manifest: `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: Foo
spec:
  replicas: 1
  selector:
    matchLabels:
      app: nginx
`,
			listErr:     fmt.Errorf("this is a test"),
			gk:          appsv1.SchemeGroupVersion.WithKind("ReplicaSet").GroupKind(),
			path:        []string{"spec", "selector"},
			expectError: true,
			errMessage:  "this is a test",
		},
		"successfully lists and observe the generated resources": {
			manifest: `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: Foo
spec:
  replicas: 1
  selector:
    matchLabels:
      app: nginx
`,
			listObjects: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"apiVersion": "apps/v1",
						"kind":       "ReplicaSet",
						"metadata": map[string]interface{}{
							"name":      "Foo-12345",
							"namespace": "default",
						},
					},
				},
			},
			gk:          appsv1.SchemeGroupVersion.WithKind("ReplicaSet").GroupKind(),
			path:        []string{"spec", "selector"},
			expectError: false,
		},
	}

	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {
			fakeReader := &fakeClusterReader{
				listResources: &unstructured.UnstructuredList{
					Items: tc.listObjects,
				},
				listErr: tc.listErr,
			}
			fakeMapper := testutil.NewFakeRESTMapper(rsGVK)
			fakeResourceObserver := &fakeResourceObserver{}

			object := testutil.YamlToUnstructured(t, tc.manifest)

			observedResources, err := observeGeneratedResources(context.Background(), fakeReader, fakeMapper,
				fakeResourceObserver, object, tc.gk, tc.path...)

			if tc.expectError {
				if err == nil {
					t.Errorf("expected an error, but didn't get one")
					return
				}
				assert.ErrorContains(t, err, tc.errMessage)
				return
			}
			if !tc.expectError && err != nil {
				t.Errorf("did not expect an error, but got %v", err)
			}

			assert.Equal(t, len(tc.listObjects), len(observedResources))
			assert.Assert(t, sort.IsSorted(observedResources))
		})
	}
}

type fakeResourceTypeObserver struct{}

func (f *fakeResourceTypeObserver) ObserveObject(_ context.Context, resource *unstructured.Unstructured) *event.ObservedResource {
	return &event.ObservedResource{
		Status:     status.CurrentStatus,
		Identifier: toIdentifier(resource),
	}
}
