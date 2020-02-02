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
	deploymentIdentifier := wait.ResourceIdentifier{
		GroupKind: deploymentGVK.GroupKind(),
		Name:      "Foo",
		Namespace: "Bar",
	}

	testCases := map[string]struct {
		identifier              wait.ResourceIdentifier
		err                     error
		returnsObservedResource bool
		expectedStatus          status.Status
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
			returnsObservedResource: true,
			expectedStatus:          status.UnknownStatus,
		},
		"resource does not exist": {
			identifier:              deploymentIdentifier,
			err:                     errors.NewNotFound(deploymentGVR.GroupResource(), "Foo"),
			returnsObservedResource: true,
			expectedStatus:          status.NotFoundStatus,
		},
		"getting resource fails": {
			identifier:              deploymentIdentifier,
			err:                     errors.NewInternalError(fmt.Errorf("this is a test")),
			returnsObservedResource: true,
			expectedStatus:          status.UnknownStatus,
		},
		"getting resource succeeds": {
			identifier: deploymentIdentifier,
		},
	}

	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {
			fakeReader := &fakeObserverReader{
				getErr: tc.err,
			}
			fakeMapper := testutil.NewFakeRESTMapper(deploymentGVK)

			baseObserver := &BaseObserver{
				Reader: fakeReader,
				Mapper: fakeMapper,
			}

			u, observedResource := baseObserver.LookupResource(context.Background(), tc.identifier)

			if tc.returnsObservedResource {
				assert.Equal(t, tc.expectedStatus, observedResource.Status)
			} else {
				assert.Equal(t, deploymentGVK, u.GroupVersionKind())
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
	}

	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {
			fakeObserverReader := &fakeObserverReader{
				listResources: &unstructured.UnstructuredList{
					Items: tc.listObjects,
				},
				listErr: tc.listErr,
			}
			fakeMapper := testutil.NewFakeRESTMapper(rsGVK)
			fakeResourceObserver := &fakeResourceObserver{}

			object := testutil.YamlToUnstructured(t, tc.manifest)

			observer := &BaseObserver{
				Reader: fakeObserverReader,
				Mapper: fakeMapper,
			}

			observedResources, err := observer.ObserveGeneratedResources(context.Background(), fakeResourceObserver, object, tc.gk, tc.path...)

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
