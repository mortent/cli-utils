// Copyright 2020 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package observers

import (
	"context"
	"fmt"
	"testing"

	"gotest.tools/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/cli-utils/pkg/kstatus/status"
	"sigs.k8s.io/cli-utils/pkg/kstatus/wait"
)

var (
	customGVK = schema.GroupVersionKind{
		Group:   "custom.io",
		Version: "v1beta1",
		Kind:    "Custom",
	}
	name      = "Foo"
	namespace = "default"
)

func TestGenericObserver(t *testing.T) {
	testCases := map[string]struct {
		result             *status.Result
		err                error
		expectedIdentifier wait.ResourceIdentifier
		expectedStatus     status.Status
	}{
		"successfully computes status": {
			result: &status.Result{
				Status:  status.InProgressStatus,
				Message: "this is a test",
			},
			expectedIdentifier: wait.ResourceIdentifier{
				GroupKind: customGVK.GroupKind(),
				Name:      name,
				Namespace: namespace,
			},
			expectedStatus: status.InProgressStatus,
		},
		"computing status fails": {
			err: fmt.Errorf("this error is a test"),
			expectedIdentifier: wait.ResourceIdentifier{
				GroupKind: customGVK.GroupKind(),
				Name:      name,
				Namespace: namespace,
			},
			expectedStatus: status.UnknownStatus,
		},
	}

	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {
			observer := genericObserver{
				statusFunc: func(u *unstructured.Unstructured) (*status.Result, error) {
					return tc.result, tc.err
				},
			}

			object := &unstructured.Unstructured{}
			object.SetGroupVersionKind(customGVK)
			object.SetName(name)
			object.SetNamespace(namespace)

			observedResource := observer.ObserveObject(context.Background(), object)

			assert.Equal(t, tc.expectedIdentifier, observedResource.Identifier)
			assert.Equal(t, tc.expectedStatus, observedResource.Status)
		})
	}
}
