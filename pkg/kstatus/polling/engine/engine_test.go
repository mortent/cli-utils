// Copyright 2020 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"gotest.tools/assert"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/cli-utils/pkg/kstatus/polling/event"
	"sigs.k8s.io/cli-utils/pkg/kstatus/polling/testutil"
	"sigs.k8s.io/cli-utils/pkg/kstatus/status"
	"sigs.k8s.io/cli-utils/pkg/object"
	fakemapper "sigs.k8s.io/cli-utils/pkg/testutil"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestStatusPollerRunner(t *testing.T) {
	testCases := map[string]struct {
		identifiers         []object.ObjMetadata
		defaultStatusReader StatusReader
		expectedEventTypes  []event.EventType
	}{
		"single resource": {
			identifiers: []object.ObjMetadata{
				{
					GroupKind: schema.GroupKind{
						Group: "apps",
						Kind:  "Deployment",
					},
					Name:      "foo",
					Namespace: "bar",
				},
			},
			defaultStatusReader: &fakeStatusReader{
				resourceStatuses: map[schema.GroupKind][]status.Status{
					schema.GroupKind{Group: "apps", Kind: "Deployment"}: { //nolint:gofmt
						status.InProgressStatus,
						status.CurrentStatus,
					},
				},
				resourceStatusCount: make(map[schema.GroupKind]int),
			},
			expectedEventTypes: []event.EventType{
				event.ResourceUpdateEvent,
				event.ResourceUpdateEvent,
			},
		},
		"multiple resources": {
			identifiers: []object.ObjMetadata{
				{
					GroupKind: schema.GroupKind{
						Group: "apps",
						Kind:  "Deployment",
					},
					Name:      "foo",
					Namespace: "default",
				},
				{
					GroupKind: schema.GroupKind{
						Group: "",
						Kind:  "Service",
					},
					Name:      "bar",
					Namespace: "default",
				},
			},
			defaultStatusReader: &fakeStatusReader{
				resourceStatuses: map[schema.GroupKind][]status.Status{
					schema.GroupKind{Group: "apps", Kind: "Deployment"}: { //nolint:gofmt
						status.InProgressStatus,
						status.CurrentStatus,
					},
					schema.GroupKind{Group: "", Kind: "Service"}: { //nolint:gofmt
						status.InProgressStatus,
						status.InProgressStatus,
						status.CurrentStatus,
					},
				},
				resourceStatusCount: make(map[schema.GroupKind]int),
			},
			expectedEventTypes: []event.EventType{
				event.ResourceUpdateEvent,
				event.ResourceUpdateEvent,
				event.ResourceUpdateEvent,
				event.ResourceUpdateEvent,
			},
		},
	}

	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			identifiers := tc.identifiers

			fakeMapper := fakemapper.NewFakeRESTMapper(
				appsv1.SchemeGroupVersion.WithKind("Deployment"),
				v1.SchemeGroupVersion.WithKind("Service"),
			)

			engine := PollerEngine{
				Mapper: fakeMapper,
			}

			options := Options{
				PollInterval: 2 * time.Second,
				ClusterReaderFactoryFunc: func(_ client.Reader, _ meta.RESTMapper, _ []object.ObjMetadata) (
					ClusterReader, error) {
					return testutil.NewNoopClusterReader(), nil
				},
				StatusReadersFactoryFunc: func(_ ClusterReader, _ meta.RESTMapper) (
					statusReaders map[schema.GroupKind]StatusReader, defaultStatusReader StatusReader) {
					return make(map[schema.GroupKind]StatusReader), tc.defaultStatusReader
				},
			}

			eventChannel := engine.Poll(ctx, identifiers, options)

			var eventTypes []event.EventType
			for ch := range eventChannel {
				eventTypes = append(eventTypes, ch.EventType)
				if len(eventTypes) == len(tc.expectedEventTypes) {
					cancel()
				}
			}

			assert.DeepEqual(t, tc.expectedEventTypes, eventTypes)
		})
	}
}

func TestNewStatusPollerRunnerCancellation(t *testing.T) {
	identifiers := make([]object.ObjMetadata, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	timer := time.NewTimer(5 * time.Second)

	engine := PollerEngine{}

	options := Options{
		PollInterval: 2 * time.Second,
		ClusterReaderFactoryFunc: func(_ client.Reader, _ meta.RESTMapper, _ []object.ObjMetadata) (
			ClusterReader, error) {
			return testutil.NewNoopClusterReader(), nil
		},
		StatusReadersFactoryFunc: func(_ ClusterReader, _ meta.RESTMapper) (
			statusReaders map[schema.GroupKind]StatusReader, defaultStatusReader StatusReader) {
			return make(map[schema.GroupKind]StatusReader), nil
		},
	}

	eventChannel := engine.Poll(ctx, identifiers, options)

	for {
		select {
		case <-eventChannel:
			timer.Stop()
			return
		case <-timer.C:
			t.Errorf("expected runner to time out, but it didn't")
			return
		}
	}
}

func TestNewStatusPollerRunnerIdentifierValidation(t *testing.T) {
	testCases := map[string]struct{
		identifier object.ObjMetadata
		expectedErrMsg string
	} {
		"namespace-scoped resource without namespace": {
			identifier: object.ObjMetadata{
				GroupKind: schema.GroupKind{
					Group: "apps",
					Kind:  "Deployment",
				},
				Name: "foo",
			},
			expectedErrMsg: "is namespace scoped, but namespace is not set",
		},
		"cluster-scoped resource with namespace": {
			identifier: object.ObjMetadata{
				GroupKind: schema.GroupKind{
					Group: "",
					Kind:  "PersistentVolume",
				},
				Name: "foo",
				Namespace: "default",
			},
			expectedErrMsg: "ffdsaf",
		},
	}

	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {
			identifiers := []object.ObjMetadata{tc.identifier}

			engine := PollerEngine{
				Mapper: fakemapper.NewFakeRESTMapper(
					appsv1.SchemeGroupVersion.WithKind("Deployment"),
					fakemapper.GVKWithScope(
						v1.SchemeGroupVersion.WithKind("PersistentVolume"),
						meta.RESTScopeNameRoot,
					),
				),
			}

			eventChannel := engine.Poll(context.Background(), identifiers, Options{
				ClusterReaderFactoryFunc: func(_ client.Reader, _ meta.RESTMapper, _ []object.ObjMetadata) (
					ClusterReader, error) {
					return testutil.NewNoopClusterReader(), nil
				},
				StatusReadersFactoryFunc: func(_ ClusterReader, _ meta.RESTMapper) (
					statusReaders map[schema.GroupKind]StatusReader, defaultStatusReader StatusReader) {
					return make(map[schema.GroupKind]StatusReader), nil
				},
			})

			timer := time.NewTimer(3 * time.Second)
			defer timer.Stop()
			select {
			case e := <-eventChannel:
				if e.EventType != event.ErrorEvent {
					t.Errorf("expected an error event, but got %s", e.EventType.String())
					return
				}
				err := e.Error
				if !strings.Contains(err.Error(), tc.expectedErrMsg) {
					t.Errorf("expected error with message %s, but got %v", tc.expectedErrMsg, err)
				}
				return
			case <-timer.C:
				t.Errorf("expected an error event, but didn't get one")
			}
		})
	}
}

type fakeStatusReader struct {
	resourceStatuses    map[schema.GroupKind][]status.Status
	resourceStatusCount map[schema.GroupKind]int
}

func (f *fakeStatusReader) ReadStatus(_ context.Context, identifier object.ObjMetadata) *event.ResourceStatus {
	count := f.resourceStatusCount[identifier.GroupKind]
	resourceStatusSlice := f.resourceStatuses[identifier.GroupKind]
	var resourceStatus status.Status
	if len(resourceStatusSlice) > count {
		resourceStatus = resourceStatusSlice[count]
	} else {
		resourceStatus = resourceStatusSlice[len(resourceStatusSlice)-1]
	}
	f.resourceStatusCount[identifier.GroupKind] = count + 1
	return &event.ResourceStatus{
		Identifier: identifier,
		Status:     resourceStatus,
	}
}

func (f *fakeStatusReader) ReadStatusForObject(_ context.Context, _ *unstructured.Unstructured) *event.ResourceStatus {
	return nil
}
