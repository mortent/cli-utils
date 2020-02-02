package observe

import (
	"context"
	"fmt"
	"testing"
	"time"

	"gotest.tools/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe/common"
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe/observers"
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe/testutil"
	"sigs.k8s.io/cli-utils/pkg/kstatus/status"
	"sigs.k8s.io/cli-utils/pkg/kstatus/wait"
)

func TestStatusObserverRunner(t *testing.T) {
	testCases := map[string]struct {
		identifiers        []wait.ResourceIdentifier
		defaultObserver    observers.ResourceObserver
		expectedEventTypes []EventType
	}{
		"no resources": {
			identifiers:        []wait.ResourceIdentifier{},
			expectedEventTypes: []EventType{CompletedEvent},
		},
		"single resource": {
			identifiers: []wait.ResourceIdentifier{
				{
					GroupKind: schema.GroupKind{
						Group: "apps",
						Kind:  "Deployment",
					},
					Name:      "foo",
					Namespace: "bar",
				},
			},
			defaultObserver: &fakeObserver{
				resourceObservations: map[schema.GroupKind][]status.Status{
					schema.GroupKind{Group: "apps", Kind: "Deployment"}: { //nolint:gofmt
						status.InProgressStatus,
						status.CurrentStatus,
					},
				},
				resourceObservationCount: make(map[schema.GroupKind]int),
			},
			expectedEventTypes: []EventType{
				ResourceUpdateEvent,
				ResourceUpdateEvent,
				CompletedEvent,
			},
		},
		"multiple resources": {
			identifiers: []wait.ResourceIdentifier{
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
			defaultObserver: &fakeObserver{
				resourceObservations: map[schema.GroupKind][]status.Status{
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
				resourceObservationCount: make(map[schema.GroupKind]int),
			},
			expectedEventTypes: []EventType{
				ResourceUpdateEvent,
				ResourceUpdateEvent,
				ResourceUpdateEvent,
				ResourceUpdateEvent,
				CompletedEvent,
			},
		},
	}

	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {
			ctx := context.Background()

			fakeReader := testutil.NewNoopObserverReader()
			fakeMapper := testutil.NewFakeRESTMapper()
			identifiers := tc.identifiers
			eventChannel := make(chan Event)

			runner := newStatusObserverRunner(ctx, fakeReader, fakeMapper, identifiers, eventChannel, true)

			// Remove all observers for testing. We only set a custom defaultObserver
			// to make testing easier.
			runner.observers = make(map[schema.GroupKind]observers.ResourceObserver)
			runner.defaultObserver = tc.defaultObserver

			go runner.Run()

			var eventTypes []EventType
			for ch := range eventChannel {
				eventTypes = append(eventTypes, ch.EventType)
			}

			assert.DeepEqual(t, tc.expectedEventTypes, eventTypes)
		})
	}
}

func TestNewStatusObserverRunnerCancellation(t *testing.T) {
	fakeReader := testutil.NewNoopObserverReader()
	fakeMapper := testutil.NewFakeRESTMapper()
	identifiers := make([]wait.ResourceIdentifier, 0)
	eventChannel := make(chan Event)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	runner := newStatusObserverRunner(ctx, fakeReader, fakeMapper, identifiers, eventChannel, false)

	timer := time.NewTimer(5 * time.Second)

	go runner.Run()

	var lastEvent Event
	for {
		select {
		case event, more := <-eventChannel:
			timer.Stop()
			if more {
				lastEvent = event
			} else {
				if want, got := AbortedEvent, lastEvent.EventType; got != want {
					t.Errorf("Expected event to have type %s, but got %s", want, got)
				}
				return
			}
		case <-timer.C:
			t.Errorf("expected runner to time out, but it didn't")
			return
		}
	}
}

func TestDeepEqual(t *testing.T) {
	testCases := map[string]struct {
		actual   common.ObservedResource
		expected common.ObservedResource
		equal    bool
	}{
		"same resource should be equal": {
			actual: common.ObservedResource{
				Identifier: wait.ResourceIdentifier{
					GroupKind: schema.GroupKind{
						Group: "apps",
						Kind:  "Deployment",
					},
					Namespace: "default",
					Name:      "Foo",
				},
				Status:  status.UnknownStatus,
				Message: "Some message",
			},
			expected: common.ObservedResource{
				Identifier: wait.ResourceIdentifier{
					GroupKind: schema.GroupKind{
						Group: "apps",
						Kind:  "Deployment",
					},
					Namespace: "default",
					Name:      "Foo",
				},
				Status:  status.UnknownStatus,
				Message: "Some message",
			},
			equal: true,
		},
		"different resources with only name different": {
			actual: common.ObservedResource{
				Identifier: wait.ResourceIdentifier{
					GroupKind: schema.GroupKind{
						Group: "apps",
						Kind:  "Deployment",
					},
					Namespace: "default",
					Name:      "Foo",
				},
				Status: status.CurrentStatus,
			},
			expected: common.ObservedResource{
				Identifier: wait.ResourceIdentifier{
					GroupKind: schema.GroupKind{
						Group: "apps",
						Kind:  "Deployment",
					},
					Namespace: "default",
					Name:      "Bar",
				},
				Status: status.CurrentStatus,
			},
			equal: false,
		},
		"different GroupKind otherwise same": {
			actual: common.ObservedResource{
				Identifier: wait.ResourceIdentifier{
					GroupKind: schema.GroupKind{
						Group: "apps",
						Kind:  "Deployment",
					},
					Namespace: "default",
					Name:      "Bar",
				},
				Status: status.CurrentStatus,
			},
			expected: common.ObservedResource{
				Identifier: wait.ResourceIdentifier{
					GroupKind: schema.GroupKind{
						Group: "custom.io",
						Kind:  "Deployment",
					},
					Namespace: "default",
					Name:      "Bar",
				},
				Status: status.CurrentStatus,
			},
			equal: false,
		},
		"same resource with same error": {
			actual: common.ObservedResource{
				Identifier: wait.ResourceIdentifier{
					GroupKind: schema.GroupKind{
						Group: "apps",
						Kind:  "Deployment",
					},
					Namespace: "default",
					Name:      "Bar",
				},
				Status: status.UnknownStatus,
				Error:  fmt.Errorf("this is a test"),
			},
			expected: common.ObservedResource{
				Identifier: wait.ResourceIdentifier{
					GroupKind: schema.GroupKind{
						Group: "apps",
						Kind:  "Deployment",
					},
					Namespace: "default",
					Name:      "Bar",
				},
				Status: status.UnknownStatus,
				Error:  fmt.Errorf("this is a test"),
			},
			equal: true,
		},
		"same resource different status": {
			actual: common.ObservedResource{
				Identifier: wait.ResourceIdentifier{
					GroupKind: schema.GroupKind{
						Group: "apps",
						Kind:  "Deployment",
					},
					Namespace: "default",
					Name:      "Bar",
				},
				Status: status.CurrentStatus,
			},
			expected: common.ObservedResource{
				Identifier: wait.ResourceIdentifier{
					GroupKind: schema.GroupKind{
						Group: "apps",
						Kind:  "Deployment",
					},
					Namespace: "default",
					Name:      "Bar",
				},
				Status: status.InProgressStatus,
			},
			equal: false,
		},
		"same resource with different number of generated resources": {
			actual: common.ObservedResource{
				Identifier: wait.ResourceIdentifier{
					GroupKind: schema.GroupKind{
						Group: "apps",
						Kind:  "Deployment",
					},
					Namespace: "default",
					Name:      "Bar",
				},
				Status: status.InProgressStatus,
				GeneratedResources: []*common.ObservedResource{
					{
						Identifier: wait.ResourceIdentifier{
							GroupKind: schema.GroupKind{
								Group: "apps",
								Kind:  "ReplicaSet",
							},
							Namespace: "default",
							Name:      "Bar-123",
						},
						Status: status.InProgressStatus,
					},
				},
			},
			expected: common.ObservedResource{
				Identifier: wait.ResourceIdentifier{
					GroupKind: schema.GroupKind{
						Group: "apps",
						Kind:  "Deployment",
					},
					Namespace: "default",
					Name:      "Bar",
				},
				Status: status.InProgressStatus,
			},
			equal: false,
		},
		"same resource with different status on generated resources": {
			actual: common.ObservedResource{
				Identifier: wait.ResourceIdentifier{
					GroupKind: schema.GroupKind{
						Group: "apps",
						Kind:  "Deployment",
					},
					Namespace: "default",
					Name:      "Bar",
				},
				Status: status.InProgressStatus,
				GeneratedResources: []*common.ObservedResource{
					{
						Identifier: wait.ResourceIdentifier{
							GroupKind: schema.GroupKind{
								Group: "apps",
								Kind:  "ReplicaSet",
							},
							Namespace: "default",
							Name:      "Bar-123",
						},
						Status: status.InProgressStatus,
					},
				},
			},
			expected: common.ObservedResource{
				Identifier: wait.ResourceIdentifier{
					GroupKind: schema.GroupKind{
						Group: "apps",
						Kind:  "Deployment",
					},
					Namespace: "default",
					Name:      "Bar",
				},
				Status: status.InProgressStatus,
				GeneratedResources: []*common.ObservedResource{
					{
						Identifier: wait.ResourceIdentifier{
							GroupKind: schema.GroupKind{
								Group: "apps",
								Kind:  "ReplicaSet",
							},
							Namespace: "default",
							Name:      "Bar-123",
						},
						Status: status.CurrentStatus,
					},
				},
			},
			equal: false,
		},
	}

	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {
			res := DeepEqual(&tc.actual, &tc.expected)

			assert.Equal(t, tc.equal, res)
		})
	}
}

type fakeObserver struct {
	resourceObservations     map[schema.GroupKind][]status.Status
	resourceObservationCount map[schema.GroupKind]int
}

func (f *fakeObserver) Observe(_ context.Context, identifier wait.ResourceIdentifier) *common.ObservedResource {
	count := f.resourceObservationCount[identifier.GroupKind]
	observedResourceStatusSlice := f.resourceObservations[identifier.GroupKind]
	var observedResourceStatus status.Status
	if len(observedResourceStatusSlice) > count {
		observedResourceStatus = observedResourceStatusSlice[count]
	} else {
		observedResourceStatus = observedResourceStatusSlice[len(observedResourceStatusSlice)-1]
	}
	f.resourceObservationCount[identifier.GroupKind] = count + 1
	return &common.ObservedResource{
		Identifier: identifier,
		Status:     observedResourceStatus,
	}
}

func (f *fakeObserver) ObserveObject(_ context.Context, _ *unstructured.Unstructured) *common.ObservedResource {
	return nil
}

func (f *fakeObserver) SetComputeStatusFunc(_ observers.ComputeStatusFunc) {}
