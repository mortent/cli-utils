package observers

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe/reader"
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe/testutil"
)

func TestStatefulSetObserver(t *testing.T) {
	manifest := `
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: FooBar
spec:
  selector:
    matchLabels:
      app: foobar
  replicas: 3
`

	podManifest1 := `
apiVersion: v1
kind: Pod
metadata:
  name: Foobar-2
  labels:
    app: foobar
`

	podManifest2 := `
apiVersion: v1
kind: Pod
metadata:
  name: Foobar-1
  labels:
    app: foobar
`

	gvk := v1.SchemeGroupVersion.WithKind("Pod")

	generatedObjects := []unstructured.Unstructured{
		*testutil.YamlToUnstructured(t, podManifest1),
		*testutil.YamlToUnstructured(t, podManifest2),
	}

	var newRsObserverFunc newResourceObserverFunc = func(reader reader.ObserverReader, mapper meta.RESTMapper) ResourceObserver {
		return NewStatefulSetObserver(reader, mapper, &fakeResourceObserver{})
	}

	basicObserverTest(t, manifest, gvk, generatedObjects, newRsObserverFunc)
}
