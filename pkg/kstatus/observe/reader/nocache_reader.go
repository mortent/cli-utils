package reader

import (
	"context"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NoCacheObserverReader is an implementation of the ObserverReader that just delegates all calls directly to
// the underlying reader. No caching.
type NoCacheObserverReader struct {
	Reader client.Reader
}

func (n *NoCacheObserverReader) Get(ctx context.Context, key client.ObjectKey, obj *unstructured.Unstructured) error {
	return n.Reader.Get(ctx, key, obj)
}

func (n *NoCacheObserverReader) ListNamespaceScoped(ctx context.Context, list *unstructured.UnstructuredList, namespace string, selector labels.Selector) error {
	return n.Reader.List(ctx, list, client.InNamespace(namespace), client.MatchingLabelsSelector{Selector: selector})
}

func (n *NoCacheObserverReader) ListClusterScoped(ctx context.Context, list *unstructured.UnstructuredList, selector labels.Selector) error {
	return n.Reader.List(ctx, list, client.MatchingLabelsSelector{Selector: selector})
}

func (n *NoCacheObserverReader) Sync(_ context.Context) error {
	return nil
}
