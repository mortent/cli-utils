package reader

import (
	"context"
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/cli-utils/pkg/kstatus/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// genGroupKinds captures the generated resources for a given GroupKind. This is used
// to expand the list GVK and namespace combinations that needs to be synced in the cache.
// TODO: These should probably be defined in the observers rather than here.
var genGroupKinds = map[schema.GroupKind][]schema.GroupKind{
	schema.GroupKind{Group: "apps", Kind: "Deployment"}: { //nolint:gofmt
		{
			Group: "apps",
			Kind:  "ReplicaSet",
		},
	},
	schema.GroupKind{Group: "apps", Kind: "ReplicaSet"}: { //nolint:gofmt
		{
			Group: "",
			Kind:  "Pod",
		},
	},
	schema.GroupKind{Group: "apps", Kind: "StatefulSet"}: { //nolint:gofmt
		{
			Group: "",
			Kind:  "Pod",
		},
	},
}

// NewCachedObserverReader returns a new instance of the ObserverReader.
func NewCachedObserverReader(reader client.Reader, mapper meta.RESTMapper, identifiers []wait.ResourceIdentifier) (*CachedObserverReader, error) {
	gvkNamespaceSet := newGnSet()
	for _, id := range identifiers {
		err := buildGvkNamespaceSet(mapper, []schema.GroupKind{id.GroupKind}, id.Namespace, gvkNamespaceSet)
		if err != nil {
			return nil, err
		}
	}

	return &CachedObserverReader{
		reader: reader,
		mapper: mapper,
		gns:    gvkNamespaceSet.gvkNamespaces,
	}, nil
}

func buildGvkNamespaceSet(mapper meta.RESTMapper, gks []schema.GroupKind, namespace string, gvkNamespaceSet *gvkNamespaceSet) error {
	for _, gk := range gks {
		mapping, err := mapper.RESTMapping(gk)
		if err != nil {
			return err
		}
		gvkNamespaceSet.add(gvkNamespace{
			GVK:       mapping.GroupVersionKind,
			Namespace: namespace,
		})
		genGKs, found := genGroupKinds[gk]
		if found {
			err := buildGvkNamespaceSet(mapper, genGKs, namespace, gvkNamespaceSet)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type gvkNamespaceSet struct {
	gvkNamespaces []gvkNamespace
	seen          map[gvkNamespace]bool
}

func newGnSet() *gvkNamespaceSet {
	return &gvkNamespaceSet{
		gvkNamespaces: make([]gvkNamespace, 0),
		seen:          make(map[gvkNamespace]bool),
	}
}

func (g *gvkNamespaceSet) add(gn gvkNamespace) {
	if _, found := g.seen[gn]; !found {
		g.gvkNamespaces = append(g.gvkNamespaces, gn)
		g.seen[gn] = true
	}
}

// CachedObserverReader is an implementation of the ObserverReader interface that will
// pre-fetch all resources needed before every sync loop. The resources needed are decided by
// finding all combinations of GroupVersionKind and namespace referenced by the provided
// identifiers. This list is then expanded to include any known generated resource types.
type CachedObserverReader struct {
	sync.RWMutex

	reader client.Reader

	mapper meta.RESTMapper

	gns []gvkNamespace

	cache map[gvkNamespace]unstructured.UnstructuredList
}

// gvkNamespace contains information about a GroupVersionKind and a namespace.
type gvkNamespace struct {
	GVK       schema.GroupVersionKind
	Namespace string
}

// Get looks up the resource identified by the key and the object GVK in the cache. If the needed combination
// of GVK and namespace is not part of the cache, that is considered an error.
func (c *CachedObserverReader) Get(_ context.Context, key client.ObjectKey, obj *unstructured.Unstructured) error {
	c.RLock()
	defer c.RUnlock()
	gvk := obj.GetObjectKind().GroupVersionKind()
	mapping, err := c.mapper.RESTMapping(gvk.GroupKind())
	if err != nil {
		return err
	}
	gn := gvkNamespace{
		GVK:       gvk,
		Namespace: key.Namespace,
	}
	cacheList, found := c.cache[gn]
	if !found {
		return fmt.Errorf("GVK %s and Namespace %s not found in cache", gvk.String(), gn.Namespace)
	}
	for _, u := range cacheList.Items {
		if u.GetName() == key.Name {
			obj.Object = u.Object
			return nil
		}
	}
	return errors.NewNotFound(mapping.Resource.GroupResource(), key.Name)
}

// ListNamespaceScoped lists all resource identifier by the GVK of the list, the namespace and the selector
// from the cache. If the needed combination of GVK and namespace is not part of the cache, that is considered an error.
func (c *CachedObserverReader) ListNamespaceScoped(_ context.Context, list *unstructured.UnstructuredList, namespace string, selector labels.Selector) error {
	c.RLock()
	defer c.RUnlock()
	gvk := list.GroupVersionKind()
	gn := gvkNamespace{
		GVK:       gvk,
		Namespace: namespace,
	}

	cacheList, found := c.cache[gn]
	if !found {
		return fmt.Errorf("GVK %s and Namespace %s not found in cache", gvk.String(), gn.Namespace)
	}

	var items []unstructured.Unstructured
	for _, u := range cacheList.Items {
		if selector.Matches(labels.Set(u.GetLabels())) {
			items = append(items, u)
		}
	}
	list.Items = items
	return nil
}

// ListNamespaceScoped lists all resource identifier by the GVK of the list and selector
// from the cache. If the needed combination of GVK and namespace is not part of the cache,
// that is considered an error.
func (c *CachedObserverReader) ListClusterScoped(ctx context.Context, list *unstructured.UnstructuredList, selector labels.Selector) error {
	return c.ListNamespaceScoped(ctx, list, "", selector)
}

// Sync loops over the list of gvkNamespace we know of, and uses list calls to fetch the resources.
// This information populates the cache.
func (c *CachedObserverReader) Sync(ctx context.Context) error {
	c.Lock()
	defer c.Unlock()
	cache := make(map[gvkNamespace]unstructured.UnstructuredList)
	for _, gn := range c.gns {
		mapping, err := c.mapper.RESTMapping(gn.GVK.GroupKind())
		if err != nil {
			return err
		}
		var listOptions []client.ListOption
		if mapping.Scope == meta.RESTScopeNamespace {
			listOptions = append(listOptions, client.InNamespace(gn.Namespace))
		}
		var list unstructured.UnstructuredList
		list.SetGroupVersionKind(gn.GVK)
		err = c.reader.List(ctx, &list, listOptions...)
		if err != nil {
			return err
		}
		cache[gn] = list
	}
	c.cache = cache
	return nil
}
