// Copyright 2020 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package testutil

import (
	"fmt"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type gvkWithScope struct {
	gvk schema.GroupVersionKind
	scope meta.RESTScopeName
}

func GVKWithScope(gvk schema.GroupVersionKind, scope meta.RESTScopeName) gvkWithScope {
	return gvkWithScope{
		gvk: gvk,
		scope: scope,
	}
}

func NewFakeRESTMapper(objs ...interface{}) meta.RESTMapper {
	gvkScopes := toGVKsWithScope(objs)
	var groupVersions []schema.GroupVersion
	for _, gvkScope := range gvkScopes {
		groupVersions = append(groupVersions, gvkScope.gvk.GroupVersion())
	}
	mapper := meta.NewDefaultRESTMapper(groupVersions)
	for _, gvkScope := range gvkScopes {
		if gvkScope.scope == meta.RESTScopeNameNamespace {
			mapper.Add(gvkScope.gvk, meta.RESTScopeNamespace)
		} else {
			mapper.Add(gvkScope.gvk, meta.RESTScopeRoot)
		}
	}
	return mapper
}

func toGVKsWithScope(objs []interface{}) []gvkWithScope {
	var c []gvkWithScope
	for _, obj := range objs {
		switch t := obj.(type) {
		case schema.GroupVersionKind:
			c = append(c, gvkWithScope{
				gvk: obj.(schema.GroupVersionKind),
				scope: meta.RESTScopeNameNamespace,
			})
		case gvkWithScope:
			c = append(c, obj.(gvkWithScope))
		default:
			panic(fmt.Errorf("unsupported type %v", t))
		}
	}
	return c
}

