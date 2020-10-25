// Copyright 2020 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package factory

import (
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubectl/pkg/validation"
)

type Factory interface {
	ToRESTConfig() (*rest.Config, error)
	ToDiscoveryClient() (discovery.CachedDiscoveryInterface, error)
	ToRESTMapper() (meta.RESTMapper, error)
	ToRawKubeConfigLoader() clientcmd.ClientConfig

	DynamicClient() (dynamic.Interface, error)
	Validator(validate bool) (validation.Schema, error)
	NewBuilder() *resource.Builder
	UnstructuredClientForMapping(mapping *meta.RESTMapping) (resource.RESTClient, error)
}
