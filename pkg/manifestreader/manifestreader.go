// Copyright 2020 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package manifestreader

import (
	"k8s.io/cli-runtime/pkg/resource"
	"sigs.k8s.io/cli-utils/pkg/factory"
)

// ManifestReader defines the interface for reading a set
// of manifests into info objects.
type ManifestReader interface {
	Read() ([]*resource.Info, error)
}

// ReaderOptions defines the shared inputs for the different
// implementations of the ManifestReader interface.
type ReaderOptions struct {
	Factory          factory.Factory
	Validate         bool
	Namespace        string
	EnforceNamespace bool
}
