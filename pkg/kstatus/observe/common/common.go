package common

import (
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/cli-utils/pkg/kstatus/wait"
)

// GetNamespaceForNamespacedResource returns the namespace for the given object,
// but includes logic for returning the default namespace if it is not set.
func GetNamespaceForNamespacedResource(object runtime.Object) string {
	acc, err := meta.Accessor(object)
	if err != nil {
		panic(err)
	}
	ns := acc.GetNamespace()
	if ns == "" {
		return "default"
	}
	return ns
}

// KeyForNamespacedResource returns the object key for the given identifier. It makes
// sure to set the namespace to default if it is not provided.
func KeyForNamespacedResource(identifier wait.ResourceIdentifier) types.NamespacedName {
	namespace := "default"
	if identifier.Namespace != "" {
		namespace = identifier.Namespace
	}
	return types.NamespacedName{
		Name:      identifier.Name,
		Namespace: namespace,
	}
}
