package info

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/client-go/rest"
	"k8s.io/kubectl/pkg/cmd/util"
)

type InfoHelper struct {
	Factory util.Factory
}

func (ih *InfoHelper) FixInfos(infos []*resource.Info) error {
	mapper, err := ih.Factory.ToRESTMapper()
	if err != nil {
		return err
	}
	for _, info := range infos {
		gvk := info.Object.GetObjectKind().GroupVersionKind()
		mapping, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if err != nil {
			return err
		}
		info.Mapping = mapping

		c, err := ih.getClient(gvk.GroupVersion())
		if err != nil {
			return err
		}
		info.Client = c
	}
	return nil
}

func (ih *InfoHelper) getClient(gv schema.GroupVersion) (*rest.RESTClient, error) {
	cfg, err := ih.Factory.ToRESTConfig()
	if err != nil {
		return nil, err
	}
	cfg.ContentConfig = resource.UnstructuredPlusDefaultContentConfig()
	cfg.GroupVersion = &gv
	if len(gv.Group) == 0 {
		cfg.APIPath = "/api"
	} else {
		cfg.APIPath = "/apis"
	}

	return rest.RESTClientFor(cfg)
}