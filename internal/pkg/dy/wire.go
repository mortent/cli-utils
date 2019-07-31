//+build wireinject

/*
Copyright 2019 The Kubernetes Authors.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dy

import (
	"io"

	"github.com/google/wire"
	"sigs.k8s.io/cli-utils/internal/pkg/util"
	"sigs.k8s.io/cli-utils/internal/pkg/wirecli"
)

// InitializeCommandLister creates a new *CommandLister object
func InitializeCommandBuilder(io.Writer, util.Args) (*CommandBuilder, error) {
	panic(wire.Build(ProviderSet, wirecli.ProviderSet))
}