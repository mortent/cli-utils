package status

import (
	"context"
	"time"

	"github.com/go-errors/errors"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/cli-utils/cmd/status/printers"
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe"
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe/collector"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/kustomize/kyaml/kio"
)

var (
	scheme = runtime.NewScheme()
)

//nolint:gochecknoinits
func init() {
	_ = clientgoscheme.AddToScheme(scheme)
}

func GetStatusRunner() *StatusRunner {
	r := &StatusRunner{}
	c := &cobra.Command{
		Use:  "status DIR...",
		RunE: r.runE,
	}
	c.Flags().BoolVar(&r.IncludeSubpackages, "include-subpackages", true,
		"also print resources from subpackages.")
	c.Flags().DurationVar(&r.Interval, "interval", 2*time.Second,
		"check every n seconds. Default is every 2 seconds.")
	c.Flags().DurationVar(&r.Timeout, "timeout", 60*time.Second,
		"give up after n seconds. Default is 60 seconds.")
	c.Flags().BoolVar(&r.StopOnComplete, "stop-on-complete", true,
		"exit when all resources have fully reconciled.")
	c.Flags().StringVar(&r.Output, "output", "table", "output format.")

	r.Command = c
	return r
}

func StatusCommand() *cobra.Command {
	return GetStatusRunner().Command
}

// WaitRunner captures the parameters for the command and contains
// the run function.
type StatusRunner struct {
	IncludeSubpackages bool
	Interval           time.Duration
	Timeout            time.Duration
	StopOnComplete     bool
	Now                bool
	Output             string
	Command            *cobra.Command
}

// runE implements the logic of the command and will call the Wait command in the wait
// package, use a ResourceStatusCollector to capture the events from the channel, and the
// TablePrinter to display the information.
func (r *StatusRunner) runE(c *cobra.Command, args []string) error {
	ctx := context.Background()

	config := ctrl.GetConfigOrDie()
	mapper, err := apiutil.NewDiscoveryRESTMapper(config)
	if err != nil {
		return errors.WrapPrefix(err, "error creating rest mapper", 1)
	}

	k8sClient, err := client.New(config, client.Options{Scheme: scheme, Mapper: mapper})
	if err != nil {
		return errors.WrapPrefix(err, "error creating client", 1)
	}

	observer := observe.NewStatusObserver(k8sClient, mapper)

	captureFilter := &CaptureIdentifiersFilter{
		Mapper: mapper,
	}
	filters := []kio.Filter{captureFilter}

	var inputs []kio.Reader
	for _, a := range args {
		inputs = append(inputs, kio.LocalPackageReader{
			PackagePath:        a,
			IncludeSubpackages: r.IncludeSubpackages,
		})
	}
	if len(inputs) == 0 {
		inputs = append(inputs, &kio.ByteReader{Reader: c.InOrStdin()})
	}

	err = kio.Pipeline{
		Inputs:  inputs,
		Filters: filters,
	}.Execute()
	if err != nil {
		return errors.WrapPrefix(err, "error reading manifests", 1)
	}

	coll := collector.NewObservedStatusCollector(captureFilter.Identifiers)
	stop := make(chan struct{})
	printer, err := printers.CreatePrinter(r.Output, coll, c.OutOrStdout())
	if err != nil {
		return errors.WrapPrefix(err, "error creating printer", 1)
	}
	printingFinished := printer.PrintUntil(stop)

	eventChannel := observer.Observe(ctx, captureFilter.Identifiers, r.StopOnComplete, true)
	completed := coll.Observe(eventChannel, stop)

	// Wait for the collector to finish. This will happen when the event channel is closed.
	<-completed
	// Close the stop channel to notify the printer that it should shut down.
	close(stop)
	// Wait for the printer to print the latest state before exiting the program.
	<-printingFinished
	return nil
}
