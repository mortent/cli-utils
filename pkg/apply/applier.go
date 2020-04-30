// Copyright 2019 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package apply

import (
	"context"
	"fmt"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/restmapper"
	"sigs.k8s.io/cli-utils/pkg/apply/info"
	"sort"
	"time"

	"github.com/go-errors/errors"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/kubectl/pkg/cmd/apply"
	"k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/scheme"
	"sigs.k8s.io/cli-utils/pkg/apply/event"
	"sigs.k8s.io/cli-utils/pkg/apply/poller"
	"sigs.k8s.io/cli-utils/pkg/apply/prune"
	"sigs.k8s.io/cli-utils/pkg/apply/solver"
	"sigs.k8s.io/cli-utils/pkg/apply/taskrunner"
	"sigs.k8s.io/cli-utils/pkg/common"
	"sigs.k8s.io/cli-utils/pkg/kstatus/polling"
	"sigs.k8s.io/cli-utils/pkg/object"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// newApplier returns a new Applier. It will set up the ApplyOptions and
// StatusOptions which are responsible for capturing any command line flags.
// It currently requires IOStreams, but this is a legacy from when
// the ApplyOptions were responsible for printing progress. This is now
// handled by a separate printer with the KubectlPrinterAdapter bridging
// between the two.
func NewApplier(factory util.Factory, ioStreams genericclioptions.IOStreams) *Applier {
	applyOptions := apply.NewApplyOptions(ioStreams)
	return &Applier{
		ApplyOptions: applyOptions,
		// VisitedUids keeps track of the unique identifiers for all
		// currently applied objects. Used to calculate prune set.
		PruneOptions: prune.NewPruneOptions(applyOptions.VisitedUids),
		factory:      factory,
		ioStreams:    ioStreams,
	}
}

// Applier performs the step of applying a set of resources into a cluster,
// conditionally waits for all of them to be fully reconciled and finally
// performs prune to clean up any resources that has been deleted.
// The applier performs its function by executing a list queue of tasks,
// each of which is one of the steps in the process of applying a set
// of resources to the cluster. The actual execution of these tasks are
// handled by a StatusRunner. So the taskqueue is effectively a
// specification that is executed by the StatusRunner. Based on input
// parameters and/or the set of resources that needs to be applied to the
// cluster, different sets of tasks might be needed.
type Applier struct {
	factory   util.Factory
	ioStreams genericclioptions.IOStreams

	ApplyOptions *apply.ApplyOptions
	PruneOptions *prune.PruneOptions
	statusPoller poller.Poller
}

// Initialize sets up the Applier for actually doing an apply against
// a cluster. This involves validating command line inputs and configuring
// clients for communicating with the cluster.
func (a *Applier) Initialize(cmd *cobra.Command, paths []string) error {
	fileNameFlags, err := common.DemandOneDirectory(paths)
	if err != nil {
		return err
	}
	a.ApplyOptions.DeleteFlags.FileNameFlags = &fileNameFlags
	err = a.ApplyOptions.Complete(a.factory, cmd)
	if err != nil {
		return errors.WrapPrefix(err, "error setting up ApplyOptions", 1)
	}
	a.ApplyOptions.PostProcessorFn = nil // Turn off the default kubectl pruning
	err = a.PruneOptions.Initialize(a.factory)
	if err != nil {
		return errors.WrapPrefix(err, "error setting up PruneOptions", 1)
	}

	statusPoller, err := a.newStatusPoller()
	if err != nil {
		return errors.WrapPrefix(err, "error creating resolver", 1)
	}
	a.statusPoller = statusPoller
	return nil
}

// SetFlags configures the command line flags needed for apply and
// status. This is a temporary solution as we should separate the configuration
// of cobra flags from the Applier.
func (a *Applier) SetFlags(cmd *cobra.Command) error {
	a.ApplyOptions.DeleteFlags.AddFlags(cmd)
	for _, flag := range []string{"kustomize", "filename", "recursive"} {
		err := cmd.Flags().MarkHidden(flag)
		if err != nil {
			return err
		}
	}
	a.ApplyOptions.RecordFlags.AddFlags(cmd)
	_ = cmd.Flags().MarkHidden("record")
	_ = cmd.Flags().MarkHidden("cascade")
	_ = cmd.Flags().MarkHidden("force")
	_ = cmd.Flags().MarkHidden("grace-period")
	_ = cmd.Flags().MarkHidden("timeout")
	_ = cmd.Flags().MarkHidden("wait")
	a.ApplyOptions.Overwrite = true
	return nil
}

// newStatusPoller sets up a new StatusPoller for computing status. The configuration
// needed for the poller is taken from the Factory.
func (a *Applier) newStatusPoller() (poller.Poller, error) {
	config, err := a.factory.ToRESTConfig()
	if err != nil {
		return nil, errors.WrapPrefix(err, "error getting RESTConfig", 1)
	}

	discoveryClient, err := a.factory.ToDiscoveryClient()
	if err != nil {
		return nil, err
	}
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(discoveryClient)

	c, err := client.New(config, client.Options{Scheme: scheme.Scheme, Mapper: mapper})
	if err != nil {
		return nil, errors.WrapPrefix(err, "error creating client", 1)
	}

	return polling.NewStatusPoller(c, mapper), nil
}

func (a *Applier) readingObjects() ([]*resource.Info, error) {
	r := a.ApplyOptions.Builder.
		Local().
		Unstructured().
		Schema(a.ApplyOptions.Validator).
		ContinueOnError().
		NamespaceParam(a.ApplyOptions.Namespace).DefaultNamespace().
		FilenameParam(a.ApplyOptions.EnforceNamespace, &a.ApplyOptions.DeleteOptions.FilenameOptions).
		LabelSelectorParam(a.ApplyOptions.Selector).
		Flatten().
		Do()
	if err := r.Err(); err != nil {
		return nil, err
	}

	var offlineInfos []*resource.Info
	err := r.Visit(func(info *resource.Info, err error) error {
		gvk := info.Object.GetObjectKind().GroupVersionKind()
		accessor, err := meta.Accessor(info.Object)
		if err != nil {
			return err
		}
		fmt.Printf("Resource: %s %s %s\n", gvk.GroupKind().String(), accessor.GetName(), accessor.GetNamespace())
		offlineInfos = append(offlineInfos, info)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return offlineInfos, nil
}

// readAndPrepareObjects reads the resources that should be applied,
// handles ordering of resources and sets up the grouping object
// based on the provided grouping object template.
func (a *Applier) readAndPrepareObjects() ([]*resource.Info, error) {
	infos, err := a.readingObjects()
	if err != nil {
		return nil, err
	}

	resources, gots := splitInfos(infos)

	if len(gots) == 0 {
		return nil, prune.NoInventoryObjError{}
	}
	if len(gots) > 1 {
		return nil, prune.MultipleInventoryObjError{
			InventoryObjectTemplates: gots,
		}
	}

	groupingObject, err := prune.CreateInventoryObj(gots[0], resources)
	if err != nil {
		return nil, err
	}

	sort.Sort(ResourceInfos(resources))

	if !validateNamespace(resources) {
		return nil, fmt.Errorf("objects have differing namespaces")
	}

	return append([]*resource.Info{groupingObject}, resources...), nil
}

// splitInfos takes a slice of resource.Info objects and splits it
// into one slice that contains the grouping object templates and
// another one that contains the remaining resources.
func splitInfos(infos []*resource.Info) ([]*resource.Info, []*resource.Info) {
	groupingObjectTemplates := make([]*resource.Info, 0)
	resources := make([]*resource.Info, 0)

	for _, info := range infos {
		if prune.IsInventoryObject(info.Object) {
			groupingObjectTemplates = append(groupingObjectTemplates, info)
		} else {
			resources = append(resources, info)
		}
	}
	return resources, groupingObjectTemplates
}

// Run performs the Apply step. This happens asynchronously with updates
// on progress and any errors are reported back on the event channel.
// Cancelling the operation or setting timeout on how long to Wait
// for it complete can be done with the passed in context.
// Note: There sn't currently any way to interrupt the operation
// before all the given resources have been applied to the cluster. Any
// cancellation or timeout will only affect how long we Wait for the
// resources to become current.
func (a *Applier) Run(ctx context.Context, options Options) <-chan event.Event {
	eventChannel := make(chan event.Event)
	setDefaults(&options)

	go func() {
		defer close(eventChannel)
		adapter := &KubectlPrinterAdapter{
			ch: eventChannel,
		}
		// The adapter is used to intercept what is meant to be printing
		// in the ApplyOptions, and instead turn those into events.
		a.ApplyOptions.ToPrinter = adapter.toPrinterFunc()

		// This provides us with a slice of all the objects that will be
		// applied to the cluster. This takes care of ordering resources
		// and handling the grouping object.
		infos, err := a.readAndPrepareObjects()
		if err != nil {
			handleError(eventChannel, err)
			return
		}

		// Extract the object metadata needed to identify each
		// of the resources. This is just a lightweight representation
		// of the resources in the infos struct. The status library
		// relies on identifiers rather than infos, so we need to use
		// both.
		identifiers := object.InfosToObjMetas(infos)

		// Fetch the queue (channel) of tasks that should be executed.
		taskQueue := (&solver.TaskQueueSolver{
			ApplyOptions: a.ApplyOptions,
			PruneOptions: a.PruneOptions,
			InfoHelper: &info.InfoHelper{
				Factory: a.factory,
			},
		}).BuildTaskQueue(infos, solver.Options{
			WaitForReconcile:        options.WaitForReconcile,
			WaitForReconcileTimeout: options.WaitTimeout,
			Prune:                   !options.NoPrune,
			DryRun:                  options.DryRun,
		})

		// Send event to inform the caller about the resources that
		// will be applied/pruned.
		eventChannel <- event.Event{
			Type: event.InitType,
			InitEvent: event.InitEvent{
				ResourceGroups: []event.ResourceGroup{
					{
						Action:      event.ApplyAction,
						Identifiers: identifiers,
					},
				},
			},
		}

		// Create a new TaskStatusRunner to execute the taskQueue.
		runner := taskrunner.NewTaskStatusRunner(identifiers, a.statusPoller)
		err = runner.Run(ctx, taskQueue, eventChannel, taskrunner.Options{
			PollInterval:     options.PollInterval,
			UseCache:         true,
			EmitStatusEvents: options.EmitStatusEvents,
		})
		if err != nil {
			handleError(eventChannel, err)
		}
	}()
	return eventChannel
}

type Options struct {
	// WaitForReconcile defines whether the applier should wait
	// until all applied resources have been reconciled before
	// pruning and exiting.
	WaitForReconcile bool

	// PollInterval defines how often we should poll for the status
	// of resources.
	PollInterval time.Duration

	// WaitTimeout defines how long we should wait for resources
	// to be reconciled before giving up.
	WaitTimeout time.Duration

	// EmitStatusEvents defines whether status events should be
	// emitted on the eventChannel to the caller.
	EmitStatusEvents bool

	// NoPrune defines whether pruning of previously applied
	// objects should happen after apply.
	NoPrune bool

	// DryRun defines whether changes should actually be performed,
	// or if it is just talk and no action.
	DryRun bool
}

// setDefaults set the options to the default values if they
// have not been provided.
func setDefaults(o *Options) {
	if o.PollInterval == time.Duration(0) {
		o.PollInterval = 2 * time.Second
	}
	if o.WaitTimeout == time.Duration(0) {
		o.WaitTimeout = time.Minute
	}
}

func handleError(eventChannel chan event.Event, err error) {
	eventChannel <- event.Event{
		Type: event.ErrorType,
		ErrorEvent: event.ErrorEvent{
			Err: err,
		},
	}
}

// validateNamespace returns true if all the objects in the passed
// infos parameter have the same namespace; false otherwise. Ignores
// cluster-scoped resources.
func validateNamespace(infos []*resource.Info) bool {
	currentNamespace := metav1.NamespaceNone
	for _, info := range infos {
		// Ignore cluster-scoped resources.
		if info.Namespaced() {
			// If the current namespace has not been set--then set it.
			if currentNamespace == metav1.NamespaceNone {
				currentNamespace = info.Namespace
			}
			if currentNamespace != info.Namespace {
				return false
			}
		}
	}
	return true
}
