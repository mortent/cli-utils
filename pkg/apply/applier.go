// Copyright 2019 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package apply

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/go-errors/errors"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/kubectl/pkg/cmd/apply"
	"k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/scheme"
	"sigs.k8s.io/cli-utils/pkg/apply/event"
	"sigs.k8s.io/cli-utils/pkg/apply/prune"
	"sigs.k8s.io/cli-utils/pkg/kstatus/polling"
	pollevent "sigs.k8s.io/cli-utils/pkg/kstatus/polling/event"
	"sigs.k8s.io/cli-utils/pkg/kstatus/status"
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
	return &Applier{
		ApplyOptions:  apply.NewApplyOptions(ioStreams),
		StatusOptions: NewStatusOptions(),
		PruneOptions:  prune.NewPruneOptions(),
		factory:       factory,
		ioStreams:     ioStreams,
	}
}

// poller defines the interface the applier needs to poll for status of resources.
type poller interface {
	Poll(ctx context.Context, identifiers []object.ObjMetadata, options polling.Options) <-chan pollevent.Event
}

// Applier performs the step of applying a set of resources into a cluster,
// conditionally waits for all of them to be fully reconciled and finally
// performs prune to clean up any resources that has been deleted.
type Applier struct {
	factory   util.Factory
	ioStreams genericclioptions.IOStreams

	ApplyOptions  *apply.ApplyOptions
	StatusOptions *StatusOptions
	PruneOptions  *prune.PruneOptions
	statusPoller  poller

	NoPrune bool
	DryRun  bool
}

// Initialize sets up the Applier for actually doing an apply against
// a cluster. This involves validating command line inputs and configuring
// clients for communicating with the cluster.
func (a *Applier) Initialize(cmd *cobra.Command, paths []string) error {
	fileNameFlags, err := demandOneDirectory(paths)
	if err != nil {
		return err
	}
	a.ApplyOptions.DeleteFlags.FileNameFlags = &fileNameFlags
	err = a.ApplyOptions.Complete(a.factory, cmd)
	if err != nil {
		return errors.WrapPrefix(err, "error setting up ApplyOptions", 1)
	}
	a.ApplyOptions.PreProcessorFn = prune.PrependGroupingObject(a.ApplyOptions)
	a.ApplyOptions.PostProcessorFn = nil // Turn off the default kubectl pruning
	err = a.PruneOptions.Initialize(a.factory)
	if err != nil {
		return errors.WrapPrefix(err, "error setting up PruneOptions", 1)
	}

	// Propagate dry-run flags.
	a.ApplyOptions.DryRun = a.DryRun
	a.PruneOptions.DryRun = a.DryRun

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
	a.StatusOptions.AddFlags(cmd)
	a.ApplyOptions.Overwrite = true
	return nil
}

// newStatusPoller sets up a new StatusPoller for computing status. The configuration
// needed for the poller is taken from the Factory.
func (a *Applier) newStatusPoller() (poller, error) {
	config, err := a.factory.ToRESTConfig()
	if err != nil {
		return nil, errors.WrapPrefix(err, "error getting RESTConfig", 1)
	}

	mapper, err := a.factory.ToRESTMapper()
	if err != nil {
		return nil, errors.WrapPrefix(err, "error getting RESTMapper", 1)
	}

	c, err := client.New(config, client.Options{Scheme: scheme.Scheme, Mapper: mapper})
	if err != nil {
		return nil, errors.WrapPrefix(err, "error creating client", 1)
	}

	return polling.NewStatusPoller(c, mapper), nil
}

// Run performs the Apply step. This happens asynchronously with updates
// on progress and any errors are reported back on the event channel.
// Cancelling the operation or setting timeout on how long to wait
// for it complete can be done with the passed in context.
// Note: There sn't currently any way to interrupt the operation
// before all the given resources have been applied to the cluster. Any
// cancellation or timeout will only affect how long we wait for the
// resources to become current.
func (a *Applier) Run(ctx context.Context) <-chan event.Event {
	ch := make(chan event.Event)

	go func() {
		defer close(ch)
		adapter := &KubectlPrinterAdapter{
			ch: ch,
		}
		// The adapter is used to intercept what is meant to be printing
		// in the ApplyOptions, and instead turn those into events.
		a.ApplyOptions.ToPrinter = adapter.toPrinterFunc()

		// Set the info objects on the ApplyOptions.
		infos, err := a.setInfos()
		if err != nil {
			ch <- event.Event{
				Type: event.ErrorType,
				ErrorEvent: event.ErrorEvent{
					Err: err,
				},
			}
			return
		}

		identifiers := infosToObjMetas(infos)

		//statusCtx, cancelFunc := context.WithCancel(context.Background())
		statusChannel := a.statusPoller.Poll(ctx, identifiers, polling.Options{
			PollUntilCancelled: true,
			PollInterval:       a.StatusOptions.period,
			UseCache:           true,
			DesiredStatus:      status.CurrentStatus,
		})

		tasks := []task{
			waitTask{
				identifiers: identifiers,
				condition: allKnown,
				timeout: 30 * time.Second,
			},
			applyTask{
				objects: infos,
			},
			waitTask{
				identifiers: identifiers,
				condition: allCurrent,
				timeout: 4 * time.Minute,
			},
			pruneTask{
				objects: infos,
			},
		}
		taskQueue := make(chan task, len(tasks))
		for _, t := range tasks {
			taskQueue <- t
		}

		aggregator := newAggregator(identifiers)

		taskChannel := make(chan taskResult)
		timeoutChannel := make(chan struct{})

		currentTask, done := a.nextTask(taskQueue, aggregator)
		if done {
			return
		}
		a.startTask(currentTask, taskChannel, timeoutChannel, ch)

		for {
			select {
			case statusEvent := <- statusChannel:
				fmt.Print("Message on status channel\n")
				ch <- event.Event{
					Type:        event.StatusType,
					StatusEvent: statusEvent,
				}
				aggregator.resourceStatus(statusEvent.Resource.Identifier, statusEvent.Resource.Status)
				if wt, ok := currentTask.task.(waitTask); ok {
					fmt.Print("We have a wait task\n")
					if aggregator.conditionMet(wt.identifiers, wt.condition) {
						fmt.Print("Condition met. Cancelling timeout\n")
						currentTask.timeoutHandle()
						currentTask, done = a.nextTask(taskQueue, aggregator)
						if done {
							return
						}
						a.startTask(currentTask, taskChannel, timeoutChannel, ch)
					}
				}
			case msg := <- taskChannel:
				fmt.Print("Message on task channel\n")
				if msg.err != nil {
					ch <- event.Event{
						Type: event.ErrorType,
						ErrorEvent: event.ErrorEvent{
							Err: msg.err,
						},
					}
					return
				}
				currentTask, done = a.nextTask(taskQueue, aggregator)
				if done {
					return
				}
				a.startTask(currentTask, taskChannel, timeoutChannel, ch)
			case <- timeoutChannel:
				fmt.Print("Message on timeout channel\n")
				ch <- event.Event{
					Type: event.ErrorType,
					ErrorEvent: event.ErrorEvent{
						Err: fmt.Errorf("timeout reached for task"),
					},
				}
				return
			}
		}
	}()
	return ch
}

func (a *Applier) nextTask(taskQueue chan task, aggregator *aggregator) (*runningTask, bool) {
	fmt.Print("Creating next task\n")
	for {
		select {
		case t := <-taskQueue:
			if wt, ok := t.(waitTask); ok {
				fmt.Print("Next task is a wait task\n")
				if aggregator.conditionMet(wt.identifiers, wt.condition) {
					fmt.Print("All conditions are already met\n")
					continue
				}
			}
			return &runningTask{
				task: t,
			}, false
		default:
			fmt.Print("No tasks left\n")
			return nil, true
		}
	}
}

func (a *Applier) startTask(rt *runningTask, taskChannel chan taskResult, timeoutChannel chan struct{}, eventChannel chan event.Event) {
	switch t := rt.task.(type) {
	case applyTask:
		fmt.Print("Starting apply task\n")
		a.ApplyOptions.SetObjects(t.objects)
		go func() {
			err := a.ApplyOptions.Run()
			taskChannel <- taskResult{
				err: err,
			}
		}()
	case pruneTask:
		fmt.Print("Starting prune task\n")
		go func() {
			err := a.PruneOptions.Prune(t.objects, eventChannel)
			taskChannel <- taskResult{
				err: err,
			}
		}()
	case waitTask:
		fmt.Print("Starting wait task\n")
		rt.timeoutHandle = setTimeout(t.timeout, timeoutChannel)
	}
}

type runningTask struct {
	task task
	timeoutHandle func()
}

type task interface{}

type applyTask struct{
	objects []*resource.Info
}

type pruneTask struct{
	objects []*resource.Info
}

type waitTask struct{
	identifiers []object.ObjMetadata
	condition   condition
	timeout time.Duration
}

type taskResult struct {
	err error
}

type condition string

const (
	allKnown    condition = "AllKnown"
	allCurrent  condition = "AllCurrent"
	allNotFound condition = "AllNotFound"
)

func setTimeout(timeout time.Duration, channel chan struct{}) func() {
	timer := time.NewTimer(timeout)
	go func() {
		<-timer.C
		fmt.Print("Timeout triggered\n")
		channel <- struct{}{}
	}()
	return func() {
		timer.Stop()
	}
}

func newAggregator(identifiers []object.ObjMetadata) *aggregator {
	rm := make(map[object.ObjMetadata]resourceInfo)

	for _, obj := range identifiers {
		rm[obj] = resourceInfo{
			Identifier: obj,
			CurrentStatus: status.UnknownStatus,
		}
	}
	return &aggregator{
		resourceMap: rm,
	}
}

type aggregator struct {
	resourceMap map[object.ObjMetadata]resourceInfo
}

type resourceInfo struct {
	Identifier object.ObjMetadata
	CurrentStatus status.Status
}

func (a *aggregator) resourceStatus(identifier object.ObjMetadata, s status.Status) {
	if ri, found := a.resourceMap[identifier]; found {
		ri.CurrentStatus = s
		a.resourceMap[identifier] = ri
	}
}

func (a *aggregator) conditionMet(identifiers []object.ObjMetadata, c condition) bool {
	switch c {
	case allCurrent:
		return a.allMatchStatus(identifiers, status.CurrentStatus)
	case allNotFound:
		return a.allMatchStatus(identifiers, status.NotFoundStatus)
	default:
		return a.noneMatchStatus(identifiers, status.UnknownStatus)
	}
}

func (a *aggregator) allMatchStatus(identifiers []object.ObjMetadata, s status.Status) bool {
	fmt.Print("Checking all match status\n")
	for id, ri := range a.resourceMap {
		if contains(identifiers, id) {
			if ri.CurrentStatus != s {
				fmt.Printf("Resource %s has status %s\n", id.GroupKind.String(), ri.CurrentStatus.String())
				return false
			}
		}
	}
	return true
}

func (a *aggregator) noneMatchStatus(identifiers []object.ObjMetadata, s status.Status) bool {
	fmt.Print("Checking non match status\n")
	for id, ri := range a.resourceMap {
		if contains(identifiers, id) {
			if ri.CurrentStatus == s {
				fmt.Printf("Resource %s has status %s\n", id.GroupKind.String(), ri.CurrentStatus.String())
				return false
			}
		}
	}
	return true
}

func contains(identifiers []object.ObjMetadata, id object.ObjMetadata) bool {
	for _, identifier := range identifiers {
		if identifier.Equals(&id) {
			return true
		}
	}
	return false
}

func (a *Applier) setInfos() ([]*resource.Info, error) {
	// This provides us with a slice of all the objects that will be
	// applied to the cluster.
	infos, err := a.ApplyOptions.GetObjects()
	if err != nil {
		return nil, errors.WrapPrefix(err, "error reading resource manifests", 1)
	}

	// Validate the objects are in the same namespace.
	if !validateNamespace(infos) {
		return nil, fmt.Errorf("currently, applied objects must be in the same namespace")
	}

	// sort the info objects starting from independent to dependent objects,
	// and set them back ordering precedence can be found in resource_infos.go
	sort.Sort(ResourceInfos(infos))
	a.ApplyOptions.SetObjects(infos)
	return infos, nil
}

func infosToObjMetas(infos []*resource.Info) []object.ObjMetadata {
	var objMetas []object.ObjMetadata
	for _, info := range infos {
		u := info.Object.(*unstructured.Unstructured)
		objMetas = append(objMetas, object.ObjMetadata{
			GroupKind: u.GroupVersionKind().GroupKind(),
			Name:      u.GetName(),
			Namespace: u.GetNamespace(),
		})
	}
	return objMetas
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
