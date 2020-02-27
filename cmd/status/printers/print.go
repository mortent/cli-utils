package printers

import (
	"fmt"
	"io"

	"sigs.k8s.io/cli-utils/pkg/kstatus/polling/collector"
)

type Printer interface {
	PrintUntil(stop <-chan struct{}) <-chan struct{}
}

func CreatePrinter(printerType string, collector *collector.ResourceStatusCollector, w io.Writer) (Printer, error) {
	switch printerType {
	case "table":
		return NewTreePrinter(collector, w), nil
	default:
		return nil, fmt.Errorf("no printer available for output %q", printerType)
	}
}
