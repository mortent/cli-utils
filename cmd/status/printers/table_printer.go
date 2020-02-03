package printers

import (
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/integer"
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe/collector"
	"sigs.k8s.io/cli-utils/pkg/kstatus/observe/common"
	"sigs.k8s.io/cli-utils/pkg/kstatus/status"
)

const (
	updateInterval = 1 * time.Second
)

type tableContentFunc func(w io.Writer, width int, resource *common.ObservedResource) (int, error)

type ColumnDef struct {
	name    string
	header  string
	width   int
	content tableContentFunc
}

var (
	columns = []ColumnDef{
		{
			name:   "namespace",
			header: "NAMESPACE",
			width:  10,
			content: func(w io.Writer, availableWidth int, resource *common.ObservedResource) (int, error) {
				namespace := resource.Identifier.Namespace
				if len(namespace) > availableWidth {
					namespace = namespace[:availableWidth]
				}
				_, err := fmt.Fprint(w, namespace)
				return len(namespace), err
			},
		},
		{
			name:   "resource",
			header: "RESOURCE",
			width:  40,
			content: func(w io.Writer, availableWidth int, resource *common.ObservedResource) (int, error) {
				text := fmt.Sprintf("%s/%s", resource.Identifier.GroupKind.Kind, resource.Identifier.Name)
				if len(text) > availableWidth {
					text = text[:availableWidth]
				}
				_, err := fmt.Fprint(w, text)
				return len(text), err
			},
		},
		{
			name:   "status",
			header: "STATUS",
			width:  10,
			content: func(w io.Writer, availableWidth int, resource *common.ObservedResource) (int, error) {
				s := resource.Status.String()
				if len(s) > availableWidth {
					s = s[:availableWidth]
				}
				color, setColor := colorForTableStatus(resource.Status)
				var outputStatus string
				if setColor {
					outputStatus = sPrintWithColor(color, s)
				} else {
					outputStatus = s
				}
				_, err := fmt.Fprint(w, outputStatus)
				return len(s), err
			},
		},
		{
			name:   "conditions",
			header: "CONDITIONS",
			width:  40,
			content: func(w io.Writer, availableWidth int, resource *common.ObservedResource) (int, error) {
				u := resource.Resource
				if u == nil {
					return fmt.Fprintf(w, "-")
				}

				conditions, found, err := unstructured.NestedSlice(u.Object, "status", "conditions")
				if !found || err != nil || len(conditions) == 0 {
					return fmt.Fprintf(w, "<None>")
				}

				realLength := 0
				for i, cond := range conditions {
					condition := cond.(map[string]interface{})
					conditionType := condition["type"].(string)
					conditionStatus := condition["status"].(string)
					var color color
					switch conditionStatus {
					case "True":
						color = GREEN
					case "False":
						color = RED
					default:
						color = YELLOW
					}
					remainingWidth := availableWidth - realLength
					if len(conditionType) > remainingWidth {
						conditionType = conditionType[:remainingWidth]
					}
					_, err := fmt.Fprint(w, sPrintWithColor(color, conditionType))
					if err != nil {
						return realLength, err
					}
					realLength += len(conditionType)
					if i < len(conditions)-1 && availableWidth-realLength > 2 {
						_, err = fmt.Fprintf(w, ",")
						if err != nil {
							return realLength, err
						}
						realLength += 1
					}
				}
				return realLength, nil
			},
		},
		{
			name:   "age",
			header: "AGE",
			width:  6,
			content: func(w io.Writer, availableWidth int, resource *common.ObservedResource) (int, error) {
				u := resource.Resource
				if u == nil {
					return fmt.Fprint(w, "-")
				}

				timestamp, found, err := unstructured.NestedString(u.Object, "metadata", "creationTimestamp")
				if !found || err != nil || timestamp == "" {
					return fmt.Fprint(w, "-")
				}
				parsedTime, err := time.Parse(time.RFC3339, timestamp)
				if err != nil {
					return fmt.Fprint(w, "-")
				}
				age := time.Since(parsedTime)
				switch {
				case age.Seconds() <= 90:
					return fmt.Fprintf(w, "%ds", integer.RoundToInt32(age.Round(time.Second).Seconds()))
				case age.Minutes() <= 90:
					return fmt.Fprintf(w, "%dm", integer.RoundToInt32(age.Round(time.Minute).Minutes()))
				default:
					return fmt.Fprintf(w, "%dh", integer.RoundToInt32(age.Round(time.Hour).Hours()))
				}
			},
		},
		{
			name:   "message",
			header: "MESSAGE",
			width:  40,
			content: func(w io.Writer, availableWidth int, resource *common.ObservedResource) (int, error) {
				var message string
				if resource.Error != nil {
					message = resource.Error.Error()
				} else {
					message = resource.Message
				}
				if len(message) > availableWidth {
					message = message[:availableWidth]
				}
				return fmt.Fprint(w, message)
			},
		},
	}
)

type TreePrinter struct {
	collector *collector.ObservedStatusCollector
	w         io.Writer
}

func NewTreePrinter(collector *collector.ObservedStatusCollector, w io.Writer) *TreePrinter {
	return &TreePrinter{
		collector: collector,
		w:         w,
	}
}

func (t *TreePrinter) PrintUntil(stop <-chan struct{}) <-chan struct{} {
	completed := make(chan struct{})

	linesPrinted := t.printTable(t.collector.LatestObservation(), 0)

	go func() {
		defer close(completed)
		ticker := time.NewTicker(updateInterval)
		for {
			select {
			case <-stop:
				ticker.Stop()
				latestObservation := t.collector.LatestObservation()
				if latestObservation.Error != nil {
					t.printError(latestObservation)
					return
				}
				linesPrinted = t.printTable(latestObservation, linesPrinted)
				return
			case <-ticker.C:
				latestObservation := t.collector.LatestObservation()
				linesPrinted = t.printTable(latestObservation, linesPrinted)
			}
		}
	}()

	return completed
}

func (t *TreePrinter) printError(data *collector.Observation) {
	t.printOrDie("Error: %s\n", data.Error.Error())
}

func (t *TreePrinter) printTable(data *collector.Observation, moveUpCount int) int {
	for i := 0; i < moveUpCount; i++ {
		t.moveUp()
		t.eraseCurrentLine()
	}
	linePrintCount := 0

	color, setColor := colorForTableStatus(data.AggregateStatus)
	var aggStatusText string
	if setColor {
		aggStatusText = sPrintWithColor(color, data.AggregateStatus.String())
	} else {
		aggStatusText = data.AggregateStatus.String()
	}
	t.printOrDie("Aggregate status: %s\n", aggStatusText)
	linePrintCount++

	for i, column := range columns {
		format := fmt.Sprintf("%%-%ds", column.width)
		t.printOrDie(format, column.header)
		if i == len(columns)-1 {
			t.printOrDie("\n")
			linePrintCount++
		} else {
			t.printOrDie("  ")
		}
	}

	for _, resource := range data.ObservedResources {
		for i, column := range columns {
			written, err := column.content(t.w, column.width, resource)
			if err != nil {
				panic(err)
			}
			remainingSpace := column.width - written
			t.printOrDie(strings.Repeat(" ", remainingSpace))
			if i == len(columns)-1 {
				t.printOrDie("\n")
				linePrintCount++
			} else {
				t.printOrDie("  ")
			}
		}

		linePrintCount += t.printSubTable(resource.GeneratedResources, "")
	}

	return linePrintCount
}

func (t *TreePrinter) printSubTable(resources []*common.ObservedResource, prefix string) int {
	linePrintCount := 0
	for j, resource := range resources {
		for i, column := range columns {
			availableWidth := column.width
			if column.name == "resource" {
				if j < len(resources)-1 {
					t.printOrDie(prefix + `├─ `)
				} else {
					t.printOrDie(prefix + `└─ `)
				}
				availableWidth -= utf8.RuneCountInString(prefix) + 3
			}
			written, err := column.content(t.w, availableWidth, resource)
			if err != nil {
				panic(err)
			}
			remainingSpace := availableWidth - written
			t.printOrDie(strings.Repeat(" ", remainingSpace))
			if i == len(columns)-1 {
				t.printOrDie("\n")
				linePrintCount++
			} else {
				t.printOrDie("  ")
			}
		}

		var prefix string
		if j < len(resources)-1 {
			prefix = `│  `
		} else {
			prefix = "   "
		}
		linePrintCount += t.printSubTable(resource.GeneratedResources, prefix)
	}
	return linePrintCount
}

func (t *TreePrinter) printOrDie(format string, a ...interface{}) {
	_, err := fmt.Fprintf(t.w, format, a...)
	if err != nil {
		panic(err)
	}
}

func (t *TreePrinter) moveUp() {
	t.printOrDie("%c[%dA", ESC, 1)
}

func (t *TreePrinter) eraseCurrentLine() {
	t.printOrDie("%c[2K\r", ESC)
}

func sPrintWithColor(color color, format string, a ...interface{}) string {
	return fmt.Sprintf("%c[%dm", ESC, color) +
		fmt.Sprintf(format, a...) +
		fmt.Sprintf("%c[%dm", ESC, RESET)
}

func colorForTableStatus(s status.Status) (color color, setColor bool) {
	switch s {
	case status.CurrentStatus:
		color = GREEN
		setColor = true
	case status.InProgressStatus:
		color = YELLOW
		setColor = true
	case status.FailedStatus:
		color = RED
		setColor = true
	}
	return
}
