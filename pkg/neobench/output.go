package neobench

import (
	"fmt"
	"github.com/codahale/hdrhistogram"
	"io"
	"os"
	"strings"
	"time"
)

type ProgressReport struct {
	Section      string
	Step         string
	Completeness float64
}

type ThroughputResult struct {
	Scenario           string
	TotalRatePerSecond float64
}

type Result struct {
	Scenario           string
	TotalLatencies     *hdrhistogram.Histogram
	TotalRate          float64
	TotalFailed        int64
	TotalSucceeded     int64
	FailedByErrorGroup map[string]FailureGroup

	// Per-worker results
	Workers []WorkerResult
}

type Output interface {
	ReportProgress(report ProgressReport)
	ReportThroughput(result Result)
	ReportLatency(result Result)
	Errorf(format string, a ...interface{})
}

func NewOutput(name string) (Output, error) {

	if name == "auto" {
		fi, _ := os.Stdout.Stat()
		if fi.Mode()&os.ModeCharDevice == 0 {
			return &CsvOutput{
				ErrStream: os.Stderr,
				OutStream: os.Stdout,
			}, nil
		} else {
			return &InteractiveOutput{
				ErrStream: os.Stderr,
				OutStream: os.Stdout,
			}, nil
		}
	}
	if name == "interactive" {
		return &InteractiveOutput{
			ErrStream: os.Stderr,
			OutStream: os.Stdout,
		}, nil
	}
	if name == "csv" {
		return &CsvOutput{
			ErrStream: os.Stderr,
			OutStream: os.Stdout,
		}, nil
	}
	return nil, fmt.Errorf("unknown output format: %s, supported formats are 'auto', 'interactive' and 'csv'", name)
}

type InteractiveOutput struct {
	ErrStream io.Writer
	OutStream io.Writer
	// Used to rate-limit progress reporting
	LastProgressReport ProgressReport
	LastProgressTime   time.Time
}

func (o *InteractiveOutput) ReportProgress(report ProgressReport) {
	now := time.Now()
	if report.Section == o.LastProgressReport.Section && report.Step == o.LastProgressReport.Step && now.Sub(o.LastProgressTime).Seconds() < 10 {
		return
	}
	o.LastProgressReport = report
	o.LastProgressTime = now
	_, err := fmt.Fprintf(o.ErrStream, "[%s][%s] %.02f%%\n", report.Section, report.Step, report.Completeness*100)
	if err != nil {
		panic(err)
	}
}

func (o *InteractiveOutput) ReportThroughput(result Result) {
	s := strings.Builder{}

	s.WriteString("== Results ==\n")
	s.WriteString(fmt.Sprintf("Scenario: %s\n", result.Scenario))
	s.WriteString(fmt.Sprintf("Successful Transactions: %d\n", result.TotalSucceeded))
	s.WriteString("\n")
	s.WriteString(fmt.Sprintf("Rate: %.03f successful transactions per second\n", result.TotalRate))
	o.reportErrors(result, &s)

	_, err := fmt.Fprintf(o.OutStream, s.String())
	if err != nil {
		panic(err)
	}
}

func (o *InteractiveOutput) ReportLatency(result Result) {
	histo := result.TotalLatencies

	s := strings.Builder{}

	s.WriteString("== Results ==\n")

	s.WriteString(fmt.Sprintf("Scenario: %s\n", result.Scenario))
	s.WriteString(fmt.Sprintf("Successful Transactions: %d\n", histo.TotalCount()))
	if result.TotalSucceeded > 0 {
		s.WriteString("\n")
		s.WriteString(fmt.Sprintf("Latency summary:\n"))
		s.WriteString(fmt.Sprintf("  Min:    %.3fms\n", float64(histo.Min())/1000.0))
		s.WriteString(fmt.Sprintf("  Mean:   %.3fms\n", histo.Mean()/1000.0))
		s.WriteString(fmt.Sprintf("  Max:    %.3fms\n", float64(histo.Max())/1000.0))
		s.WriteString(fmt.Sprintf("  Stddev: %.3fms\n", histo.StdDev()/1000.0))
		s.WriteString("\n")
		s.WriteString(fmt.Sprintf("Latency distribution:\n"))
		s.WriteString(fmt.Sprintf("  P50.000: %.03fms\n", float64(histo.ValueAtQuantile(50))/1000.0))
		s.WriteString(fmt.Sprintf("  P75.000: %.03fms\n", float64(histo.ValueAtQuantile(75))/1000.0))
		s.WriteString(fmt.Sprintf("  P95.000: %.03fms\n", float64(histo.ValueAtQuantile(95))/1000.0))
		s.WriteString(fmt.Sprintf("  P99.000: %.03fms\n", float64(histo.ValueAtQuantile(99))/1000.0))
		s.WriteString(fmt.Sprintf("  P99.999: %.03fms\n", float64(histo.ValueAtQuantile(99.999))/1000.0))
	}
	o.reportErrors(result, &s)

	_, err := fmt.Fprint(o.OutStream, s.String())
	if err != nil {
		panic(err)
	}
}

func (o *InteractiveOutput) reportErrors(result Result, s *strings.Builder) {
	s.WriteString("\n")
	s.WriteString(fmt.Sprintf("Error stats:\n"))
	if result.TotalFailed == 0 {
		s.WriteString(fmt.Sprintf("  No errors!\n"))
	} else {
		s.WriteString(fmt.Sprintf("  Failed transactions: %d (%.3f %%)\n", result.TotalFailed, 100*float64(result.TotalFailed)/float64(result.TotalFailed+result.TotalSucceeded)))
		s.WriteString(fmt.Sprintf("\n"))
		s.WriteString(fmt.Sprintf("  Causes:\n"))
		for name, info := range result.FailedByErrorGroup {
			s.WriteString(fmt.Sprintf("    %s: %d failures\n", name, info.Count))
			s.WriteString(fmt.Sprintf("      (ex: %s)\n", info.FirstFailure))
		}
	}
}

func (o *InteractiveOutput) Errorf(format string, a ...interface{}) {
	_, err := fmt.Fprintf(o.ErrStream, "ERROR: %s\n", fmt.Sprintf(format, a...))
	if err != nil {
		panic(err)
	}
}

// Writes simple progress to stderr, and then a result for easy import into eg. a spreadsheet or other app
// in CSV format to stdout
type CsvOutput struct {
	ErrStream io.Writer
	OutStream io.Writer
	// Used to rate-limit progress reporting
	LastProgressReport ProgressReport
	LastProgressTime   time.Time
}

func (o *CsvOutput) ReportProgress(report ProgressReport) {
	now := time.Now()
	if report.Section == o.LastProgressReport.Section && report.Step == o.LastProgressReport.Step && now.Sub(o.LastProgressTime).Seconds() < 10 {
		return
	}
	o.LastProgressReport = report
	o.LastProgressTime = now
	_, err := fmt.Fprintf(o.ErrStream, "[%s][%s] %.02f%%\n", report.Section, report.Step, report.Completeness*100)
	if err != nil {
		panic(err)
	}
}

func (o *CsvOutput) ReportThroughput(result Result) {
	columns := []string{"scenario", "succeeded", "failed", "transactions_per_second"}
	row := []float64{
		float64(result.TotalSucceeded),
		float64(result.TotalFailed),
		result.TotalRate,
	}

	s := strings.Builder{}
	separator := ","
	s.WriteString(strings.Join(columns, separator))
	s.WriteString("\n")

	for i, cell := range row {
		if i > 0 {
			s.WriteString(separator)
		}
		s.WriteString(fmt.Sprintf("%.03f", cell))
	}
	s.WriteString("\n")

	_, err := fmt.Fprint(o.OutStream, s.String())
	if err != nil {
		panic(err)
	}

}

func (o *CsvOutput) ReportLatency(result Result) {
	histo := result.TotalLatencies

	columns := []string{"scenario", "succeeded", "failed", "min_ms", "mean_ms", "max_ms", "stdev", "p50_ms", "p75_ms", "p99_ms", "p99999_ms"}
	row := []float64{
		float64(histo.TotalCount()),
		float64(result.TotalFailed),
		float64(histo.Min()) / 1000.0,
		histo.Mean() / 1000.0,
		float64(histo.Max()) / 1000.0,
		histo.StdDev() / 1000.0,
		float64(histo.ValueAtQuantile(50)) / 1000.0,
		float64(histo.ValueAtQuantile(75)) / 1000.0,
		float64(histo.ValueAtQuantile(95)) / 1000.0,
		float64(histo.ValueAtQuantile(99)) / 1000.0,
		float64(histo.ValueAtQuantile(99.999)) / 1000.0,
	}

	s := strings.Builder{}
	separator := ","
	s.WriteString(strings.Join(columns, separator))
	s.WriteString("\n")

	s.WriteString(fmt.Sprintf("\"%s\"", result.Scenario))

	for i, cell := range row {
		if i > 0 {
			s.WriteString(separator)
		}
		s.WriteString(fmt.Sprintf("%.03f", cell))
	}
	s.WriteString("\n")

	_, err := fmt.Fprint(o.OutStream, s.String())
	if err != nil {
		panic(err)
	}

}

func (o *CsvOutput) Errorf(format string, a ...interface{}) {
	_, err := fmt.Fprintf(o.ErrStream, "ERROR: %s\n", fmt.Sprintf(format, a...))
	if err != nil {
		panic(err)
	}
}
