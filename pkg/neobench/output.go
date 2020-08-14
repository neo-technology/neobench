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

type Result struct {
	// Targeted database
	DatabaseName string
	Scenario     string

	FailedByErrorGroup map[string]FailureGroup

	// Results by script
	Scripts []ScriptResult

	// Per-worker results
	Workers []WorkerResult
}

func (r *Result) TotalSucceeded() (n int64) {
	for _, s := range r.Scripts {
		n += s.Succeeded
	}
	return
}

func (r *Result) TotalFailed() (n int64) {
	for _, s := range r.Scripts {
		n += s.Failed
	}
	return
}

func (r *Result) TotalRate() (n float64) {
	for _, s := range r.Scripts {
		n += s.Rate
	}
	return
}

// Result for one script; normally a workload is just one script, but we allow workloads to be made up of
// lots of scripts as well, with a weighted random mix of them. We report results per-script, since latencies
// between different scripts will mean totally different things.
type ScriptResult struct {
	ScriptName string
	// Rate is scripts executed per second, both succeeded and failed
	// TODO should this just count succeeded? That creates confusing effects with how the workload paces itself tho..
	Rate      float64
	Failed    int64
	Succeeded int64
	Latencies *hdrhistogram.Histogram
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
	s.WriteString(fmt.Sprintf("Successful Transactions: %d (%.3f per second)\n", result.TotalSucceeded(), result.TotalRate()))
	s.WriteString("\n")
	for _, script := range result.Scripts {
		s.WriteString(fmt.Sprintf("  [%s]: %.03f successful transactions per second\n", script.ScriptName, script.Rate))
	}
	o.reportErrors(result, &s)

	_, err := fmt.Fprintf(o.OutStream, s.String())
	if err != nil {
		panic(err)
	}
}

func (o *InteractiveOutput) ReportLatency(result Result) {
	s := strings.Builder{}

	s.WriteString("== Results ==\n")

	s.WriteString(fmt.Sprintf("Scenario: %s\n", result.Scenario))
	s.WriteString(fmt.Sprintf("Successful Transactions: %d (%.3f per second)\n", result.TotalSucceeded(), result.TotalRate()))

	if result.TotalSucceeded() > 0 {
		if len(result.Scripts) == 1 {
			workload := result.Scripts[0]
			s.WriteString("\n")
			summarizeLatency(workload, &s, "  ")
		} else {
			for _, workload := range result.Scripts {
				s.WriteString("\n")
				s.WriteString(fmt.Sprintf("-- Script: %s --\n\n", workload.ScriptName))
				summarizeLatency(workload, &s, "  ")
			}
		}
	}
	s.WriteString("\n")
	writeErrorReport(result, &s)

	_, err := fmt.Fprint(o.OutStream, s.String())
	if err != nil {
		panic(err)
	}
}

func summarizeLatency(script ScriptResult, s *strings.Builder, indent string) {
	histo := script.Latencies
	lines := []string{
		fmt.Sprintf("Successful Transactions: %d (%.3f per second)\n\n", script.Succeeded, script.Rate),
		fmt.Sprintf("Max: %.3fms, Min: %.3fms, Mean: %.3fms, Stddev: %.3f\n\n",
			float64(histo.Max())/1000.0, float64(histo.Min())/1000.0, histo.Mean()/1000.0, histo.StdDev()/1000.0),
		fmt.Sprintf("Latency distribution:\n"),
		fmt.Sprintf("  P00.000: %.03fms\n", float64(histo.Min())/1000.0),
		fmt.Sprintf("  P25.000: %.03fms\n", float64(histo.ValueAtQuantile(25))/1000.0),
		fmt.Sprintf("  P50.000: %.03fms\n", float64(histo.ValueAtQuantile(50))/1000.0),
		fmt.Sprintf("  P75.000: %.03fms\n", float64(histo.ValueAtQuantile(75))/1000.0),
		fmt.Sprintf("  P95.000: %.03fms\n", float64(histo.ValueAtQuantile(95))/1000.0),
		fmt.Sprintf("  P99.000: %.03fms\n", float64(histo.ValueAtQuantile(99))/1000.0),
		fmt.Sprintf("  P99.999: %.03fms\n", float64(histo.ValueAtQuantile(99.999))/1000.0),
	}
	for _, line := range lines {
		s.WriteString(indent)
		s.WriteString(line)
	}
}

func writeErrorReport(result Result, s *strings.Builder) {
	s.WriteString(fmt.Sprintf("Error stats:\n"))
	if result.TotalFailed() == 0 {
		s.WriteString(fmt.Sprintf("  No errors!\n"))
	} else {
		s.WriteString(fmt.Sprintf("  Failed transactions: %d (%.3f %%)\n", result.TotalFailed(), 100*float64(result.TotalFailed())/float64(result.TotalFailed()+result.TotalSucceeded())))
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
	columns := []string{"script", "succeeded", "failed", "transactions_per_second"}

	s := strings.Builder{}
	separator := ","
	s.WriteString(strings.Join(columns, separator))
	s.WriteString("\n")

	for _, script := range result.Scripts {
		row := []float64{
			float64(script.Succeeded),
			float64(script.Failed),
			script.Rate,
		}
		s.WriteString(fmt.Sprintf("\"%s\",", script.ScriptName))
		for i, cell := range row {
			if i > 0 {
				s.WriteString(separator)
			}
			s.WriteString(fmt.Sprintf("%.03f", cell))
		}
		s.WriteString("\n")
	}

	if _, err := fmt.Fprint(o.OutStream, s.String()); err != nil {
		panic(err)
	}

	if result.TotalFailed() > 0 {
		s.Reset()
		writeErrorReport(result, &s)
		if _, err := fmt.Fprint(o.ErrStream, s.String()); err != nil {
			panic(err)
		}
	}
}

func (o *CsvOutput) ReportLatency(result Result) {
	fmtFloat := func(v interface{}) string {
		switch v.(type) {
		case int64:
			return fmt.Sprintf("%.3f", float64(v.(int64)))
		case float64:
			return fmt.Sprintf("%.3f", v.(float64))
		}
		return fmt.Sprintf("%v?", v)
	}
	columns := []struct {
		name  string
		value func(s ScriptResult) string
	}{
		{"db", func(s ScriptResult) string { return fmt.Sprintf("\"%s\",", result.DatabaseName) }},
		{"script", func(s ScriptResult) string { return fmt.Sprintf("\"%s\",", s.ScriptName) }},
		{"rate", func(s ScriptResult) string { return fmtFloat(s.Rate) }},
		{"succeeded", func(s ScriptResult) string { return fmtFloat(s.Latencies.TotalCount()) }},
		{"failed", func(s ScriptResult) string { return fmtFloat(s.Failed) }},
		{"mean", func(s ScriptResult) string { return fmtFloat(s.Latencies.Mean() / 1000.0) }},
		{"stdev", func(s ScriptResult) string { return fmtFloat(s.Latencies.StdDev()) }},
		{"p0", func(s ScriptResult) string { return fmtFloat(float64(s.Latencies.Min()) / 1000.0) }},
		{"p25", func(s ScriptResult) string { return fmtFloat(float64(s.Latencies.ValueAtQuantile(25)) / 1000.0) }},
		{"p50", func(s ScriptResult) string { return fmtFloat(float64(s.Latencies.ValueAtQuantile(50)) / 1000.0) }},
		{"p75", func(s ScriptResult) string { return fmtFloat(float64(s.Latencies.ValueAtQuantile(75)) / 1000.0) }},
		{"p99", func(s ScriptResult) string { return fmtFloat(float64(s.Latencies.ValueAtQuantile(99)) / 1000.0) }},
		{"p99999", func(s ScriptResult) string { return fmtFloat(float64(s.Latencies.ValueAtQuantile(99.999)) / 1000.0) }},
		{"p100", func(s ScriptResult) string { return fmtFloat(float64(s.Latencies.Max()) / 1000.0) }},
	}

	columnNames := make([]string, 0, len(columns))
	for _, col := range columns {
		columnNames = append(columnNames, col.name)
	}

	s := strings.Builder{}
	separator := ","
	s.WriteString(strings.Join(columnNames, separator))
	s.WriteString("\n")

	for _, script := range result.Scripts {
		for i, col := range columns {
			if i != 0 {
				s.WriteString(",")
			}
			s.WriteString(col.value(script))
		}
		s.WriteString("\n")
	}

	_, err := fmt.Fprint(o.OutStream, s.String())
	if err != nil {
		panic(err)
	}

	if result.TotalFailed() > 0 {
		s.Reset()
		writeErrorReport(result, &s)
		if _, err := fmt.Fprint(o.ErrStream, s.String()); err != nil {
			panic(err)
		}
	}
}

func (o *CsvOutput) Errorf(format string, a ...interface{}) {
	_, err := fmt.Fprintf(o.ErrStream, "ERROR: %s\n", fmt.Sprintf(format, a...))
	if err != nil {
		panic(err)
	}
}
