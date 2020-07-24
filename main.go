package main

import (
	"flag"
	"fmt"
	"github.com/codahale/hdrhistogram"
	"io/ioutil"
	"log"
	"neobench/pkg/neobench"
	"os"
	"strings"
	"sync"
	"time"
)
import "github.com/neo4j/neo4j-go-driver/neo4j"

var initMode = flag.Bool("i", false, "initialize dataset before running workload")
var scale = flag.Int64("s", 1, "scale factor, effect depends on workload but in general this scales the size of the dataset linearly")
var clients = flag.Int("c", 1, "number of clients, ie. number of concurrent simulated database sessions")
var rate = flag.Float64("r", 1, "in latency mode (see -m) this sets transactions per second, total across all clients. This can be set to a fraction if you want")
var url = flag.String("a", "neo4j://localhost:7687", "address to connect to, eg. bolt+routing://mydb:7687")
var user = flag.String("u", "neo4j", "username")
var password = flag.String("p", "neo4j", "password")
var encryption = flag.String("e", "auto", "whether to use encryption, `auto`, `true` or `false`")
var duration = flag.Int("d", 60, "seconds to run")
var workloadPath = flag.String("w", "builtin:tpcb-like", "workload to run")
var benchmarkMode = flag.String("m", "throughput", "benchmark mode: throughput or latency, latency uses a fixed rate workload, see -r")
var outputFormat = flag.String("o", "auto", "output report format, `auto`, `interactive` or `csv`; auto uses `interactive` stdout is to a terminal, otherwise csv")

func main() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), `
neobench - scriptable workload generator for Neo4j

  neobench runs a canned or user-defined workload against a specified Neo4j Database.
  By default, it measures the maximum throughput it can achieve, but it can also
  measure latencies, automatically correcting for coordinated omission.

  There is one built-in workload, used by default: builtin:tpcb-like, it is very 
  similar to the tpcb-like workload found in pgbench.

Usage:

`)
		flag.PrintDefaults()
	}
	flag.Parse()
	seed := time.Now().Unix()
	runtime := time.Duration(*duration) * time.Second
	scenario := describeScenario()

	out, err := neobench.NewOutput(*outputFormat)
	if err != nil {
		log.Fatal(err)
	}

	var encryptionMode neobench.EncryptionMode
	switch strings.ToLower(*encryption) {
	case "auto":
		encryptionMode = neobench.EncryptionAuto
	case "true", "yes", "y", "1":
		encryptionMode = neobench.EncryptionOn
	case "false", "no", "n", "0":
		encryptionMode = neobench.EncryptionOff
	default:
		log.Fatalf("Invalid encryption mode '%s', needs to be one of 'auto', 'true' or 'false'", *encryption)
	}

	driver, err := neobench.NewDriver(*url, *user, *password, encryptionMode)
	if err != nil {
		log.Fatal(err)
	}

	wrk, err := createWorkload(*workloadPath, *scale, seed)
	if err != nil {
		log.Fatal(err)
	}

	if *initMode {
		err = initWorkload(*workloadPath, *scale, driver, out)
		if err != nil {
			log.Fatal(err)
		}
	}

	switch *benchmarkMode {
	case "throughput":
		result, err := runBenchmark(driver, scenario, out, wrk, runtime)
		if err != nil {
			out.Errorf(err.Error())
			os.Exit(1)
		}
		out.ReportThroughput(result)
		os.Exit(0)
	case "latency":
		result, err := runBenchmark(driver, scenario, out, wrk, runtime)
		if err != nil {
			out.Errorf(err.Error())
			os.Exit(1)
		}
		out.ReportLatency(result)
		os.Exit(0)
	default:
		fmt.Printf("unknown mode: %s, supported modes are 'latency' and 'throughput'", *benchmarkMode)
		os.Exit(1)
	}
}

func describeScenario() string {
	out := strings.Builder{}
	out.WriteString(fmt.Sprintf("-m %s", *benchmarkMode))
	out.WriteString(fmt.Sprintf(" -w %s", *workloadPath))
	out.WriteString(fmt.Sprintf(" -c %d", *clients))
	out.WriteString(fmt.Sprintf(" -s %d", *scale))
	out.WriteString(fmt.Sprintf(" -d %d", *duration))
	out.WriteString(fmt.Sprintf(" -e %s", *encryption))
	if *benchmarkMode == "latency" {
		out.WriteString(fmt.Sprintf(" -r %f", *rate))
	}
	if *initMode {
		out.WriteString(" -i")
	}
	return out.String()
}

func runBenchmark(driver neo4j.Driver, scenario string, out neobench.Output, wrk neobench.Workload, runtime time.Duration) (neobench.Result, error) {
	stopCh, stop := neobench.SetupSignalHandler()
	defer stop()

	ratePerWorkerDuration := time.Duration(0)
	if *benchmarkMode == "latency" {
		ratePerWorkerPerSecond := *rate / float64(*clients)
		ratePerWorkerDuration = time.Duration(1000*1000/ratePerWorkerPerSecond) * time.Microsecond
	}

	resultChan := make(chan neobench.WorkerResult, *clients)
	var wg sync.WaitGroup
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		worker := neobench.NewWorker(driver)
		workerId := i
		clientWork := wrk.NewClient()
		go func() {
			defer wg.Done()
			result := worker.RunBenchmark(clientWork, ratePerWorkerDuration, stopCh)
			resultChan <- result
			if result.Error != nil {
				out.Errorf("worker %d crashed: %s", workerId, result.Error)
				stop()
			}
		}()
	}

	out.ReportProgress(neobench.ProgressReport{
		Section:      "benchmark",
		Step:         "run",
		Completeness: 0,
	})
	deadline := time.Now().Add(runtime)
	awaitCompletion(stopCh, deadline, out)
	stop()
	out.ReportProgress(neobench.ProgressReport{
		Section:      "benchmark",
		Step:         "stopping",
		Completeness: 0,
	})
	wg.Wait()

	return collectResults(scenario, out, *clients, resultChan)
}

func collectResults(scenario string, out neobench.Output, concurrency int, resultChan chan neobench.WorkerResult) (neobench.Result, error) {
	// Collect results
	results := make([]neobench.WorkerResult, 0, concurrency)
	for i := 0; i < concurrency; i++ {
		results = append(results, <-resultChan)
	}

	total := neobench.Result{
		Scenario:           scenario,
		FailedByErrorGroup: make(map[string]neobench.FailureGroup),
		Workers:            results,
	}
	// Process results into one histogram and check for errors
	var combinedHistogram *hdrhistogram.Histogram
	for _, res := range results {
		if res.Error != nil {
			out.Errorf("Worker failed: %v", res.Error)
			continue
		}
		if combinedHistogram == nil {
			// Copy the first one, we merge the others into this
			combinedHistogram = hdrhistogram.Import(res.Latencies.Export())
		} else {
			combinedHistogram.Merge(res.Latencies)
		}
		total.TotalRate += res.Rate
		total.TotalSucceeded += res.Succeeded
		total.TotalFailed += res.Failed
		for name, group := range res.FailedByErrorGroup {
			existing, found := total.FailedByErrorGroup[name]
			if found {
				total.FailedByErrorGroup[name] = neobench.FailureGroup{
					Count:        existing.Count + group.Count,
					FirstFailure: existing.FirstFailure,
				}
			} else {
				total.FailedByErrorGroup[name] = group
			}
		}
	}

	if combinedHistogram == nil {
		return neobench.Result{}, fmt.Errorf("all workers failed")
	}

	total.TotalLatencies = combinedHistogram

	return total, nil
}

func initWorkload(path string, scale int64, driver neo4j.Driver, out neobench.Output) error {
	if path == "builtin:tpcb-like" {
		return neobench.InitTPCBLike(scale, driver, out)
	}
	return fmt.Errorf("init option is only supported for built-in workloads; if you want to initialize a database for a custom script, simply set up the database as you prefer first")
}

func createWorkload(path string, scale, seed int64) (neobench.Workload, error) {
	if path == "builtin:tpcb-like" {
		return neobench.Parse("builtin:tpcp-like", neobench.TPCBLike, scale, seed)
	}

	scriptContent, err := ioutil.ReadFile(path)
	if err != nil {
		return neobench.Workload{}, fmt.Errorf("failed to read workload file at %s: %s", path, err)
	}

	return neobench.Parse(path, string(scriptContent), scale, seed)
}

func awaitCompletion(stopCh chan struct{}, deadline time.Time, out neobench.Output) {
	originalDelta := deadline.Sub(time.Now()).Seconds()
	for {
		select {
		case <-stopCh:
			return
		default:
		}

		now := time.Now()
		delta := deadline.Sub(now)
		if delta < 2*time.Second {
			time.Sleep(delta)
			break
		}
		out.ReportProgress(neobench.ProgressReport{
			Section:      "benchmark",
			Step:         "run",
			Completeness: 1 - delta.Seconds()/originalDelta,
		})
		time.Sleep(time.Millisecond * 100)
	}
}
