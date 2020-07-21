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
var url = flag.String("a", "bolt://localhost:7687", "address to connect to, eg. bolt+routing://mydb:7687")
var user = flag.String("u", "neo4j", "username")
var password = flag.String("p", "neo4j", "password")
var encrypted = flag.Bool("e", true, "use encrypted connections")
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
		fmt.Fprintf(flag.CommandLine.Output(), `
Custom scripts

  Workload scripts consist of commands and meta-commands. Commands are Cypher queries, 
  separated by semi-colon, like so:

      CREATE (n:Person);
      MATCH (n) RETURN n;

  Currently only one meta-command is available: \set, it lets you set variables to use
  in your queries and in subsequent meta-commands:

      \set myParam random(1, 100)
      CREATE (n:Person {name: $myParam});

  Meta-commands are separated by newline.

  Commands are executed one-at-a-time, all of them in one single transaction. Latency and
  throughput rates are for the full script, not per-query.
`)
	}
	flag.Parse()
	seed := time.Now().Unix()
	runtime := time.Duration(*duration) * time.Second
	scenario := describeScenario()

	out, err := neobench.NewOutput(*outputFormat)
	if err != nil {
		log.Fatal(err)
	}

	driver, err := newDriver(*url, *user, *password, *encrypted)
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
		os.Exit(runThroughputBenchmark(driver, scenario, out, wrk, runtime))
	case "latency":
		os.Exit(runLatencyBenchmark(driver, scenario, out, wrk, runtime))
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
	if *benchmarkMode == "latency" {
		out.WriteString(fmt.Sprintf(" -r %f", *rate))
	}
	if !*encrypted {
		out.WriteString(" -e=false")
	}
	if *initMode {
		out.WriteString(" -i")
	}
	return out.String()
}

func runLatencyBenchmark(driver neo4j.Driver, scenario string, out neobench.Output, wrk neobench.Workload, runtime time.Duration) int {
	stopCh, stop := neobench.SetupSignalHandler()
	defer stop()

	ratePerWorkerPerSecond := *rate / float64(*clients)
	ratePerWorkerDuration := time.Duration(1000*1000/ratePerWorkerPerSecond) * time.Microsecond

	resultChan := make(chan workerLatencyResult, *clients)
	var wg sync.WaitGroup
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		worker := neobench.NewWorker(driver)
		workerId := i
		clientWork := wrk.NewClient()
		go func() {
			defer wg.Done()
			result, err := worker.RunLatencyBenchmark(clientWork, ratePerWorkerDuration, stopCh)
			if err != nil {
				out.Errorf("worker %d crashed: %s", workerId, err)
				stop()
				resultChan <- workerLatencyResult{
					workerId: workerId,
					err:      err,
				}
				return
			}

			resultChan <- workerLatencyResult{
				workerId: workerId,
				hdr:      result,
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

	return processLatencyResults(scenario, out, *clients, resultChan)
}

func runThroughputBenchmark(driver neo4j.Driver, scenario string, out neobench.Output, wrk neobench.Workload, runtime time.Duration) int {
	stopCh, stop := neobench.SetupSignalHandler()
	defer stop()

	resultChan := make(chan workerThroughputResult, *clients)
	var wg sync.WaitGroup
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		worker := neobench.NewWorker(driver)
		workerId := i
		clientWork := wrk.NewClient()
		go func() {
			defer wg.Done()
			result, err := worker.RunThroughputBenchmark(clientWork, stopCh)
			if err != nil {
				out.Errorf("worker %d crashed: %s", workerId, err)
				stop()
				resultChan <- workerThroughputResult{
					workerId: workerId,
					err:      err,
				}
				return
			}

			resultChan <- workerThroughputResult{
				workerId:      workerId,
				ratePerSecond: result,
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

	return processThroughputResults(scenario, out, *clients, resultChan)
}

func processLatencyResults(scenario string, out neobench.Output, concurrency int, resultChan chan workerLatencyResult) int {
	// Collect results
	results := make([]workerLatencyResult, 0, concurrency)
	for i := 0; i < concurrency; i++ {
		results = append(results, <-resultChan)
	}

	// Process results into one histogram and check for errors
	var combinedHistogram *hdrhistogram.Histogram
	for _, res := range results {
		if res.err != nil {
			out.Errorf("Worker failed: %v", res.err)
			continue
		}
		if combinedHistogram == nil {
			// Copy the first one, we merge the others into this
			combinedHistogram = hdrhistogram.Import(res.hdr.Export())
		} else {
			combinedHistogram.Merge(res.hdr)
		}
	}

	// Report findings
	if combinedHistogram == nil {
		out.Errorf("all workers failed")
		return 1
	}
	out.ReportLatencyResult(neobench.LatencyResult{
		Scenario:       scenario,
		TotalHistogram: combinedHistogram,
	})
	return 0
}

func processThroughputResults(scenario string, out neobench.Output, concurrency int, resultChan chan workerThroughputResult) int {
	// Collect results
	out.ReportProgress(neobench.ProgressReport{
		Section:      "benchmark",
		Step:         "collect-results",
		Completeness: 0,
	})
	results := make([]workerThroughputResult, 0, concurrency)
	for i := 0; i < concurrency; i++ {
		out.ReportProgress(neobench.ProgressReport{
			Section:      "benchmark",
			Step:         "collect-results",
			Completeness: float64(i) / float64(concurrency),
		})
		results = append(results, <-resultChan)
	}

	totalRatePerSecond := 0.0
	allFailed := false
	for _, res := range results {
		if res.err != nil {
			out.Errorf("worker failed: %v", res.err)
			continue
		}
		allFailed = false
		totalRatePerSecond += res.ratePerSecond
	}

	// Report findings
	if allFailed {
		out.Errorf("all workers failed")
		return 1
	}
	out.ReportThroughputResult(neobench.ThroughputResult{
		Scenario:           scenario,
		TotalRatePerSecond: totalRatePerSecond,
	})
	return 0
}

type workerLatencyResult struct {
	workerId int
	hdr      *hdrhistogram.Histogram
	err      error
}

type workerThroughputResult struct {
	workerId      int
	ratePerSecond float64
	err           error
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

func newDriver(url, user, password string, encrypted bool) (neo4j.Driver, error) {
	config := func(conf *neo4j.Config) { conf.Encrypted = encrypted }
	return neo4j.NewDriver(url, neo4j.BasicAuth(user, password, ""), config)
}
