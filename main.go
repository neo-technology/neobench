package main

import (
	"flag"
	"fmt"
	"github.com/codahale/hdrhistogram"
	"go.uber.org/zap"
	"io/ioutil"
	"log"
	"neobench/pkg"
	"neobench/pkg/workload"
	"os"
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

	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
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
		err = initWorkload(*workloadPath, *scale, seed, driver, logger)
		if err != nil {
			panic(err)
		}
	}

	switch *benchmarkMode {
	case "throughput":
		os.Exit(runThroughputBenchmark(driver, logger, wrk, runtime))
	case "latency":
		os.Exit(runLatencyBenchmark(driver, logger, wrk, runtime))
	default:
		fmt.Printf("unknown mode: %s, supported modes are 'latency' and 'throughput'", *benchmarkMode)
		os.Exit(1)
	}
}

func runLatencyBenchmark(driver neo4j.Driver, logger *zap.Logger, wrk workload.Workload, runtime time.Duration) int {
	sLogger := logger.Sugar()
	stopCh, stop := pkg.SetupSignalHandler(sLogger)
	defer stop()

	ratePerWorkerPerSecond := *rate / float64(*clients)
	ratePerWorkerDuration := time.Duration(1000*1000/ratePerWorkerPerSecond) * time.Microsecond

	resultChan := make(chan workerLatencyResult, *clients)
	var wg sync.WaitGroup
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		worker := pkg.NewWorker(driver, logger, ratePerWorkerDuration)
		workerId := i
		clientWork := wrk.NewClient()
		go func() {
			defer wg.Done()
			result, err := worker.RunLatencyBenchmark(clientWork, ratePerWorkerDuration, stopCh)
			sLogger.Infof("Worker %d completed with %v", workerId, err)
			if err != nil {
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

	sLogger.Infof("Started %d workers, now running for %s", *clients, runtime.String())
	deadline := time.Now().Add(runtime)
	awaitCompletion(stopCh, deadline, sLogger)
	stop()
	sLogger.Infof("Waiting for clients to stop..")
	wg.Wait()
	sLogger.Infof("Processing results..")

	exitCode := processLatencyResults(sLogger, *clients, resultChan)
	return exitCode
}

func runThroughputBenchmark(driver neo4j.Driver, logger *zap.Logger, wrk workload.Workload, runtime time.Duration) int {
	sLogger := logger.Sugar()
	stopCh, stop := pkg.SetupSignalHandler(sLogger)
	defer stop()

	resultChan := make(chan workerThroughputResult, *clients)
	var wg sync.WaitGroup
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		worker := pkg.NewWorker(driver, logger, 0)
		workerId := i
		clientWork := wrk.NewClient()
		go func() {
			defer wg.Done()
			result, err := worker.RunThroughputBenchmark(clientWork, stopCh)
			sLogger.Infof("Worker %d completed with %v", workerId, err)
			if err != nil {
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

	sLogger.Infof("Started %d workers, now running for %s", *clients, runtime.String())
	deadline := time.Now().Add(runtime)
	awaitCompletion(stopCh, deadline, sLogger)
	stop()
	sLogger.Infof("Waiting for clients to stop..")
	wg.Wait()
	sLogger.Infof("Processing results..")

	return processThroughputResults(sLogger, *clients, resultChan)
}

func processLatencyResults(sLogger *zap.SugaredLogger, concurrency int, resultChan chan workerLatencyResult) int {
	// Collect results
	results := make([]workerLatencyResult, 0, concurrency)
	for i := 0; i < concurrency; i++ {
		sLogger.Infof("Waiting on result from %d", i)
		results = append(results, <-resultChan)
	}

	// Process results into one histogram and check for errors
	var combinedHistogram *hdrhistogram.Histogram
	for _, res := range results {
		if res.err != nil {
			sLogger.Errorf("Worker failed: %v", res.err)
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
	fmt.Printf("== Results ==\n")
	if combinedHistogram == nil {
		fmt.Printf("All workers failed.\n")
		return 1
	}
	for _, bracket := range combinedHistogram.CumulativeDistribution() {
		fmt.Printf("  %.2f: %.5fms (N=%d)\n", bracket.Quantile, float64(bracket.ValueAt)/1000, bracket.Count)
	}
	return 0
}

func processThroughputResults(sLogger *zap.SugaredLogger, concurrency int, resultChan chan workerThroughputResult) int {
	// Collect results
	results := make([]workerThroughputResult, 0, concurrency)
	for i := 0; i < concurrency; i++ {
		sLogger.Infof("Waiting on result from %d", i)
		results = append(results, <-resultChan)
	}

	totalRatePerSecond := 0.0
	allFailed := false
	for _, res := range results {
		if res.err != nil {
			sLogger.Errorf("Worker failed: %v", res.err)
			continue
		}
		allFailed = false
		totalRatePerSecond += res.ratePerSecond
	}

	// Report findings
	fmt.Printf("== Results ==\n")
	if allFailed {
		fmt.Printf("All workers failed.\n")
		return 1
	}
	fmt.Printf("  %.2f operations per second\n", totalRatePerSecond)
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

func initWorkload(path string, scale int64, seed int64, driver neo4j.Driver, logger *zap.Logger) error {
	if path == "builtin:tpcb-like" {
		return workload.InitTPCBLike(scale, driver, logger.Sugar())
	}
	return fmt.Errorf("init option is only supported for built-in workloads; if you want to initialize a database for a custom script, simply set up the database as you prefer first")
}

func createWorkload(path string, scale, seed int64) (workload.Workload, error) {
	if path == "builtin:tpcb-like" {
		return workload.Parse("builtin:tpcp-like", workload.TPCBLike, scale, seed)
	}

	scriptContent, err := ioutil.ReadFile(path)
	if err != nil {
		return workload.Workload{}, fmt.Errorf("failed to read workload file at %s: %s", path, err)
	}

	return workload.Parse(path, string(scriptContent), scale, seed)
}

func awaitCompletion(stopCh chan struct{}, deadline time.Time, sLogger *zap.SugaredLogger) {
	lastProgressReport := time.Now()
	for {
		select {
		case <-stopCh:
			return
		default:
		}

		now := time.Now()
		delta := deadline.Sub(now)
		if delta < 2*time.Second {
			sLogger.Infof("Wrapping up..")
			time.Sleep(delta)
			break
		}

		if now.Sub(lastProgressReport) > 15*time.Second {
			sLogger.Infof("%s remaining", delta.String())
			lastProgressReport = now
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func newDriver(url, user, password string, encrypted bool) (neo4j.Driver, error) {
	fmt.Printf("URL=%s, ENCRYPTED=%v\n", url, encrypted)
	config := func(conf *neo4j.Config) { conf.Encrypted = encrypted }
	return neo4j.NewDriver(url, neo4j.BasicAuth(user, password, ""), config)
}
