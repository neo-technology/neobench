package main

import (
	"flag"
	"fmt"
	"github.com/codahale/hdrhistogram"
	"github.com/neo4j/neo4j-go-driver/neo4j"
	"github.com/spf13/pflag"
	"io/ioutil"
	"log"
	"neobench/pkg/neobench"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var fInitMode bool
var fLatencyMode bool
var fScale int64
var fClients int
var fRate float64
var fAddress string
var fUser string
var fPassword string
var fEncryptionMode string
var fDuration int
var fVariables map[string]string
var fWorkloadPath string
var fOutputFormat string

func init() {
	pflag.BoolVarP(&fInitMode, "init", "i", false, "run in initialization mode; if using built-in workloads this creates the initial dataset")
	pflag.Int64VarP(&fScale, "scale", "s", 1, "sets the `scale` variable, impact depends on workload")
	pflag.IntVarP(&fClients, "clients", "c", 1, "number of concurrent clients / sessions")
	pflag.Float64VarP(&fRate, "rate", "r", 1, "in latency mode (see -l) this sets transactions per second, total across all clients")
	pflag.StringVarP(&fAddress, "address", "a", "neo4j://localhost:7687", "address to connect to, eg. neo4j://mydb:7687")
	pflag.StringVarP(&fUser, "user", "u", "neo4j", "username")
	pflag.StringVarP(&fPassword, "password", "p", "neo4j", "password")
	pflag.StringVarP(&fEncryptionMode, "encryption", "e", "auto", "whether to use encryption, `auto`, `true` or `false`")
	pflag.IntVarP(&fDuration, "duration", "d", 60, "seconds to run")
	pflag.StringToStringVarP(&fVariables, "define", "D", nil, "defines variables for workload scripts and query parameters")
	pflag.StringVarP(&fWorkloadPath, "workload", "w", "builtin:tpcb-like", "workload to run, either a builtin: one or a path to a workload script")
	pflag.BoolVarP(&fLatencyMode, "latency", "l", false, "run in latency testing more rather than throughput mode")
	pflag.StringVarP(&fOutputFormat, "output", "o", "auto", "output format, `auto`, `interactive` or `csv`")
}

func main() {
	pflag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), `neobench - scriptable benchmarks for Neo4j

  neobench runs a canned or user-defined workload against a Neo4j Database.
  By default, it measures the maximum throughput it can achieve, but it can also
  measure latencies.

  There is one built-in workload, used by default: builtin:tpcb-like, it is very 
  similar to the tpcb-like workload found in pgbench.

Usage:

`)
		pflag.PrintDefaults()
	}
	pflag.Parse()

	seed := time.Now().Unix()
	runtime := time.Duration(fDuration) * time.Second
	scenario := describeScenario()

	out, err := neobench.NewOutput(fOutputFormat)
	if err != nil {
		log.Fatal(err)
	}

	var encryptionMode neobench.EncryptionMode
	switch strings.ToLower(fEncryptionMode) {
	case "auto":
		encryptionMode = neobench.EncryptionAuto
	case "true", "yes", "y", "1":
		encryptionMode = neobench.EncryptionOn
	case "false", "no", "n", "0":
		encryptionMode = neobench.EncryptionOff
	default:
		log.Fatalf("Invalid encryption mode '%s', needs to be one of 'auto', 'true' or 'false'", fEncryptionMode)
	}

	driver, err := neobench.NewDriver(fAddress, fUser, fPassword, encryptionMode)
	if err != nil {
		log.Fatal(err)
	}

	variables := make(map[string]interface{})
	variables["scale"] = fScale
	for k, v := range fVariables {
		intVal, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			variables[k] = intVal
			continue
		}
		floatVal, err := strconv.ParseFloat(v, 64)
		if err == nil {
			variables[k] = floatVal
			continue
		}
		log.Fatalf("-D and --define values must be integers or floats, failing to parse '%s': %s", v, err)
	}

	wrk, err := createWorkload(fWorkloadPath, seed, variables)
	if err != nil {
		log.Fatal(err)
	}

	if fInitMode {
		err = initWorkload(fWorkloadPath, fScale, driver, out)
		if err != nil {
			log.Fatal(err)
		}
	}

	if fLatencyMode {
		result, err := runBenchmark(driver, scenario, out, wrk, runtime, fLatencyMode, fClients, fRate)
		if err != nil {
			out.Errorf(err.Error())
			os.Exit(1)
		}
		out.ReportLatency(result)
		os.Exit(0)
	} else {
		result, err := runBenchmark(driver, scenario, out, wrk, runtime, fLatencyMode, fClients, fRate)
		if err != nil {
			out.Errorf(err.Error())
			os.Exit(1)
		}
		out.ReportThroughput(result)
		os.Exit(0)
	}
}

func describeScenario() string {
	out := strings.Builder{}
	out.WriteString(fmt.Sprintf(" -w %s", fWorkloadPath))
	out.WriteString(fmt.Sprintf(" -c %d", fClients))
	out.WriteString(fmt.Sprintf(" -s %d", fScale))
	out.WriteString(fmt.Sprintf(" -d %d", fDuration))
	out.WriteString(fmt.Sprintf(" -e %s", fEncryptionMode))
	if fLatencyMode {
		out.WriteString(fmt.Sprintf("-l -r %f", fRate))
	}
	if fInitMode {
		out.WriteString(" -i")
	}
	return out.String()
}

func runBenchmark(driver neo4j.Driver, scenario string, out neobench.Output, wrk neobench.Workload, runtime time.Duration,
	latencyMode bool, numClients int, rate float64) (neobench.Result, error) {
	stopCh, stop := neobench.SetupSignalHandler()
	defer stop()

	ratePerWorkerDuration := time.Duration(0)
	if latencyMode {
		ratePerWorkerPerSecond := rate / float64(numClients)
		ratePerWorkerDuration = time.Duration(1000*1000/ratePerWorkerPerSecond) * time.Microsecond
	}

	resultChan := make(chan neobench.WorkerResult, numClients)
	var wg sync.WaitGroup
	for i := 0; i < numClients; i++ {
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

	return collectResults(scenario, out, numClients, resultChan)
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

func createWorkload(path string, seed int64, vars map[string]interface{}) (neobench.Workload, error) {
	if path == "builtin:tpcb-like" {
		return neobench.Parse("builtin:tpcp-like", neobench.TPCBLike, vars, seed)
	}

	scriptContent, err := ioutil.ReadFile(path)
	if err != nil {
		return neobench.Workload{}, fmt.Errorf("failed to read workload file at %s: %s", path, err)
	}

	return neobench.Parse(path, string(scriptContent), vars, seed)
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
