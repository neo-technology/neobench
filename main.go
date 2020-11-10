package main

import (
	"flag"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/neo4j"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"io/ioutil"
	"log"
	"math/rand"
	"neobench/pkg/neobench"
	"neobench/pkg/neobench/builtin"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var fInitMode bool
var fLatencyMode bool
var fScale int64
var fSeed int64
var fClients int
var fRate float64
var fAddress string
var fUser string
var fPassword string
var fEncryptionMode string
var fDuration int
var fProgress int
var fVariables map[string]string
var fWorkloads []string
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
	pflag.IntVar(&fProgress, "progress", 10, "interval, in seconds, to report progress")
	pflag.StringToStringVarP(&fVariables, "define", "D", nil, "defines variables for workload scripts and query parameters")
	pflag.StringSliceVarP(&fWorkloads, "workload", "w", []string{"builtin:tpcb-like"}, "workload to run, either a builtin: one or a path to a workload script")
	pflag.BoolVarP(&fLatencyMode, "latency", "l", false, "run in latency testing more rather than throughput mode")
	pflag.StringVarP(&fOutputFormat, "output", "o", "auto", "output format, `auto`, `interactive` or `csv`")
}

func main() {
	pflag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), `neobench is a benchmarking tool for Neo4j.

Usage:
  neobench [OPTION]... [DBNAME]

Options:
`)
		pflag.PrintDefaults()
	}
	pflag.Parse()
	if len(os.Args) == 1 {
		pflag.Usage()
		os.Exit(1)
	}

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

	dbName := ""
	if pflag.NArg() > 0 {
		dbName = pflag.Arg(0)
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

	wrk, err := createWorkload(driver, dbName, variables, seed)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	if fInitMode {
		err = initWorkload(fWorkloads, dbName, fScale, seed, driver, out)
		if err != nil {
			log.Fatalf("%+v", err)
		}
	}

	if fDuration == 0 {
		fmt.Printf("Duration (--duration) is 0, exiting without running any load\n")
		os.Exit(0)
	}

	progressInterval := time.Duration(fProgress) * time.Second

	if fLatencyMode {
		result, err := runBenchmark(driver, fAddress, dbName, scenario, out, wrk, runtime, fLatencyMode, fClients, fRate, progressInterval)
		if err != nil {
			out.Errorf(err.Error())
			os.Exit(1)
		}
		out.ReportLatency(result)
		if result.TotalFailed() == 0 {
			os.Exit(0)
		} else {
			os.Exit(1)
		}
	} else {
		result, err := runBenchmark(driver, fAddress, dbName, scenario, out, wrk, runtime, fLatencyMode, fClients, fRate, progressInterval)
		if err != nil {
			out.Errorf(err.Error())
			os.Exit(1)
		}
		out.ReportThroughput(result)
		if result.TotalFailed() == 0 {
			os.Exit(0)
		} else {
			os.Exit(1)
		}
	}
}

func createWorkload(driver neo4j.Driver, dbName string, variables map[string]interface{}, seed int64) (neobench.Workload, error) {
	var err error
	scripts := make([]neobench.Script, 0)
	for _, path := range fWorkloads {
		parts := strings.Split(path, "@")
		weight := 1.0
		if len(parts) > 1 {
			weight, err = strconv.ParseFloat(parts[1], 64)
			if err != nil {
				log.Fatalf("Failed to parse weight; value after @ symbol for workload weight must be a number: %s", path)
			}
			path = parts[0]
		}

		if strings.HasPrefix(path, "builtin:") {
			builtinScripts, err := loadBuiltinWorkload(path, weight)
			if err != nil {
				return neobench.Workload{}, errors.Wrapf(err, "failed to load script '%s'", path)
			}
			scripts = append(scripts, builtinScripts...)
		} else {
			script, err := loadScript(driver, dbName, variables, path, weight)
			if err != nil {
				return neobench.Workload{}, errors.Wrapf(err, "failed to load script '%s'", path)
			}
			scripts = append(scripts, script)
		}
	}

	return neobench.Workload{
		Variables: variables,
		Scripts:   neobench.NewScripts(scripts...),
		Rand:      rand.New(rand.NewSource(seed)),
	}, err
}

func loadScript(driver neo4j.Driver, dbName string, vars map[string]interface{}, path string, weight float64) (neobench.Script, error) {
	scriptContent, err := ioutil.ReadFile(path)
	if err != nil {
		return neobench.Script{}, fmt.Errorf("failed to read workload file at %s: %s", path, err)
	}

	script, err := neobench.Parse(path, string(scriptContent), weight)
	if err != nil {
		return neobench.Script{}, err
	}

	readonly, err := neobench.WorkloadPreflight(driver, dbName, script, vars)
	script.Readonly = readonly
	return script, err
}

func loadBuiltinWorkload(path string, weight float64) ([]neobench.Script, error) {
	if path == "builtin:tpcb-like" {
		script, err := neobench.Parse("builtin:tpcp-like", builtin.TPCBLike, weight)
		return []neobench.Script{script}, err
	}

	if path == "builtin:match-only" {
		script, err := neobench.Parse("builtin:match-only", builtin.MatchOnly, weight)
		return []neobench.Script{script}, err
	}

	if path == "builtin:ldbc-like" {
		ic2Rate, ic6Rate, ic10Rate, ic14Rate := 37.0, 129.0, 30.0, 49.0
		totalRate := ic2Rate + ic6Rate + ic10Rate + ic14Rate
		ic2, err := neobench.Parse("builtin:ldbc-like/ic2", builtin.LDBCIC2, ic2Rate/totalRate*weight)
		if err != nil {
			return []neobench.Script{}, err
		}
		ic6, err := neobench.Parse("builtin:ldbc-like/ic6", builtin.LDBCIC6, ic6Rate/totalRate*weight)
		if err != nil {
			return []neobench.Script{}, err
		}
		ic10, err := neobench.Parse("builtin:ldbc-like/ic10", builtin.LDBCIC10, ic10Rate/totalRate*weight)
		if err != nil {
			return []neobench.Script{}, err
		}
		ic14, err := neobench.Parse("builtin:ldbc-like/ic14", builtin.LDBCIC14, ic14Rate/totalRate*weight)
		if err != nil {
			return []neobench.Script{}, err
		}
		return []neobench.Script{
			ic2,
			ic6,
			ic10,
			ic14,
		}, err
	}

	if path == "builtin:ldbc-like/ic2" {
		script, err := neobench.Parse("builtin:ldbc-like/ic2", builtin.LDBCIC2, weight)
		return []neobench.Script{script}, err
	}

	if path == "builtin:ldbc-like/ic6" {
		script, err := neobench.Parse("builtin:ldbc-like/ic6", builtin.LDBCIC6, weight)
		return []neobench.Script{script}, err
	}

	if path == "builtin:ldbc-like/ic10" {
		script, err := neobench.Parse("builtin:ldbc-like/ic10", builtin.LDBCIC10, weight)
		return []neobench.Script{script}, err
	}

	if path == "builtin:ldbc-like/ic14" {
		script, err := neobench.Parse("builtin:ldbc-like/ic14", builtin.LDBCIC14, weight)
		return []neobench.Script{script}, err
	}

	return []neobench.Script{}, fmt.Errorf("unknown built-in workload: %s, supported built-in workloads are 'tpcb-like', 'match-only' and 'ldbc-like'", path)
}

func describeScenario() string {
	out := strings.Builder{}
	for _, path := range fWorkloads {
		out.WriteString(fmt.Sprintf(" -w %s", path))
	}
	out.WriteString(fmt.Sprintf(" -c %d", fClients))
	out.WriteString(fmt.Sprintf(" -s %d", fScale))
	out.WriteString(fmt.Sprintf(" -d %d", fDuration))
	out.WriteString(fmt.Sprintf(" -e %s", fEncryptionMode))
	if fLatencyMode {
		out.WriteString(fmt.Sprintf(" -l -r %.3f", fRate))
	}
	if fInitMode {
		out.WriteString(" -i")
	}
	return out.String()
}

func runBenchmark(driver neo4j.Driver, url, databaseName, scenario string, out neobench.Output, wrk neobench.Workload,
	runtime time.Duration, latencyMode bool, numClients int, rate float64, progressInterval time.Duration) (neobench.Result, error) {
	stopCh, stop := neobench.SetupSignalHandler()
	defer stop()

	ratePerWorkerDuration := time.Duration(0)
	if latencyMode {
		ratePerWorkerDuration = neobench.TotalRatePerSecondToDurationPerClient(numClients, rate)
	}

	out.BenchmarkStart(databaseName, url)

	resultChan := make(chan neobench.WorkerResult, numClients)
	resultRecorders := make([]*neobench.ResultRecorder, 0)
	var wg sync.WaitGroup
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		recorder := neobench.NewResultRecorder(int64(i))
		resultRecorders = append(resultRecorders, recorder)
		worker := neobench.NewWorker(driver, int64(i))
		workerId := i
		clientWork := wrk.NewClient()
		go func() {
			defer wg.Done()
			result := worker.RunBenchmark(clientWork, databaseName, ratePerWorkerDuration, 0, stopCh, recorder)
			resultChan <- result
			if result.Error != nil {
				out.Errorf("worker %d crashed: %s", workerId, result.Error)
				stop()
			}
		}()
	}

	deadline := time.Now().Add(runtime)
	awaitCompletion(stopCh, deadline, out, databaseName, scenario, progressInterval, resultRecorders)
	stop()
	wg.Wait()

	return collectResults(databaseName, scenario, out, numClients, resultChan)
}

func collectResults(databaseName, scenario string, out neobench.Output, concurrency int, resultChan chan neobench.WorkerResult) (neobench.Result, error) {
	// Collect results
	results := make([]neobench.WorkerResult, 0, concurrency)
	for i := 0; i < concurrency; i++ {
		results = append(results, <-resultChan)
	}

	total := neobench.NewResult(databaseName, scenario)
	// Process results into one histogram and check for errors
	for _, res := range results {
		if res.Error != nil {
			out.Errorf("Worker failed: %v", res.Error)
			continue
		}
		total.Add(res)
	}

	return total, nil
}

func initWorkload(paths []string, dbName string, scale, seed int64, driver neo4j.Driver, out neobench.Output) error {
	for _, path := range paths {
		if path == "builtin:tpcb-like" {
			return builtin.InitTPCBLike(scale, dbName, driver, out)
		}
		if path == "builtin:match-only" {
			return builtin.InitTPCBLike(scale, dbName, driver, out)
		}
		if path == "builtin:ldbc-like" {
			return builtin.InitLDBCLike(scale, seed, dbName, driver, out)
		}
	}
	return nil
}

func awaitCompletion(stopCh chan struct{}, deadline time.Time, out neobench.Output, databaseName, scenario string, progressInterval time.Duration, recorders []*neobench.ResultRecorder) {
	nextProgressReport := time.Now().Add(progressInterval)
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

		if now.After(nextProgressReport) {
			nextProgressReport = nextProgressReport.Add(progressInterval)
			checkpoint := neobench.NewResult(databaseName, scenario)
			for _, r := range recorders {
				checkpoint.Add(r.ProgressReport(time.Now()))
			}

			completeness := 1 - delta.Seconds()/originalDelta
			out.ReportWorkloadProgress(completeness, checkpoint)
		}
		time.Sleep(time.Millisecond * 100)
	}
}
