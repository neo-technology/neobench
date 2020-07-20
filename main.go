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
var rate = flag.Float64("r", 1, "transactions per second, total across all clients. This can be set to a fraction if you want")
var url = flag.String("a", "bolt://localhost:7687", "address to connect to, eg. bolt+routing://mydb:7687")
var user = flag.String("u", "neo4j", "username")
var password = flag.String("p", "neo4j", "password")
var encrypted = flag.Bool("e", true, "use encrypted connections")
var duration = flag.Int("d", 60, "seconds to run")
var workloadPath = flag.String("w", "builtin:tpcb-like", "workload to run")

func main() {
	flag.Parse()
	seed := time.Now().Unix()
	runtime := time.Duration(*duration) * time.Second

	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	sLogger := logger.Sugar()

	ratePerWorkerPerSecond := *rate / float64(*clients)
	ratePerWorkerDuration := time.Duration(1000*1000/ratePerWorkerPerSecond) * time.Microsecond

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

	stopCh, stop := pkg.SetupSignalHandler(sLogger)
	defer stop()

	resultChan := make(chan workerResult, *clients)
	var wg sync.WaitGroup
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		worker := pkg.NewWorker(driver, logger, ratePerWorkerDuration)
		workerId := i
		clientWork := wrk.NewClient()
		go func() {
			defer wg.Done()
			result, err := worker.Run(clientWork, stopCh)
			sLogger.Infof("Worker %d completed with %v", workerId, err)
			if err != nil {
				stop()
				resultChan <- workerResult{
					workerId: workerId,
					err:      err,
				}
				return
			}

			resultChan <- workerResult{
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

	exitCode := processResults(sLogger, *clients, resultChan)
	os.Exit(exitCode)
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

func processResults(sLogger *zap.SugaredLogger, concurrency int, resultChan chan workerResult) int {
	// Collect results
	results := make([]workerResult, 0, concurrency)
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

type workerResult struct {
	workerId int
	hdr      *hdrhistogram.Histogram
	err      error
}

func newDriver(url, user, password string, encrypted bool) (neo4j.Driver, error) {
	fmt.Printf("URL=%s, ENCRYPTED=%v\n", url, encrypted)
	config := func(conf *neo4j.Config) { conf.Encrypted = encrypted }
	return neo4j.NewDriver(url, neo4j.BasicAuth(user, password, ""), config)
}
