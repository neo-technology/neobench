package main

import (
	"flag"
	"fmt"
	"github.com/codahale/hdrhistogram"
	"go.uber.org/zap"
	"log"
	"neobench/pkg"
	"neobench/pkg/workload/tpcb"
	"sync"
	"time"
)
import "github.com/neo4j/neo4j-go-driver/neo4j"

var initMode = flag.Bool("i", false, "initialize dataset before running workload")
var scale = flag.Int("s", 1, "scale factor, effect depends on workload but in general this scales the size of the dataset linearly")
var clients = flag.Int("c", 1, "number of clients, ie. number of concurrent simulated database sessions")
var rate = flag.Float64("r", 1, "transactions per second, total across all clients. This can be set to a fraction if you want")
var url = flag.String("a", "bolt://localhost:7687", "address to connect to, eg. bolt+routing://mydb:7687")
var user = flag.String("u", "neo4j", "username")
var password = flag.String("p", "neo4j", "password")
var encrypted = flag.Bool("e", true, "use encrypted connections")
var duration = flag.Int("d", 60, "seconds to run")


func main() {
	flag.Parse()
	//
	//url := "bolt+routing://35a43747-launch41.databases.neo4j.io:7687" // hammer4
	//password := "LjlKod4P4Xea8afhDFWVMZuMLq34KjExwMt5-Br2MI4" // hammer4
	//url := "bolt+routing://088a4483-launch41.databases.neo4j.io:7687" // hammer3
	//password := "5MxfzeALfFuRjJX7VwB8UdHfua-A7_juUoYqtc1J9jc" // hammer3
	//user := "neo4j"
	//
	//encrypted := true
	//scale := 5
	//rate := 60
	//runtime := 5 * 60 * time.Second
	//
	//concurrency := 4

	runtime := time.Duration(*duration) * time.Second

	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	sLogger := logger.Sugar()

	ratePerWorkerPerSecond := *rate / float64(*clients)
	ratePerWorkerDuration := time.Duration(1000 * 1000 / ratePerWorkerPerSecond) * time.Microsecond

	driver, err := newDriver(*url, *user, *password, *encrypted)
	if err != nil {
		log.Fatal(err)
	}

	workload := tpcb.NewTpcB(*scale)

	if *initMode {
		err = workload.Initialize(driver, sLogger)
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
		go func() {
			defer wg.Done()
			result, err := worker.Run(workload, stopCh)
			sLogger.Infof("Worker %d completed with %v", workerId, err)
			if err != nil {
				stop()
				resultChan <- workerResult{
					workerId: workerId,
					err: err,
				}
				return
			}

			resultChan <- workerResult{
				workerId: workerId,
				hdr: result,
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

	processResults(sLogger, *clients, resultChan)
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

func processResults(sLogger *zap.SugaredLogger, concurrency int, resultChan chan workerResult) {
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
		return
	}
	for _, bracket := range combinedHistogram.CumulativeDistribution() {
		fmt.Printf("  %.2f: %.5fms (N=%d)\n", bracket.Quantile, float64(bracket.ValueAt)/1000, bracket.Count)
	}
}

type workerResult struct {
	workerId int
	hdr *hdrhistogram.Histogram
	err error
}

func newDriver(url, user, password string, encrypted bool) (neo4j.Driver, error) {
	config := func(conf *neo4j.Config) { conf.Encrypted = encrypted }
	return neo4j.NewDriver(url, neo4j.BasicAuth(user, password, ""), config)
}
