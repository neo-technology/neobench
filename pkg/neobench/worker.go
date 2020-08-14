package neobench

import (
	"github.com/codahale/hdrhistogram"
	"github.com/neo4j/neo4j-go-driver/neo4j"
	"github.com/pkg/errors"
	"strings"
	"time"
)

type Worker struct {
	workerId int64
	driver   neo4j.Driver
	now      func() time.Time
	sleep    func(duration time.Duration)
}

// transactionRate is Time between transactions; this defines the workload rate
// if the database can't keep up at this pace the workload will report
// the latency as the time from when the transaction *would* have started,
// rather than from when it actually started.
//
// If transactionRate is 0, we go as fast as we can, this is used to measure throughput
// If numTransactions is 0, we go until stopCh tells us to stop
func (w *Worker) RunBenchmark(wrk ClientWorkload, databaseName string, transactionRate time.Duration,
	numTransactions uint64, stopCh <-chan struct{}) WorkerResult {
	session, err := w.driver.NewSession(neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: databaseName,
	})
	if err != nil {
		return WorkerResult{WorkerId: w.workerId, Error: err}
	}
	defer session.Close()

	workStartTime := w.now()
	nextStart := workStartTime
	failureGroups := make(map[string]FailureGroup)

	scriptStats := make(map[string]*ScriptResult)
	transactionCounter := uint64(0)

	for {
		select {
		case <-stopCh:
			return WorkerResult{
				WorkerId:           w.workerId,
				Scripts:            w.gatherResults(scriptStats, workStartTime),
				FailedByErrorGroup: failureGroups,
			}
		default:
		}

		uow, err := wrk.Next()
		if err != nil {
			return WorkerResult{WorkerId: w.workerId, Error: err}
		}
		stats, found := scriptStats[uow.ScriptName]
		if !found {
			stats = &ScriptResult{
				ScriptName: uow.ScriptName,
				Latencies:  hdrhistogram.New(0, 60*60*1000000, 5),
			}
			scriptStats[uow.ScriptName] = stats
		}

		outcome := w.runUnit(session, uow)

		uowLatency := w.now().Sub(nextStart)

		if outcome.succeeded {
			stats.Succeeded++
			if err = stats.Latencies.RecordValue(uowLatency.Microseconds()); err != nil {
				return WorkerResult{WorkerId: w.workerId, Error: errors.Wrapf(err, "failed to record latency: %s", uowLatency)}
			}
		} else {
			stats.Failed++
			failedGroup, found := failureGroups[outcome.failureGroup]
			if !found {
				failureGroups[outcome.failureGroup] = FailureGroup{
					Count:        1,
					FirstFailure: outcome.err,
				}
			} else {
				failureGroups[outcome.failureGroup] = FailureGroup{
					Count:        failedGroup.Count + 1,
					FirstFailure: failedGroup.FirstFailure,
				}
			}
		}

		transactionCounter++
		if numTransactions != 0 && transactionCounter >= numTransactions {
			return WorkerResult{
				WorkerId:           w.workerId,
				Scripts:            w.gatherResults(scriptStats, workStartTime),
				FailedByErrorGroup: failureGroups,
			}
		}

		if transactionRate > 0 {
			// Note something critical here: We don't add the actual time the unit took,
			// we add the *max* time it *should* have taken. This means that if the database
			// is not keeping up with the workload, nextStart will drift further and further
			// behind wall clock time. This is what corrects for coordinated omission; we're measuring
			// the start time given a rate of users showing up and making request that is independent
			// of the rate the database processes them at.
			//
			// If the database isn't keeping up,
			// then the latency numbers will grow extremely large, showing the actual wait time
			// real users would see from when they ask the system to do something to when they get service.
			if uowLatency < transactionRate {
				w.sleep(transactionRate - uowLatency)
			}
			nextStart = nextStart.Add(transactionRate)
		} else {
			// No rate limit set, so just track when each transaction started; this effectively
			// makes us coordinate with the database such that our workload rate exactly matches
			// the databases ability to process - eg. this measures throughput, but makes the
			// latencies useless
			nextStart = time.Now()
		}
	}
}

func (w *Worker) gatherResults(workloadStats map[string]*ScriptResult, workStartTime time.Time) []ScriptResult {
	workloadResults := make([]ScriptResult, 0, len(workloadStats))
	for _, result := range workloadStats {
		workloadResults = append(workloadResults, ScriptResult{
			ScriptName: result.ScriptName,
			Rate:       float64(result.Succeeded+result.Failed) / w.now().Sub(workStartTime).Seconds(),
			Failed:     result.Failed,
			Succeeded:  result.Succeeded,
			Latencies:  result.Latencies,
		})
	}
	return workloadResults
}

func (w *Worker) runUnit(session neo4j.Session, uow UnitOfWork) uowOutcome {
	transaction := func(tx neo4j.Transaction) (interface{}, error) {
		for _, s := range uow.Statements {
			res, err := tx.Run(s.Query, s.Params)
			if err != nil {
				return nil, err
			}
			_, err = res.Consume()
			if err != nil {
				return nil, err
			}
		}
		return nil, nil
	}

	var err error
	if uow.Readonly {
		_, err = session.ReadTransaction(transaction)
	} else {
		_, err = session.WriteTransaction(transaction)
	}

	if err != nil {
		return uowOutcome{
			succeeded:    false,
			failureGroup: groupError(err),
			err:          err,
		}
	}

	return uowOutcome{succeeded: true}
}

// Converts a total target rate into a per-client "pacing" duration, used to slow down workers to match
// the target rate.
func TotalRatePerSecondToDurationPerClient(numClients int, rate float64) time.Duration {
	ratePerWorkerPerSecond := rate / float64(numClients)
	return time.Duration(1000*1000/ratePerWorkerPerSecond) * time.Microsecond
}

type WorkerResult struct {
	// Unique identifier for this worker
	WorkerId int64
	// If the worker crashed unrecoverably and exited early, this has the error cause
	// if this is set, the rest of this struct will be 0-ed
	Error error

	// Statistics grouped by scripts this worker ran
	Scripts []ScriptResult

	// Failure counts by cause
	FailedByErrorGroup map[string]FailureGroup
}

// Combines the count with the last error we saw, to help users see what the errors were
type FailureGroup struct {
	Count        int64
	FirstFailure error
}

func groupError(err error) string {
	msg := err.Error()
	if strings.HasPrefix(msg, "Server error: [") {
		return strings.Split(strings.Split(msg, "[")[1], "]")[0]
	}
	return "unknown"
}

type uowOutcome struct {
	succeeded bool
	// An opaque string used to group errors; we track counts for each unique string
	failureGroup string
	err          error
}

func NewWorker(driver neo4j.Driver) *Worker {
	return &Worker{
		driver: driver,
		now:    time.Now,
	}
}
