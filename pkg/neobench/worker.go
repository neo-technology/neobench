package neobench

import (
	"github.com/codahale/hdrhistogram"
	"github.com/neo4j/neo4j-go-driver/neo4j"
	"strings"
	"time"
)

type Worker struct {
	workerId int64
	driver   neo4j.Driver
	now      func() time.Time
}

// transactionRate is Time between transactions; this defines the workload rate
// if the database can't keep up at this pace the workload will report
// the latency as the time from when the transaction *would* have started,
// rather than from when it actually started.
//
// If transactionRate is 0, we go as fast as we can, this is used to measure throughput
func (w *Worker) RunBenchmark(wrk ClientWorkload, transactionRate time.Duration, stopCh <-chan struct{}) WorkerResult {
	session, err := w.driver.Session(neo4j.AccessModeWrite)
	if err != nil {
		return WorkerResult{WorkerId: w.workerId, Error: err}
	}
	defer session.Close()

	hdr := hdrhistogram.New(0, 60*60*1000000, 5)
	workStartTime := w.now()
	nextStart := workStartTime
	succeededCounter := int64(0)
	failedCounter := int64(0)
	failureGroups := make(map[string]FailureGroup)

	for {
		select {
		case <-stopCh:
			rate := float64(succeededCounter) / w.now().Sub(workStartTime).Seconds()
			return WorkerResult{
				WorkerId:           w.workerId,
				Rate:               rate,
				Latencies:          hdr,
				Succeeded:          succeededCounter,
				Failed:             failedCounter,
				FailedByErrorGroup: failureGroups,
			}
		default:
		}

		uow, err := wrk.Next()
		if err != nil {
			return WorkerResult{WorkerId: w.workerId, Error: err}
		}

		outcome := w.runUnit(session, uow)

		uowLatency := w.now().Sub(nextStart)

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
			// real users would see from when they ask the system to do something to when they get service. nextStart = nextStart.Add(transactionRate)
			if uowLatency < transactionRate {
				time.Sleep(transactionRate - uowLatency)
			}
		} else {
			// No rate limit set, so just track when each transaction started; this effectively
			// makes us coordinate with the database such that our workload rate exactly matches
			// the databases ability to process - eg. this measures throughput, but makes the
			// latencies useless
			nextStart = time.Now()
		}

		if outcome.succeeded {
			succeededCounter++
			if err = hdr.RecordValue(uowLatency.Microseconds()); err != nil {
				return WorkerResult{WorkerId: w.workerId, Error: err}
			}
		} else {
			failedCounter++
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
	}
}

func (w *Worker) runUnit(session neo4j.Session, uow UnitOfWork) uowOutcome {
	_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
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
	})

	if err != nil {
		return uowOutcome{
			succeeded:    false,
			failureGroup: groupError(err),
			err:          err,
		}
	}

	return uowOutcome{succeeded: true}
}

type WorkerResult struct {
	// Unique identifier for this worker
	WorkerId int64
	// If the worker crashed unrecoverably and exited early, this has the error cause
	// if this is set, the rest of this struct will be 0-ed
	Error error

	// Successful units of work per second
	Rate float64
	// Latencies for successful results
	Latencies *hdrhistogram.Histogram
	// Total successful units of work
	Succeeded int64
	// Total failed units of work
	Failed int64
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
