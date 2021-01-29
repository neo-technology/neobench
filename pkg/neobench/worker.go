package neobench

import (
	"fmt"
	"github.com/codahale/hdrhistogram"
	"github.com/neo4j/neo4j-go-driver/neo4j"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"strings"
	"sync"
	"time"
)

type Worker struct {
	workerId int64
	tracer   opentracing.Tracer
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
	numTransactions uint64, stopCh <-chan struct{}, recorder *ResultRecorder) WorkerResult {
	session, err := w.driver.NewSession(neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: databaseName,
	})
	if err != nil {
		return WorkerResult{WorkerId: w.workerId, Error: err}
	}
	defer session.Close()

	workStartTime := w.now()
	recorder.totalStart = workStartTime
	recorder.currentStart = workStartTime

	nextStart := workStartTime

	transactionCounter := uint64(0)

	for {
		select {
		case <-stopCh:
			return recorder.Complete(w.now())
		default:
		}

		uow, err := wrk.Next(w.workerId)
		if err != nil {
			return WorkerResult{WorkerId: w.workerId, Error: err}
		}

		outcome := w.runUnit(session, uow)

		uowLatency := w.now().Sub(nextStart)

		if err = recorder.record(uow.ScriptName, uowLatency, outcome); err != nil {
			return WorkerResult{WorkerId: w.workerId, Error: err}
		}

		transactionCounter++
		if numTransactions != 0 && transactionCounter >= numTransactions {
			return recorder.Complete(w.now())
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
	span := w.tracer.StartSpan("tx").SetTag("worker", w.workerId)
	defer span.Finish()
	transaction := func(tx neo4j.Transaction) (interface{}, error) {
		for _, s := range uow.Statements {
			{
				qspan := w.tracer.StartSpan("query", opentracing.ChildOf(span.Context())).SetTag("query", s.Query)
				defer qspan.Finish()
				res, err := tx.Run(s.Query, s.Params)
				if err != nil {
					return nil, err
				}
				_, err = res.Consume()
				if err != nil {
					return nil, err
				}
			}
		}
		return nil, nil
	}

	var err error
	if uow.Readonly {
		span.SetTag("mode", "readonly")
		_, err = session.ReadTransaction(transaction, txAsChildOfSpan(w.tracer, span))
	} else {
		span.SetTag("mode", "write")
		_, err = session.WriteTransaction(transaction, txAsChildOfSpan(w.tracer, span))
	}

	if err != nil {
		span.SetTag("outcome", fmt.Sprintf("err: %s", err))
		return uowOutcome{
			succeeded:    false,
			failureGroup: groupError(err),
			err:          err,
		}
	}

	span.SetTag("outcome", "ok")
	return uowOutcome{succeeded: true}
}

// transaction modified that adds the given span as a parent for neo4j to pick up and trace with
func txAsChildOfSpan(tracer opentracing.Tracer, span opentracing.Span) func(config *neo4j.TransactionConfig) {
	serializedSpan := make(map[string]string)
	carrier := opentracing.TextMapCarrier(serializedSpan)
	err := tracer.Inject(span.Context(), opentracing.TextMap, carrier)
	if err != nil {
		panic(err)
	}
	return func(config *neo4j.TransactionConfig) {
		config.Metadata = map[string]interface{}{
			"opentrace.ctx": serializedSpan,
		}
	}
}

// Converts a total target rate into a per-client "pacing" duration, used to slow down workers to match
// the target rate.
func TotalRatePerSecondToDurationPerClient(numClients int, rate float64) time.Duration {
	ratePerWorkerPerSecond := rate / float64(numClients)
	return time.Duration(1000*1000/ratePerWorkerPerSecond) * time.Microsecond
}

// Concurrent data structure; used by the worker to record progress, accessible from other threads
// to read progress checkpoints.
type ResultRecorder struct {
	mut sync.Mutex

	// Stats since last progress report, read and reset by calling ProgressReport
	current      WorkerResult
	currentStart time.Time

	// Total since the workload started
	total      WorkerResult
	totalStart time.Time
}

func NewResultRecorder(workerId int64) *ResultRecorder {
	return &ResultRecorder{
		current: NewWorkerResult(workerId),
		total:   NewWorkerResult(workerId),
	}
}

func (t *ResultRecorder) record(scriptName string, latency time.Duration, outcome uowOutcome) error {
	t.mut.Lock()
	defer t.mut.Unlock()

	if err := t.current.record(scriptName, latency, outcome); err != nil {
		return err
	}
	return t.total.record(scriptName, latency, outcome)
}

// Reports progress since last time you called this function
func (t *ResultRecorder) ProgressReport(now time.Time) WorkerResult {
	t.mut.Lock()
	defer t.mut.Unlock()

	out := t.current

	delta := now.Sub(t.currentStart)
	out.calculateRate(delta)

	t.current = NewWorkerResult(out.WorkerId)
	t.currentStart = now

	return out
}

func (t *ResultRecorder) Complete(now time.Time) WorkerResult {
	t.mut.Lock()
	defer t.mut.Unlock()

	out := t.total

	delta := now.Sub(t.totalStart)
	out.calculateRate(delta)

	// Not needed at the time of writing this, but since we're returning pointers
	// (the maps etc inside t.total), clear this structures references before we exit the mutex
	t.total = NewWorkerResult(out.WorkerId)
	t.totalStart = now

	return out
}

func NewWorkerResult(workerId int64) WorkerResult {
	return WorkerResult{
		WorkerId:           workerId,
		Scripts:            make(map[string]*ScriptResult),
		FailedByErrorGroup: make(map[string]FailureGroup),
	}
}

type WorkerResult struct {
	// Unique identifier for this worker
	WorkerId int64
	// If the worker crashed unrecoverably and exited early, this has the error cause
	// if this is set, the rest of this struct will be 0-ed
	Error error

	// Statistics grouped by scripts this worker ran
	Scripts map[string]*ScriptResult

	// Failure counts by cause
	FailedByErrorGroup map[string]FailureGroup
}

func (r *WorkerResult) getOrCreateScriptResult(scriptName string) *ScriptResult {
	stats, found := r.Scripts[scriptName]
	if found {
		return stats
	}
	stats = &ScriptResult{
		ScriptName: scriptName,
		Latencies:  hdrhistogram.New(0, 60*60*1000000, 5),
	}
	r.Scripts[scriptName] = stats
	return stats
}

func (r *WorkerResult) record(scriptName string, latency time.Duration, outcome uowOutcome) error {
	stats, found := r.Scripts[scriptName]
	if !found {
		stats = &ScriptResult{
			ScriptName: scriptName,
			Latencies:  hdrhistogram.New(0, 60*60*1000000, 3),
		}
		r.Scripts[scriptName] = stats
	}

	if outcome.succeeded {
		stats.Succeeded++
		if err := stats.Latencies.RecordValue(latency.Microseconds()); err != nil {
			return errors.Wrapf(err, "failed to record latency: %s", latency)
		}
	} else {
		stats.Failed++
		failedGroup, found := r.FailedByErrorGroup[outcome.failureGroup]
		if !found {
			r.FailedByErrorGroup[outcome.failureGroup] = FailureGroup{
				Count:        1,
				FirstFailure: outcome.err,
			}
		} else {
			r.FailedByErrorGroup[outcome.failureGroup] = FailureGroup{
				Count:        failedGroup.Count + 1,
				FirstFailure: failedGroup.FirstFailure,
			}
		}
	}
	return nil
}

// Calculates the throughput rate for each script in this result, given the delta time it took the
// workload to run.
func (r *WorkerResult) calculateRate(delta time.Duration) {
	for _, script := range r.Scripts {
		script.Rate = (float64(script.Succeeded+script.Failed) / float64(delta.Microseconds())) * 1000 * 1000
	}
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

func NewWorker(driver neo4j.Driver, tracer opentracing.Tracer, workerId int64) *Worker {
	return &Worker{
		workerId: workerId,
		tracer:   tracer,
		driver:   driver,
		now:      time.Now,
		sleep:    time.Sleep,
	}
}
