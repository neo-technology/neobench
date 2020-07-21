package neobench

import (
	"fmt"
	"github.com/codahale/hdrhistogram"
	"github.com/neo4j/neo4j-go-driver/neo4j"
	"time"
)

type Worker struct {
	driver neo4j.Driver
	now    func() time.Time
}

// transactionRate is Time between transactions; this defines the workload rate
// if the database can't keep up at this pace the workload will report
// the latency as the time from when the transaction *would* have started,
// rather than from when it actually started.
func (w *Worker) RunLatencyBenchmark(wrk ClientWorkload, transactionRate time.Duration, stopCh <-chan struct{}) (*hdrhistogram.Histogram, error) {
	session, err := w.driver.Session(neo4j.AccessModeWrite)
	if err != nil {
		return nil, err
	}
	defer session.Close()

	hdr := hdrhistogram.New(0, 60*60*1000000, 5)
	workStartTime := w.now()
	nextStart := workStartTime.Add(transactionRate)
	completedCounter := 0

	for {
		select {
		case <-stopCh:
			return hdr, nil
		default:
		}

		deltaStart := nextStart.Sub(w.now())
		if err = hdr.RecordValue((transactionRate - deltaStart).Microseconds()); err != nil {
			return nil, err
		}
		if deltaStart > 0 {
			time.Sleep(deltaStart)
		}

		nextStart = nextStart.Add(transactionRate)

		uow, err := wrk.Next()
		if err != nil {
			return nil, err
		}
		err = w.runUnit(session, uow)
		if err != nil {
			return nil, err
		}
		completedCounter += 1
	}
}

func (w *Worker) RunThroughputBenchmark(wrk ClientWorkload, stopCh <-chan struct{}) (float64, error) {
	session, err := w.driver.Session(neo4j.AccessModeWrite)
	if err != nil {
		return 0, err
	}
	defer session.Close()

	workStartTime := w.now()
	completedCounter := 0

	for {
		select {
		case <-stopCh:
			rate := float64(completedCounter) / float64(w.now().Sub(workStartTime).Seconds())
			return rate, nil
		default:
		}

		uow, err := wrk.Next()
		if err != nil {
			return 0, err
		}
		err = w.runUnit(session, uow)
		if err != nil {
			return 0, err
		}
		completedCounter += 1
	}
}

func (w *Worker) runUnit(session neo4j.Session, uow UnitOfWork) error {
	_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		for _, s := range uow.Statements {
			res, err := tx.Run(s.Query, s.Params)
			if err != nil {
				return nil, fmt.Errorf("query failed: %w", err)
			}
			_, err = res.Consume()
			if err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	return err
}

func NewWorker(driver neo4j.Driver) *Worker {
	return &Worker{
		driver: driver,
		now:    time.Now,
	}
}
