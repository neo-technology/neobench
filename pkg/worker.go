package pkg

import (
	"fmt"
	"github.com/codahale/hdrhistogram"
	"github.com/neo4j/neo4j-go-driver/neo4j"
	"go.uber.org/zap"
	"neobench/pkg/workload"
	"time"
)

type Worker struct {
	driver neo4j.Driver
	logger *zap.Logger
	// Time between transactions; this defines the workload rate
	// if the database can't keep up at this pace the workload will report
	// the latency as the time from when the transaction *would* have started,
	// rather than from when it actually started.
	transactionRate time.Duration
	now func() time.Time
}

func (w *Worker) Run(wrk workload.ClientWorkload, stopCh <-chan struct{}) (*hdrhistogram.Histogram, error) {
	session, err := w.driver.Session(neo4j.AccessModeWrite)
	if err != nil {
		return nil, err
	}
	defer session.Close()

	hdr := hdrhistogram.New(0, 60*60*1000000, 5)
	workStartTime := w.now()
	nextStart := workStartTime.Add(w.transactionRate)
	completedCounter := 0

	for {
		select {
		case <- stopCh:
			return hdr, nil
		default:
		}

		deltaStart := nextStart.Sub(w.now())
		if err = hdr.RecordValue((w.transactionRate - deltaStart).Microseconds()); err != nil {
			return nil, err
		}
		if deltaStart < 0 {
			w.logger.With().Warn("database is not keeping up with requested rate",
				zap.Int64("behindMs", deltaStart.Milliseconds()))
		} else {
			time.Sleep(deltaStart)
		}

		nextStart = nextStart.Add(w.transactionRate)

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

func (w *Worker) runUnit(session neo4j.Session, uow workload.UnitOfWork) error {
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

func NewWorker(driver neo4j.Driver, logger *zap.Logger, rate time.Duration) *Worker {
	return &Worker{
		driver: driver,
		logger: logger,
		transactionRate: rate,
		now: time.Now,
	}
}