package neobench

import (
	"fmt"
	"github.com/neo4j/neo4j-go-driver/neo4j"
	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"net/url"
	"testing"
	"time"
)

func TestMaintainsRateInFaceOfFailure(t *testing.T) {
	r := rand.New(rand.NewSource(1337))
	stopCh := make(chan struct{})
	clock := &fakeSpaceTimeContinuum{}
	clock.currentTime = time.Date(2020, 1, 1, 1, 1, 1, 1, time.UTC)
	driver := &fakeDriver{
		clock:       clock,
		r:           r,
		failureRate: 0.2,
		minLatency:  2 * time.Millisecond,
		maxLatency:  2000 * time.Millisecond,
	}
	w := Worker{
		workerId: 0,
		driver:   driver,
		tracer:   opentracing.NoopTracer{},
		now:      clock.now,
		sleep:    clock.sleep,
	}
	rec := NewResultRecorder(0)

	targetRatePerSecond := float64(1)
	txDuration := TotalRatePerSecondToDurationPerClient(1, targetRatePerSecond)

	result := w.RunBenchmark(newTestWorkload(r), "", txDuration, 100, stopCh, rec)

	assert.NoError(t, result.Error)
	sr := result.Scripts["workertest"]
	assert.InDelta(t, targetRatePerSecond, sr.Rate, 0.1)
}

func newTestWorkload(r *rand.Rand) ClientWorkload {
	script, err := Parse("workertest", `RETURN 1;`, 1)
	if err != nil {
		panic(err)
	}
	wrkld := ClientWorkload{
		Scripts: NewScripts(script),
		Rand:    r,
	}
	return wrkld
}

type fakeSpaceTimeContinuum struct {
	currentTime time.Time
}

func (c *fakeSpaceTimeContinuum) now() time.Time {
	return c.currentTime
}

// Returns immediately, just moves the clock forward
func (c *fakeSpaceTimeContinuum) sleep(duration time.Duration) {
	c.currentTime = c.currentTime.Add(duration)
}

type fakeDriver struct {
	clock       *fakeSpaceTimeContinuum
	r           *rand.Rand
	failureRate float64
	minLatency  time.Duration
	maxLatency  time.Duration
}

func (d *fakeDriver) VerifyConnectivity() error {
	panic("implement me")
}

func (d *fakeDriver) Target() url.URL {
	panic("implement me")
}

func (d *fakeDriver) Session(accessMode neo4j.AccessMode, bookmarks ...string) (neo4j.Session, error) {
	panic("implement me")
}

func (d *fakeDriver) NewSession(config neo4j.SessionConfig) (neo4j.Session, error) {
	return d, nil
}

func (d *fakeDriver) Close() error {
	return nil
}

func (d *fakeDriver) LastBookmark() string {
	panic("implement me")
}

func (d *fakeDriver) BeginTransaction(configurers ...func(*neo4j.TransactionConfig)) (neo4j.Transaction, error) {
	panic("implement me")
}

func (d *fakeDriver) ReadTransaction(work neo4j.TransactionWork, configurers ...func(*neo4j.TransactionConfig)) (interface{}, error) {
	panic("implement me")
}

func (d *fakeDriver) WriteTransaction(work neo4j.TransactionWork, configurers ...func(*neo4j.TransactionConfig)) (interface{}, error) {
	if d.r.Float64() <= d.failureRate {
		return nil, fmt.Errorf("induced error from test harness")
	}

	latency, err := ExponentialRand(d.r, d.minLatency.Milliseconds(), d.maxLatency.Milliseconds(), 0.5)
	if err != nil {
		panic(err)
	}
	d.clock.sleep(time.Duration(latency) * time.Millisecond)
	return nil, nil
}

func (d *fakeDriver) Run(cypher string, params map[string]interface{}, configurers ...func(*neo4j.TransactionConfig)) (neo4j.Result, error) {
	panic("implement me")
}

var _ neo4j.Driver = &fakeDriver{}

var _ neo4j.Session = &fakeDriver{}
