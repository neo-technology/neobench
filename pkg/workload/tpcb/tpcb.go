package tpcb

import (
	"github.com/neo4j/neo4j-go-driver/neo4j"
	"go.uber.org/zap"
	"math/rand"
	"neobench/pkg/workload"
)

type TpcBWorkload struct {
	NumAccounts int
	NumTellers int
	NumBranches int
	Rand *rand.Rand
}

func (t *TpcBWorkload) Initialize(driver neo4j.Driver, logger *zap.SugaredLogger) error {
	session, err := driver.Session(neo4j.AccessModeWrite)
	if err != nil {
		return err
	}
	defer session.Close()

	logger.Infof("Creating indexes..")
	_, err = session.Run(`CREATE CONSTRAINT ON (b:Branch) ASSERT b.bid IS UNIQUE
CREATE CONSTRAINT ON (t:Teller) ASSERT t.tid IS UNIQUE
CREATE CONSTRAINT ON (a:Account) ASSERT a.aid IS UNIQUE
`, map[string]interface{}{})
	if err != nil {
		return err
	}

	logger.Infof("Ensuring %d branches..", t.NumBranches)
	_, err = session.Run(`UNWIND range(1, $nBranches) AS branchId 
MERGE (:Branch {bid: branchId, balance: 0})
`, map[string]interface{}{
		"nBranches": t.NumBranches,
	})
	if err != nil {
		return err
	}

	logger.Infof("Ensuring %d tellers..", t.NumTellers)
	_, err = session.Run(`UNWIND range(1, $nTellers) AS tellerId 
MERGE (t:Teller {tid: tellerId, balance: 0})
`, map[string]interface{} {
		"nTellers": t.NumTellers,
	})
	if err != nil {
		return err
	}

	logger.Infof("Ensuring %d accounts..", t.NumAccounts)
	result, err := session.Run("MATCH (:Account) RETURN COUNT(*) AS n", nil)
	if err != nil {
		return err
	}
	result.Next()
	existingAccountNum := int(result.Record().GetByIndex(0).(int64))

	batchSize := 5000
	numBatches := t.NumAccounts / batchSize
	for batchNo := 0; batchNo <= numBatches; batchNo++ {
		startAccount := max(existingAccountNum, batchSize * batchNo + 1)
		endAccount := min(t.NumAccounts, startAccount + batchSize)
		if endAccount <= startAccount {
			continue
		}
		logger.Infof("  Batch %d/%d (%d -> %d]..", batchNo, numBatches, startAccount, endAccount)
		_, err = session.Run(`UNWIND range($startAccount, $endAccount) AS accountId 
CREATE (a:Account {aid: accountId, balance: 0})
`, map[string]interface{}{
			"startAccount": startAccount,
			"endAccount": endAccount,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *TpcBWorkload) Next() workload.UnitOfWork {
	aid := 1 + t.Rand.Intn(t.NumAccounts)
	tid := 1 + t.Rand.Intn(t.NumTellers)
	bid := 1 + t.Rand.Intn(t.NumBranches)
	delta := -5000 + t.Rand.Intn(10000)

	work := workload.UnitOfWork{
		Statements: []workload.Statement{
			{
				Query: "MATCH (account:Account {aid:$aid}) SET account.balance = account.balance + $delta",
				Params: map[string]interface{}{
					"aid": aid,
					"delta": delta,
				},
			},
			{
				Query: "MATCH (account:Account {aid:$aid}) RETURN account.balance",
				Params: map[string]interface{}{
					"aid": aid,
				},
			},
			{
				Query: "MATCH (teller:Tellers {tid: $tid}) SET teller.balance = teller.balance + $delta",
				Params: map[string]interface{}{
					"tid": tid,
					"delta": delta,
				},
			},
			{
				Query: "MATCH (branch:Branch {bid: $bid}) SET branch.balance = branch.balance + $delta",
				Params: map[string]interface{}{
					"bid": bid,
					"delta": delta,
				},
			},
			{
				Query: "CREATE (:History { tid: $tid, bid: $bid, aid: $aid, delta: $delta, mtime: timestamp() });",
				Params: map[string]interface{}{
					"bid": bid,
					"tid": tid,
					"aid": aid,
					"delta": delta,
				},
			},
		},
	}
	return work
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func NewTpcB(scale int) *TpcBWorkload {
	return &TpcBWorkload{
		NumBranches: 1 * scale,
		NumTellers:  10 * scale,
		NumAccounts: 100000 * scale,
		Rand: rand.New(rand.NewSource(1337)),
	}
}