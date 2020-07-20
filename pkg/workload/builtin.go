package workload

import (
	"github.com/neo4j/neo4j-go-driver/neo4j"
	"go.uber.org/zap"
)

const TPCBLike = `
\set aid random(1, 100000 * :scale)
\set bid random(1, 1 * :scale)
\set tid random(1, 10 * :scale)
\set delta random(-5000, 5000)

MATCH (account:Account {aid:$aid}) 
SET account.balance = account.balance + $delta;

MATCH (account:Account {aid:$aid}) RETURN account.balance;
MATCH (teller:Tellers {tid: $tid}) SET teller.balance = teller.balance + $delta;
MATCH (branch:Branch {bid: $bid}) SET branch.balance = branch.balance + $delta;
CREATE (:History { tid: $tid, bid: $bid, aid: $aid, delta: $delta, mtime: timestamp() });
`

func InitTPCBLike(scale int64, driver neo4j.Driver, logger *zap.SugaredLogger) error {
	numBranches := 1 * scale
	numTellers :=  10 * scale
	numAccounts := 100000 * scale
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

	logger.Infof("Ensuring %d branches..", numBranches)
	_, err = session.Run(`UNWIND range(1, $nBranches) AS branchId 
MERGE (:Branch {bid: branchId, balance: 0})
`, map[string]interface{}{
		"nBranches": numBranches,
	})
	if err != nil {
		return err
	}

	logger.Infof("Ensuring %d tellers..", numTellers)
	_, err = session.Run(`UNWIND range(1, $nTellers) AS tellerId 
MERGE (t:Teller {tid: tellerId, balance: 0})
`, map[string]interface{} {
		"nTellers": numTellers,
	})
	if err != nil {
		return err
	}

	logger.Infof("Ensuring %d accounts..", numAccounts)
	result, err := session.Run("MATCH (:Account) RETURN COUNT(*) AS n", nil)
	if err != nil {
		return err
	}
	result.Next()
	existingAccountNum := result.Record().GetByIndex(0).(int64)

	batchSize := int64(5000)
	numBatches := numAccounts / batchSize
	for batchNo := int64(0); batchNo <= numBatches; batchNo++ {
		startAccount := max(existingAccountNum, batchSize * batchNo + 1)
		endAccount := min(numAccounts, startAccount + batchSize)
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
