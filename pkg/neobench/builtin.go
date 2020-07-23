package neobench

import (
	"github.com/neo4j/neo4j-go-driver/neo4j"
)

const TPCBLike = `
\set aid random(1, 100000 * $scale)
\set bid random(1, 1 * $scale)
\set tid random(1, 10 * $scale)
\set delta random(-5000, 5000)

MATCH (account:Account {aid:$aid}) 
SET account.balance = account.balance + $delta;

MATCH (account:Account {aid:$aid}) RETURN account.balance;
MATCH (teller:Tellers {tid: $tid}) SET teller.balance = teller.balance + $delta;
MATCH (branch:Branch {bid: $bid}) SET branch.balance = branch.balance + $delta;
CREATE (:History { tid: $tid, bid: $bid, aid: $aid, delta: $delta, mtime: timestamp() });
`

func InitTPCBLike(scale int64, driver neo4j.Driver, out Output) error {
	numBranches := 1 * scale
	numTellers := 10 * scale
	numAccounts := 100000 * scale
	session, err := driver.Session(neo4j.AccessModeWrite)
	if err != nil {
		return err
	}
	defer session.Close()

	out.ReportProgress(ProgressReport{
		Section:      "init",
		Step:         "create schema",
		Completeness: 0,
	})
	_, err = session.Run(`CREATE CONSTRAINT ON (b:Branch) ASSERT b.bid IS UNIQUE
CREATE CONSTRAINT ON (t:Teller) ASSERT t.tid IS UNIQUE
CREATE CONSTRAINT ON (a:Account) ASSERT a.aid IS UNIQUE
`, map[string]interface{}{})
	if err != nil {
		return err
	}

	out.ReportProgress(ProgressReport{
		Section:      "init",
		Step:         "create branches & tellers",
		Completeness: 0,
	})
	_, err = session.Run(`UNWIND range(1, $nBranches) AS branchId 
MERGE (:Branch {bid: branchId, balance: 0})
`, map[string]interface{}{
		"nBranches": numBranches,
	})
	if err != nil {
		return err
	}

	_, err = session.Run(`UNWIND range(1, $nTellers) AS tellerId 
MERGE (t:Teller {tid: tellerId, balance: 0})
`, map[string]interface{}{
		"nTellers": numTellers,
	})
	if err != nil {
		return err
	}

	out.ReportProgress(ProgressReport{
		Section:      "init",
		Step:         "create accounts",
		Completeness: 0,
	})
	result, err := session.Run("MATCH (:Account) RETURN COUNT(*) AS n", nil)
	if err != nil {
		return err
	}
	result.Next()
	existingAccountNum := result.Record().GetByIndex(0).(int64)

	batchSize := int64(5000)
	numBatches := numAccounts / batchSize
	for batchNo := int64(0); batchNo <= numBatches; batchNo++ {
		startAccount := max(existingAccountNum, batchSize*batchNo+1)
		endAccount := min(numAccounts, startAccount+batchSize)
		if endAccount <= startAccount {
			continue
		}
		_, err = session.Run(`UNWIND range($startAccount, $endAccount) AS accountId 
CREATE (a:Account {aid: accountId, balance: 0})
`, map[string]interface{}{
			"startAccount": startAccount,
			"endAccount":   endAccount,
		})
		if err != nil {
			return err
		}
		out.ReportProgress(ProgressReport{
			Section:      "init",
			Step:         "create accounts",
			Completeness: float64(batchNo) / float64(numBatches),
		})
	}
	return nil
}
