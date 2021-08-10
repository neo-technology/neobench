package builtin

import (
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"neobench/pkg/neobench"
)

const TPCBLike = `
:set aid random(1, 100000 * $scale)
:set bid random(1, 1 * $scale)
:set tid random(1, 10 * $scale)
:set delta random(-5000, 5000)

MATCH (account:Account {aid:$aid}) 
SET account.balance = account.balance + $delta;

MATCH (account:Account {aid:$aid}) RETURN account.balance;
MATCH (teller:Tellers {tid: $tid}) SET teller.balance = teller.balance + $delta;
MATCH (branch:Branch {bid: $bid}) SET branch.balance = branch.balance + $delta;
CREATE (:History { tid: $tid, bid: $bid, aid: $aid, delta: $delta, mtime: timestamp() });
`

const MatchOnly = `
:set aid random(1, 100000 * $scale)
MATCH (account:Account {aid:$aid}) RETURN account.balance;
`

func InitTPCBLike(scale int64, dbName string, driver neo4j.Driver, out neobench.Output) error {
	numBranches := 1 * scale
	numTellers := 10 * scale
	numAccounts := 100000 * scale
	session := driver.NewSession(neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: dbName,
	})
	defer session.Close()

	out.ReportProgress(neobench.ProgressReport{
		Section:      "init",
		Step:         "create schema",
		Completeness: 0,
	})

	var err = ensureSchema(session, []schemaEntry{
		{Label: "Branch", Property: "bid", Unique: true},
		{Label: "Teller", Property: "tid", Unique: true},
		{Label: "Account", Property: "aid", Unique: true},
	})
	if err != nil {
		return err
	}

	out.ReportProgress(neobench.ProgressReport{
		Section:      "init",
		Step:         "create branches & tellers",
		Completeness: 0,
	})
	err = runQ(session, `UNWIND range(1, $nBranches) AS branchId 
MERGE (b:Branch {bid: branchId}) SET b.balance = 0
`, map[string]interface{}{
		"nBranches": numBranches,
	})
	if err != nil {
		return err
	}

	err = runQ(session, `UNWIND range(1, $nTellers) AS tellerId 
MERGE (t:Teller {tid: tellerId}) SET t.balance = 0
`, map[string]interface{}{
		"nTellers": numTellers,
	})
	if err != nil {
		return err
	}

	out.ReportProgress(neobench.ProgressReport{
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
		startAccount := max(existingAccountNum, batchSize*batchNo) + 1
		endAccount := min(numAccounts, startAccount+batchSize) - 1
		if endAccount <= startAccount {
			continue
		}
		err = runQ(session, `UNWIND range($startAccount, $endAccount) AS accountId 
CREATE (a:Account {aid: accountId, balance: 0})
`, map[string]interface{}{
			"startAccount": startAccount,
			"endAccount":   endAccount,
		})
		if err != nil {
			return err
		}
		out.ReportProgress(neobench.ProgressReport{
			Section:      "init",
			Step:         "create accounts",
			Completeness: float64(batchNo) / float64(numBatches),
		})
	}
	return nil
}
