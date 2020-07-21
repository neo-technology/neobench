package neobench

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParser(t *testing.T) {
	wrk, err := Parse("builtin:tpcb-like", TPCBLike, 1, 1337)

	assert.NoError(t, err)
	clientWork := wrk.NewClient()
	uow, err := clientWork.Next()
	assert.NoError(t, err)
	params := map[string]interface{}{"aid": int64(96828), "bid": int64(1), "delta": int64(4583), "scale": int64(1), "tid": int64(1)}
	assert.Equal(t, []Statement{
		{
			Query:  "MATCH (account:Account {aid:$aid}) \nSET account.balance = account.balance + $delta",
			Params: params,
		},
		{
			Query:  "MATCH (account:Account {aid:$aid}) RETURN account.balance",
			Params: params,
		},
		{
			Query:  "MATCH (teller:Tellers {tid: $tid}) SET teller.balance = teller.balance + $delta",
			Params: params,
		},
		{
			Query:  "MATCH (branch:Branch {bid: $bid}) SET branch.balance = branch.balance + $delta",
			Params: params,
		},
		{
			Query:  "CREATE (:History { tid: $tid, bid: $bid, aid: $aid, delta: $delta, mtime: timestamp() })",
			Params: params,
		},
	}, uow.Statements)
}
