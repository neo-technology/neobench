package builtin

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"neobench/pkg/neobench"
	"testing"
)

func TestParseTpcBLike(t *testing.T) {
	vars := map[string]interface{}{"scale": int64(1)}
	script, err := neobench.Parse("builtin:tpcb-like", TPCBLike, 1)

	assert.NoError(t, err)
	uow, err := script.Eval(neobench.ScriptContext{
		Vars: vars,
		Rand: rand.New(rand.NewSource(1337)),
	})
	assert.NoError(t, err)
	if err != nil {
		return
	}
	params := map[string]interface{}{"aid": int64(90704), "bid": int64(1), "delta": int64(-3348), "scale": int64(1), "tid": int64(1)}
	assert.Equal(t, []neobench.Statement{
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
