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
	assert.Equal(t, []neobench.Statement{
		{
			Query:  "MATCH (account:Account {aid:$aid}) \nSET account.balance = account.balance + $delta",
			Params: map[string]interface{}{"aid": int64(90704), "delta": int64(-3348)},
		},
		{
			Query:  "MATCH (account:Account {aid:$aid}) RETURN account.balance",
			Params: map[string]interface{}{"aid": int64(90704)},
		},
		{
			Query:  "MATCH (teller:Tellers {tid: $tid}) SET teller.balance = teller.balance + $delta",
			Params: map[string]interface{}{"delta": int64(-3348), "tid": int64(1)},
		},
		{
			Query:  "MATCH (branch:Branch {bid: $bid}) SET branch.balance = branch.balance + $delta",
			Params: map[string]interface{}{"bid": int64(1), "delta": int64(-3348)},
		},
		{
			Query:  "CREATE (:History { tid: $tid, bid: $bid, aid: $aid, delta: $delta, mtime: timestamp() })",
			Params: map[string]interface{}{"aid": int64(90704), "bid": int64(1), "delta": int64(-3348), "tid": int64(1)},
		},
	}, uow.Statements)
}
