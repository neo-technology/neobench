package builtin

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"neobench/pkg/neobench"
	"testing"
)

func TestParseIC2(t *testing.T) {
	vars := map[string]interface{}{"scale": int64(1)}
	script, err := neobench.Parse("LDBCIC2", LDBCIC2, 1)

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
			Query: `MATCH (:Person {id: $personId})-[:KNOWS]-(friend),
      (friend)<-[:HAS_CREATOR]-(message)
WHERE message.creationDate <= date({year: 2010, month:10, day:10})
RETURN friend.id AS personId,
       friend.firstName AS personFirstName,
       friend.lastName AS personLastName,
       message.id AS messageId,
       coalesce(message.content, message.imageFile) AS messageContent,
       message.creationDate AS messageDate
ORDER BY messageDate DESC, messageId ASC
LIMIT 20
`,
			Params: map[string]interface{}{"personId": int64(6023)},
		},
	}, uow.Statements)
}
