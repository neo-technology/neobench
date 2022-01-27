package neobench

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math"
	"math/rand"
	"testing"
	"time"
)

// Temporary, can remove by 1.0, just to help people along
func TestBackslashToColonBreakingChange(t *testing.T) {
	_, err := Parse("sleep", `\set sleeptime 13
RETURN 1;`, 1)

	assert.Errorf(t, err, "meta-commands now use ':' rather than '\\' as prefix to align with the rest of the Neo4j ecosystem")
}

func TestSleep(t *testing.T) {
	vars := map[string]interface{}{"scale": int64(1)}
	script, err := Parse("sleep", `:set sleeptime 13
:sleep $sleeptime us
RETURN 1;`, 1)

	assert.NoError(t, err)
	uow, err := script.Eval(ScriptContext{
		Vars: vars,
		Rand: rand.New(rand.NewSource(1337)),
	})
	assert.NoError(t, err)
	assert.Equal(t, []Statement{
		{
			Query:  "RETURN 1",
			Params: map[string]interface{}{},
		},
	}, uow.Statements)
}

func TestSleepDuration(t *testing.T) {
	tests := map[string]struct {
		expectSleepDuration time.Duration
		expectError         error
	}{
		":sleep 10": {
			expectSleepDuration: 10 * time.Second,
		},
		":sleep 10 s": {
			expectSleepDuration: 10 * time.Second,
		},
		":sleep 10s": {
			expectSleepDuration: 10 * time.Second,
		},
		":sleep 10 ms": {
			expectSleepDuration: 10 * time.Millisecond,
		},
		":sleep 10 us": {
			expectSleepDuration: 10 * time.Microsecond,
		},
		":sleep 10 days": {
			expectError: fmt.Errorf(":sleep command must use 'us', 'ms', or 's' unit argument - or none. got: days (at testSleep:':sleep 10 days':1:15)"),
		},
	}

	for given, tc := range tests {
		given, tc := given, tc
		t.Run(given, func(t *testing.T) {
			script, err := Parse(fmt.Sprintf("testSleep:'%s'", given), given, 1)

			if tc.expectError != nil {
				assert.Equal(t, tc.expectError, err)
				return
			}

			assert.NoError(t, err)
			cmd := script.Commands[0].(SleepCommand)
			actualDurationBase, err := cmd.Duration.Eval(nil)
			assert.Equal(t, tc.expectSleepDuration, time.Duration(actualDurationBase.(int64))*cmd.Unit)
		})
	}
}

func TestExpressions(t *testing.T) {
	tc := map[string]interface{}{
		// Scalars
		"0":                     int64(0),
		"-0":                    int64(-0),
		"1":                     int64(1),
		"9999999000":            int64(9999999000),
		"-9999999000":           int64(-9999999000),
		"\"Hello\"":             "Hello",
		"\"Hello\" + 123":       "Hello123",
		"123 + \"Hello\" + 123": "123Hello123",

		// Composites
		"[1, 2, [3]]":    []interface{}{int64(1), int64(2), []interface{}{int64(3)}},
		"[\"a\", \"b\"]": []interface{}{"a", "b"},
		"{}":             map[string]interface{}{},
		"{ key: 1 }": map[string]interface{}{
			"key": int64(1),
		},
		"{ key: 1, nest: [ 1 ] }": map[string]interface{}{
			"key":  int64(1),
			"nest": []interface{}{int64(1)},
		},

		// Single-operator arithmetic
		"1 * 2":     int64(2),
		"1 * 2 * 4": int64(8),
		"-1 * 1337": int64(-1337),
		"7 % 8":     int64(7),
		"6 % 6":     int64(0),

		"2 / 2":      float64(1),
		"16 / 2 / 2": float64(4),

		"1 + 2":     int64(3),
		"1 + 2 + 4": int64(7),
		"-1 + 1337": int64(1336),

		"1 - 2":     int64(-1),
		"1 - 2 - 4": int64(-5),
		"-1 - 1337": int64(-1338),

		// Mixed operator precedence
		"1 * 2 + 1":     int64(3),
		"1 + 1 * 2":     int64(3),
		"2 * 2 / 4":     float64(1),
		"2 / 2 * 4":     float64(4),
		"2 - 1 * 2 + 1": int64(1),

		// Parantheticals
		"1 * (2 + 1)":     int64(3),
		"(1 * (2 + 1))":   int64(3),
		"(1 * (2 + (1)))": int64(3),

		// Indexing
		"[1,2][0]":             int64(1),
		"[1,2][1]":             int64(2),
		"range(1, 5)[abs(-1)]": int64(2),

		// List comprehension
		"[ i in range(1,3) | $i ]": []interface{}{int64(1), int64(2), int64(3)},

		// Functions
		"abs(-17)":   int64(17),
		"abs(-17.6)": 17.6,
		"csv(\"/data.csv\")": []interface{}{
			[]interface{}{"row1", int64(1), 1.3},
			[]interface{}{"row2", int64(2), 1.0}},
		"double(5432)":                   float64(5432),
		"double(5432.0)":                 float64(5432),
		"greatest(5, 4, 3, 2)":           int64(5),
		"greatest(-5, -4, -3, -2)":       int64(-2),
		"greatest(5, 4, 3, 2.0, 8)":      float64(8),
		"least(5, 4, 3, 2)":              int64(2),
		"least(5, 4, 3, 2.0, 8)":         2.0,
		"least(-5, -4, -3, -2)":          int64(-5),
		"len([1,2,3])":                   int64(3),
		"len([])":                        int64(0),
		"int(5.4 + 3.8)":                 int64(9),
		"int(5 + 4)":                     int64(9),
		"pi()":                           math.Pi,
		"random(1, 5)":                   int64(3),
		"random_gaussian(1, 10, 2.5)":    int64(3),
		"random_exponential(1, 10, 2.5)": int64(4),
		"range(1, 5)":                    []interface{}{int64(1), int64(2), int64(3), int64(4), int64(5)},
		"random_matrix(2, [1,5], [5,8])": []interface{}{
			[]interface{}{int64(3), int64(5)},
			[]interface{}{int64(1), int64(5)}},
		"sqrt(2.0)": 1.414213562,
	}

	for expr, expected := range tc {
		expr, expected := expr, expected
		t.Run(expr, func(t *testing.T) {
			vars := map[string]interface{}{"scale": int64(1), "somelist": []interface{}{int64(1), int64(2)}}
			script, err := Parse(fmt.Sprintf("expr:'%s'", expr), fmt.Sprintf(`:set v %s
RETURN {v};`, expr), 1)

			assert.NoError(t, err)
			if err != nil {
				return
			}
			uow, err := script.Eval(ScriptContext{
				Vars: vars,
				Rand: rand.New(rand.NewSource(1337)),
				CsvLoader: fakeCsvLoader(map[string]string{
					"/data.csv": `row1, 1, 1.3
"row2", 2, 1.0`,
				}),
			})
			assert.NoError(t, err, "%+v", err)
			actual := uow.Statements[0].Params["v"]
			if expectedFloat, ok := expected.(float64); ok {
				assert.InDelta(t, expectedFloat, actual, 0.00001)
			} else {
				assert.Equal(t, expected, actual)
			}
		})
	}
}

func TestDebugFunction(t *testing.T) {
	vars := map[string]interface{}{"scale": int64(1)}
	script, err := Parse("test:debug(..)", ":set blah debug(1337) * 10\nRETURN { blah };", 1)

	assert.NoError(t, err)
	if err != nil {
		return
	}

	stderr := bytes.NewBuffer(nil)
	uow, err := script.Eval(ScriptContext{
		Stderr: stderr,
		Vars:   vars,
		Rand:   rand.New(rand.NewSource(1337)),
	})
	assert.NoError(t, err)
	assert.Equal(t, int64(13370), uow.Statements[0].Params["blah"])
	assert.Equal(t, "1337\n", stderr.String())
}

func TestComment(t *testing.T) {
	vars := map[string]interface{}{"scale": int64(1)}
	script, err := Parse("sleep", `
// This is a comment on the set metacommand
:set sleeptime 13 // this is a comment at end-of-line in a metacommand

// This is a comment on a query
RETURN {sleeptime};`, 1)

	assert.NoError(t, err)
	uow, err := script.Eval(ScriptContext{
		Vars: vars,
		Rand: rand.New(rand.NewSource(1337)),
	})
	assert.NoError(t, err)
	assert.Equal(t, []Statement{
		{
			Query:  "RETURN {sleeptime}",
			Params: map[string]interface{}{"sleeptime": int64(13)},
		},
	}, uow.Statements)
}

// This allows script authors to bring large datasets into scope, like to randomly pick a value
// from a big set, but then not have that big set be sent off to the database.
func TestExcludesUnusedParams(t *testing.T) {
	vars := map[string]interface{}{"scale": int64(1)}
	script, err := Parse("sleep", `
:set notSent 13
:set sent $notSent + 10
:set alsoSent $notSent + 1
:set quotedSent $notSent + 2

RETURN {sent} + $alsoSent`+" + {`quotedSent`};", 1)

	assert.NoError(t, err)
	uow, err := script.Eval(ScriptContext{
		Vars: vars,
		Rand: rand.New(rand.NewSource(1337)),
	})
	assert.NoError(t, err)
	assert.Equal(t, []Statement{
		{
			Query:  "RETURN {sent} + $alsoSent + {`quotedSent`}",
			Params: map[string]interface{}{"sent": int64(23), "alsoSent": int64(14), "quotedSent": int64(15)},
		},
	}, uow.Statements)
}

// This allows emulating large volumes of distinct query strings, using raw template substitution
func TestClientSideParams(t *testing.T) {
	vars := map[string]interface{}{"scale": int64(1)}
	script, err := Parse("sleep", `
:set clientSide 7331
:set serverSide 1337
:set clientSideList [ i in range(1,2) | "hello" + $i ]

RETURN $serverSide + {serverSide} + $$clientSide, $$clientSideList`, 1)

	assert.NoError(t, err)
	uow, err := script.Eval(ScriptContext{
		Vars: vars,
		Rand: rand.New(rand.NewSource(1337)),
	})
	assert.NoError(t, err)
	assert.Equal(t, []Statement{
		{
			Query:  "RETURN $serverSide + {serverSide} + 7331, [\"hello1\", \"hello2\"]",
			Params: map[string]interface{}{"serverSide": int64(1337)},
		},
	}, uow.Statements)
}

// Partially a regression test for a parser bug in list comprehensions, but covers multi-statement scripts
func TestMultiQuery(t *testing.T) {
	vars := map[string]interface{}{"scale": int64(1), "ids": []interface{}{1}}
	script, err := Parse("sleep", `
:set comp [ i in range(1, 10) | {i: $i, id: $ids[random(0, len($ids))]} ]
:set date "2021-01-27"

MATCH (a);
MATCH (b);`, 1)

	assert.NoError(t, err)
	uow, err := script.Eval(ScriptContext{
		Vars: vars,
		Rand: rand.New(rand.NewSource(1337)),
	})
	assert.NoError(t, err)
	assert.Equal(t, []Statement{
		{
			Query:  "MATCH (a)",
			Params: map[string]interface{}{},
		},
		{
			Query:  "MATCH (b)",
			Params: map[string]interface{}{},
		},
	}, uow.Statements)
}
