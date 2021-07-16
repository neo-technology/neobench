package neobench

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"github.com/pkg/errors"
)

// Useful for creating sharded workloads or other logic that tie in session-esque concepts
const WorkerIdVar = "nbWorkerId"

type Workload struct {
	// set on command line and built in
	Variables map[string]interface{}

	Scripts Scripts

	Rand      *rand.Rand
	CsvLoader *CsvLoader
}

// Scripts in a workload, and utilities to draw a weighted random script
type Scripts struct {
	// Scripts sorted by weight
	Scripts []Script
	// Lookup table for choice of scripts; one entry for each script, each entry records the cumulative
	// weight of that script and all scripts before it in the array. See Choose() for details
	WeightedLookup *WeightedRandom
}

func NewScripts(scripts ...Script) Scripts {
	wr := &WeightedRandom{}
	for _, script := range scripts {
		wr.Add(script, int(script.Weight*10000))
	}

	return Scripts{
		Scripts:        scripts,
		WeightedLookup: wr,
	}
}

func (s *Scripts) Choose(r *rand.Rand) Script {
	return s.WeightedLookup.Draw(r).(Script)
}

// List of items that can be randomly drawn from; each item has a weight determining its probability to be drawn
type WeightedRandom struct {
	// See draw(..)
	lookupTable []int
	totalWeight int
	entries     []interface{}
}

func (w *WeightedRandom) Add(entry interface{}, weight int) {
	w.lookupTable = append(w.lookupTable, w.totalWeight+weight)
	w.entries = append(w.entries, entry)
	w.totalWeight += weight
}

func (w *WeightedRandom) Draw(r *rand.Rand) interface{} {
	// How do you take the uniformly random number we get from rand, and convert it into a weighted choice of
	// a script to use?
	//
	// Imagine that we create a segmented number line, each segment representing one script. The length of each
	// segment is the weight of that script. So for three scripts, A@2, B@3, C@3, we create a line like:
	//
	//   0 1 2 3 4 5 6 7 8
	//   [AA][BBBB][CCCC]
	//
	// Then we pick a number between 0 and the max of the number line (eg. 8 since 2+3+3 is 8). Say we get 4:
	//
	//   0 1 2 3 4 5 6 7 8
	//   [AA][BBBB][CCCC]
	//           ^
	//
	// The problem with this is that while it's easy visually to see which "item" we landed on, it's not obvious
	// how to do it quickly on a computer. The solution used here is to maintain a lookup table with the cumulative
	// weight at each segment, one entry per segment:
	//
	//   0 1 2 3 4 5 6 7 8
	//   [AA][BBBB][CCCC]
	//    +2   +3    +3    <-- weight of each segment
	//    2     5     8    <-- lookup table value (eg. cumulation of weights, summing left-to-right)
	//
	// We can then do binary search into the lookup table, the index we get back is the segment our number fell on.

	// 1: Pick a random number between 1 and the combined weight of all scripts
	point := r.Intn(w.totalWeight) + 1

	// 2: Use binary search in the weighted lookup table to find the closest index for this weight
	index := sort.SearchInts(w.lookupTable, point)

	return w.entries[index]
}

type Script struct {
	// Either path to script provided by user, or builtin:<name>
	Name       string
	Readonly   bool
	Weight     float64
	Commands   []Command
	Autocommit bool
}

// Context that scripts are executed in; these are not thread safe, and are re-created on each script
// invocation, so need to be kept lightish.
type ScriptContext struct {
	// Set true to skip sleeps and other things that should not execute during preflights
	PreflightMode bool
	Script        Script
	Stderr        io.Writer
	Vars          map[string]interface{}
	Rand          *rand.Rand
	CsvLoader     *CsvLoader
}

// Evaluate this script in the given context
func (s *Script) Eval(ctx ScriptContext) (UnitOfWork, error) {
	uow := UnitOfWork{
		ScriptName: s.Name,
		Readonly:   s.Readonly,
		Autocommit: s.Autocommit,
		Statements: nil,
	}

	for _, cmd := range s.Commands {
		if err := cmd.Execute(&ctx, &uow); err != nil {
			return uow, err
		}
	}

	return uow, nil
}

func (s *Workload) NewClient() ClientWorkload {
	return ClientWorkload{
		Variables: s.Variables,
		Scripts:   s.Scripts,
		Rand:      rand.New(rand.NewSource(s.Rand.Int63())),
		Stderr:    os.Stderr,
		CsvLoader: s.CsvLoader,
	}
}

type ClientWorkload struct {
	Readonly bool
	// variables set on command line and built-in
	Variables map[string]interface{}
	Scripts   Scripts
	Rand      *rand.Rand
	Stderr    io.Writer
	CsvLoader *CsvLoader
}

func (s *ClientWorkload) Next(workerId int64) (UnitOfWork, error) {
	script := s.Scripts.Choose(s.Rand)
	return script.Eval(ScriptContext{
		Script:    script,
		Stderr:    s.Stderr,
		Vars:      createVars(s.Variables, workerId),
		Rand:      s.Rand,
		CsvLoader: s.CsvLoader,
	})
}

type UnitOfWork struct {
	// Path to user-provided script, or builtin:<name>
	ScriptName string
	Readonly   bool
	Statements []Statement
	Autocommit bool
}

type Statement struct {
	Query  string
	Params map[string]interface{}
}

type Command interface {
	Execute(ctx *ScriptContext, uow *UnitOfWork) error
}

type QueryCommand struct {
	Query string
	// Parameters used in the above query
	RemoteParams []string
	// Locally substituted parameters
	LocalParams []string
}

func (c QueryCommand) Execute(ctx *ScriptContext, uow *UnitOfWork) error {
	params := make(map[string]interface{})
	for _, pname := range c.RemoteParams {
		params[pname] = ctx.Vars[pname]
	}
	query := c.Query
	if len(c.LocalParams) > 0 {
		for _, pname := range c.LocalParams {
			literal, err := varToCypherLiteral(ctx.Vars[pname])
			if err != nil {
				return errors.Wrapf(err, "don't yet know how to convert $$%s (%v) to a cypher literal string", pname, ctx.Vars[pname])
			}
			query = strings.ReplaceAll(query, fmt.Sprintf("$$%s", pname), literal)
		}
	}
	uow.Statements = append(uow.Statements, Statement{
		Query:  query,
		Params: params,
	})
	return nil
}

func varToCypherLiteral(v interface{}) (string, error) {
	switch v := v.(type) {
	case int, int32, int64:
		return fmt.Sprintf("%d", v), nil
	case float32, float64:
		return fmt.Sprintf("%f", v), nil
	case bool:
		if v {
			return "true", nil
		} else {
			return "false", nil
		}
	case string:
		return fmt.Sprintf("\"%s\"", v), nil // TODO escaping
	case []interface{}:
		var sb strings.Builder
		sb.WriteString("[")
		for i, e := range v {
			eLit, err := varToCypherLiteral(e)
			if err != nil {
				return "", err
			}
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(eLit)
		}
		sb.WriteString("]")
		return sb.String(), nil
	default:
		return "", fmt.Errorf("don't know how to convert %v to cypher literal", v)
	}
}

type SetCommand struct {
	VarName    string
	Expression Expression
}

func (c SetCommand) Execute(ctx *ScriptContext, uow *UnitOfWork) error {
	value, err := c.Expression.Eval(ctx)
	if err != nil {
		return err
	}
	ctx.Vars[c.VarName] = value
	return nil
}

type SleepCommand struct {
	Duration Expression
	Unit     time.Duration
}

func (c SleepCommand) Execute(ctx *ScriptContext, uow *UnitOfWork) error {
	sleepNumber, err := c.Duration.Eval(ctx)
	if err != nil {
		return err
	}
	sleepInt, ok := sleepNumber.(int64)
	if !ok {
		return fmt.Errorf("\\sleep must be given an integer expression, got %v", sleepNumber)
	}

	if ctx.PreflightMode {
		return nil
	}

	time.Sleep(time.Duration(sleepInt) * c.Unit)
	return nil
}

// Validates that a workload doesn't have syntax errors etc, and tells us if it is read-only
func WorkloadPreflight(driver neo4j.Driver, dbName string, script Script, vars map[string]interface{},
	csvLoader *CsvLoader) (readonly bool, err error) {
	session := driver.NewSession(neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: dbName,
	})
	defer session.Close()

	r := rand.New(rand.NewSource(1337))

	unitOfWork, err := script.Eval(ScriptContext{
		PreflightMode: true,
		Script:        script,
		Stderr:        os.Stderr,
		Vars:          createVars(vars, 0),
		Rand:          r,
		CsvLoader:     csvLoader,
	})
	if err != nil {
		return false, err
	}
	readonlyRaw, err := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		readonly := true
		for _, stmt := range unitOfWork.Statements {
			res, err := tx.Run(fmt.Sprintf("EXPLAIN %s", stmt.Query), stmt.Params)
			if err != nil {
				return false, err
			}
			summary, err := res.Consume()
			if err != nil {
				return false, err
			}
			readonly = summary.StatementType() == neo4j.StatementTypeReadOnly && readonly
		}

		return readonly, nil
	})
	if err != nil {
		return false, errors.Wrapf(err, "script '%s' failed preflight checks", script.Name)
	}
	readonly = readonlyRaw.(bool)
	return
}

func createVars(globalVars map[string]interface{}, workerId int64) map[string]interface{} {
	vars := make(map[string]interface{})
	vars[WorkerIdVar] = workerId
	for k, v := range globalVars {
		vars[k] = v
	}
	return vars
}
