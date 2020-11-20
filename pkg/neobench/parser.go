package neobench

import (
	"fmt"
	"github.com/pkg/errors"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"text/scanner"
	"time"
)

func Parse(filename, script string, weight float64) (Script, error) {
	var s scanner.Scanner
	s.Init(strings.NewReader(script))
	s.Filename = filename
	s.Whitespace ^= 1 << '\n' // don't skip newlines

	c := &context{
		s: s,
	}

	commands := make([]Command, 0)

	for !c.done {
		tok := c.Peek()
		if tok == scanner.EOF {
			break
		} else if tok == '\\' {
			commands = append(commands, metaCommand(c))
		} else if tok == '\n' {
			c.Next()
		} else {
			commands = append(commands, command(c))
		}
	}

	if c.err != nil {
		return Script{}, c.err
	}

	return Script{
		Name:     filename,
		Readonly: false, // TODO
		Commands: commands,
		Weight:   weight,
	}, nil
}

func metaCommand(c *context) Command {
	expect(c, '\\')
	cmd := ident(c)

	switch cmd {
	case "set":
		varName := ident(c)
		setExpr := expr(c)
		return SetCommand{
			VarName:    varName,
			Expression: setExpr,
		}
	case "sleep":
		durationBase := expr(c)
		unit := time.Second
		switch c.Peek() {
		case '\n', scanner.EOF:
			break
		default:
			_, unitStr := c.Next()
			switch unitStr {
			case "s":
				unit = time.Second
			case "ms":
				unit = time.Millisecond
			case "us":
				unit = time.Microsecond
			default:
				c.fail(fmt.Errorf("\\sleep command must use 'us', 'ms', or 's' unit argument - or none. got: %s", c.peekText))
				return nil
			}
		}
		return SleepCommand{
			Duration: durationBase,
			Unit:     unit,
		}
	default:
		c.fail(fmt.Errorf("unexpected meta command: '%s'", cmd))
		return nil
	}
}

func command(c *context) Command {
	originalWhitespace := c.s.Whitespace
	defer func() {
		c.s.Whitespace = originalWhitespace
	}()
	c.s.Whitespace = 0
	var b strings.Builder
	for tok, content := c.Next(); tok != ';' && tok != scanner.EOF; tok, content = c.Next() {
		b.WriteString(content)
	}
	return QueryCommand{
		Query: b.String(),
	}
}

func ident(c *context) string {
	tok, content := c.Next()
	if tok != scanner.Ident {
		c.fail(fmt.Errorf("expected identifier, got '%s'", scanner.TokenString(tok)))
	}
	return content
}

func expr(c *context) Expression {
	lhs := term(c)
	for {
		tok := c.Peek()
		if tok == '+' {
			c.Next()
			rhs := term(c)
			lhs = Expression{
				Kind: callExpr,
				Payload: CallExpr{
					name: "+",
					args: []Expression{lhs, rhs},
				},
			}
		} else if tok == '-' {
			c.Next()
			rhs := term(c)
			lhs = Expression{
				Kind: callExpr,
				Payload: CallExpr{
					name: "-",
					args: []Expression{lhs, rhs},
				},
			}
		} else {
			return lhs
		}
	}
}

func term(c *context) Expression {
	lhs := factor(c)
	for {
		tok := c.Peek()
		if tok == '*' {
			c.Next()
			rhs := factor(c)
			lhs = Expression{
				Kind: callExpr,
				Payload: CallExpr{
					name: "*",
					args: []Expression{lhs, rhs},
				},
			}
		} else if tok == '/' {
			c.Next()
			rhs := factor(c)
			lhs = Expression{
				Kind: callExpr,
				Payload: CallExpr{
					name: "/",
					args: []Expression{lhs, rhs},
				},
			}
		} else if tok == '%' {
			c.Next()
			rhs := factor(c)
			lhs = Expression{
				Kind: callExpr,
				Payload: CallExpr{
					name: "%",
					args: []Expression{lhs, rhs},
				},
			}
		} else {
			return lhs
		}
	}
}

func factor(c *context) Expression {
	tok, content := c.Next()
	if tok == scanner.Ident {
		funcName := content
		var args []Expression
		expect(c, '(')
		tok := c.Peek()
		for tok != ')' {
			if len(args) > 0 {
				expect(c, ',')
			}
			args = append(args, expr(c))
			if c.done {
				return Expression{}
			}
			tok = c.Peek()
		}
		c.Next()
		return Expression{Kind: callExpr, Payload: CallExpr{
			name: funcName,
			args: args,
		}}
	} else if tok == scanner.Int {
		intVal, err := strconv.Atoi(content)
		if err != nil {
			c.fail(err)
			return Expression{}
		}
		return Expression{Kind: intExpr, Payload: int64(intVal)}
	} else if tok == scanner.Float {
		floatVal, err := strconv.ParseFloat(content, 64)
		if err != nil {
			c.fail(err)
			return Expression{}
		}
		return Expression{Kind: floatExpr, Payload: floatVal}

	} else if tok == '(' {
		innerExp := expr(c)
		expect(c, ')')
		return innerExp
	} else if tok == '-' {
		tok, content := c.Next()
		if tok == scanner.Int {
			intVal, err := strconv.Atoi(content)
			if err != nil {
				c.fail(err)
				return Expression{}
			}
			return Expression{Kind: intExpr, Payload: int64(-1 * intVal)}
		} else if tok == scanner.Float {
			floatVal, err := strconv.ParseFloat(content, 64)
			if err != nil {
				c.fail(err)
				return Expression{}
			}
			return Expression{Kind: floatExpr, Payload: -1.0 * floatVal}
		} else {
			c.fail(fmt.Errorf("unexpected token, expected integer after minus sign: %s", scanner.TokenString(tok)))
			return Expression{}
		}
	} else if tok == '$' {
		varName := ident(c)
		return Expression{Kind: varExpr, Payload: varName}
	} else if tok == '[' {
		list := make([]Expression, 0)
		tok := c.Peek()
		for tok != ']' {
			if len(list) > 0 {
				expect(c, ',')
			}
			list = append(list, expr(c))
			if c.done {
				return Expression{}
			}
			tok = c.Peek()
		}
		c.Next()
		return Expression{Kind: listExpr, Payload: list}
	} else {
		c.fail(fmt.Errorf("unexpected token, expected Expression: %s", scanner.TokenString(tok)))
		return Expression{}
	}
}

func expect(c *context, expected rune) {
	tok, _ := c.Next()
	if tok != expected {
		c.fail(fmt.Errorf("expected '%s', got '%s'", scanner.TokenString(expected), scanner.TokenString(tok)))
	}
}

type ExprKind uint8

const (
	nullExpr ExprKind = 0
	// payload int64
	intExpr ExprKind = 1
	// payload float64
	floatExpr ExprKind = 2
	// payload []Expression
	listExpr ExprKind = 3
	// payload []CallExpr
	callExpr ExprKind = 4
	// payload string (varname)
	varExpr ExprKind = 5
)

func (e ExprKind) String() string {
	return exprKindNames[e]
}

var exprKindNames = []string{
	nullExpr:  "N/A",
	intExpr:   "int",
	floatExpr: "double",
	listExpr:  "list",
	callExpr:  "call",
	varExpr:   "var",
}

type Expression struct {
	Kind    ExprKind
	Payload interface{}
}

func (e Expression) Eval(ctx *ScriptContext) (interface{}, error) {
	switch e.Kind {
	case intExpr, floatExpr:
		return e.Payload, nil
	case listExpr:
		innerExprs := e.Payload.([]Expression)
		out := make([]interface{}, 0, len(innerExprs))
		for _, innerExpr := range innerExprs {
			exprResult, err := innerExpr.Eval(ctx)
			if err != nil {
				return nil, errors.Wrapf(err, "error when evaluating %s", e)
			}
			out = append(out, exprResult)
		}
		return out, nil
	case varExpr:
		value, found := ctx.Vars[e.Payload.(string)]
		if !found {
			return nil, fmt.Errorf("this variable is not defined: %s", e.Payload.(string))
		}
		return value, nil
	case callExpr:
		return e.Payload.(CallExpr).Eval(ctx)
	default:
		return nil, fmt.Errorf("unknown expression: %s", e.String())
	}
}

func (e Expression) String() string {
	switch e.Kind {
	case intExpr:
		return fmt.Sprintf("%d", e.Payload)
	case floatExpr:
		return fmt.Sprintf("%f", e.Payload)
	case listExpr:
		return fmt.Sprintf("%v", e.Payload)
	case callExpr:
		return e.Payload.(CallExpr).String()
	case varExpr:
		return fmt.Sprintf(":%v", e.Payload)
	default:
		return fmt.Sprintf("err(%v)", e.Payload)
	}
}

type CallExpr struct {
	name string
	args []Expression
}

func (f CallExpr) String() string {
	args := make([]string, 0, len(f.args))
	for _, a := range f.args {
		args = append(args, a.String())
	}
	return fmt.Sprintf("%s(%s)", f.name, strings.Join(args, ", "))
}

func (f CallExpr) argAsNumber(i int, ctx *ScriptContext) (Number, error) {
	if len(f.args) <= i {
		return Number{}, fmt.Errorf("expected at least %d arguments, got %d", i+1, len(f.args))
	}
	value, err := f.args[i].Eval(ctx)
	if err != nil {
		return Number{}, err
	}
	switch value.(type) {
	case int64:
		iVal := value.(int64)
		return Number{isDouble: false, val: float64(iVal), iVal: iVal}, nil
	case float64:
		return Number{isDouble: true, val: value.(float64)}, nil
	default:
		return Number{}, fmt.Errorf("expected int64 or float64, got %s (which is %T)", f.args[i].String(), value)
	}
}

func (f CallExpr) Eval(ctx *ScriptContext) (interface{}, error) {
	switch f.name {
	case "abs":
		a, err := f.argAsNumber(0, ctx)
		if err != nil {
			return nil, fmt.Errorf("in %s: %s", f.String(), err)
		}
		if a.isDouble {
			return math.Abs(a.val), nil
		} else {
			if a.iVal < 0 {
				return -1 * a.iVal, nil
			} else {
				return a.iVal, nil
			}
		}
	case "int":
		a, err := f.argAsNumber(0, ctx)
		if err != nil {
			return nil, fmt.Errorf("in %s: %s", f.String(), err)
		}
		if a.isDouble {
			return int64(a.val), nil
		} else {
			return a.iVal, nil
		}
	case "debug":
		a, err := f.argAsNumber(0, ctx)
		if err != nil {
			return nil, fmt.Errorf("in %s: %s", f.String(), err)
		}
		if a.isDouble {
			_, err := fmt.Fprintf(ctx.Stderr, "%f\n", a.val)
			if err != nil {
				return nil, fmt.Errorf("in %s: %s", f.String(), err)
			}
			return a.val, nil
		} else {
			_, err := fmt.Fprintf(ctx.Stderr, "%d\n", a.iVal)
			if err != nil {
				return nil, fmt.Errorf("in %s: %s", f.String(), err)
			}
			return a.iVal, nil
		}
	case "double":
		a, err := f.argAsNumber(0, ctx)
		if err != nil {
			return nil, fmt.Errorf("in %s: %s", f.String(), err)
		}
		return a.val, nil
	case "greatest":
		if len(f.args) == 0 {
			return nil, fmt.Errorf("greatest(..) requires at least one argument")
		}
		var max Number
		isDouble := false
		for i := range f.args {
			arg, err := f.argAsNumber(i, ctx)
			if err != nil {
				return nil, fmt.Errorf("in %s: %s", f.String(), err)
			}
			isDouble = isDouble || arg.isDouble
			if i == 0 {
				max = arg
				continue
			}

			if isDouble {
				if arg.val > max.val {
					max = arg
				}
			} else {
				if arg.iVal > max.iVal {
					max = arg
				}
			}
		}
		if isDouble {
			return max.val, nil
		}
		return max.iVal, nil
	case "least":
		if len(f.args) == 0 {
			return nil, fmt.Errorf("least(..) requires at least one argument")
		}
		var min Number
		isDouble := false
		for i := range f.args {
			arg, err := f.argAsNumber(i, ctx)
			if err != nil {
				return nil, fmt.Errorf("in %s: %s", f.String(), err)
			}
			isDouble = isDouble || arg.isDouble
			if i == 0 {
				min = arg
				continue
			}
			if isDouble {
				if arg.val < min.val {
					min = arg
				}
			} else {
				if arg.iVal < min.iVal {
					min = arg
				}
			}
		}
		if isDouble {
			return min.val, nil
		}
		return min.iVal, nil
	case "pi":
		return math.Pi, nil
	case "sqrt":
		a, err := f.argAsNumber(0, ctx)
		if err != nil {
			return nil, fmt.Errorf("in %s: %s", f.String(), err)
		}
		return math.Sqrt(a.val), nil
	case "random":
		lb, err := f.argAsNumber(0, ctx)
		if err != nil {
			return nil, fmt.Errorf("in %s: %s", f.String(), err)
		}
		ub, err := f.argAsNumber(1, ctx)
		if err != nil {
			return nil, fmt.Errorf("in %s: %s", f.String(), err)
		}

		if lb.isDouble || ub.isDouble {
			return nil, fmt.Errorf("interval for random() must be integers, not doubles, in %s", f.String())
		}

		if lb.iVal == ub.iVal {
			return lb.iVal, nil
		}

		return uniformRand(ctx.Rand, lb.iVal, ub.iVal), nil
	case "random_exponential":
		lb, err := f.argAsNumber(0, ctx)
		if err != nil {
			return nil, fmt.Errorf("in %s: %s", f.String(), err)
		}
		ub, err := f.argAsNumber(1, ctx)
		if err != nil {
			return nil, fmt.Errorf("in %s: %s", f.String(), err)
		}
		param, err := f.argAsNumber(2, ctx)
		if err != nil {
			return nil, fmt.Errorf("in %s: %s", f.String(), err)
		}

		if lb.isDouble || ub.isDouble {
			return nil, fmt.Errorf("interval for random() must be integers, not doubles, in %s", f.String())
		}

		if lb.iVal == ub.iVal {
			return lb.iVal, nil
		}

		min, max := lb.iVal, ub.iVal
		return ExponentialRand(ctx.Rand, min, max, param.val)
	case "random_gaussian":
		lb, err := f.argAsNumber(0, ctx)
		if err != nil {
			return nil, fmt.Errorf("in %s: %s", f.String(), err)
		}
		ub, err := f.argAsNumber(1, ctx)
		if err != nil {
			return nil, fmt.Errorf("in %s: %s", f.String(), err)
		}
		param, err := f.argAsNumber(2, ctx)
		if err != nil {
			return nil, fmt.Errorf("in %s: %s", f.String(), err)
		}

		if lb.isDouble || ub.isDouble {
			return nil, fmt.Errorf("interval for random() must be integers, not doubles, in %s", f.String())
		}

		if lb.iVal == ub.iVal {
			return lb.iVal, nil
		}

		min, max := lb.iVal, ub.iVal
		return gaussianRand(ctx.Rand, min, max, param.val)
	case "range":
		lb, err := f.argAsNumber(0, ctx)
		if err != nil {
			return nil, fmt.Errorf("in %s: %s", f.String(), err)
		}
		ub, err := f.argAsNumber(1, ctx)
		if err != nil {
			return nil, fmt.Errorf("in %s: %s", f.String(), err)
		}

		if lb.isDouble || ub.isDouble {
			return nil, fmt.Errorf("interval for range() must be integers, not doubles, in %s", f.String())
		}

		min, max := lb.iVal, ub.iVal
		return rangeFn(min, max)
	case "random_matrix":
		numRows, err := f.argAsNumber(0, ctx)
		if err != nil || numRows.isDouble {
			return nil, errors.Wrapf(err, "random_matrix numRows must be integer, in %s", f.String())
		}

		spec := make([][]int64, 0)
		for i := 1; i < len(f.args); i++ {
			rawRowSpec, err := f.args[i].Eval(ctx)
			if err != nil {
				return nil, errors.Wrapf(err, "in %s at %s", f.String(), f.args[i].String())
			}
			rowSpec, ok := rawRowSpec.([]interface{})
			if !ok || len(rowSpec) != 2 {
				return nil, fmt.Errorf("random_matrix column specs should be 2-integer lists specifying the range in that column, like '[1,14]', got %s", f.args[i].String())
			}
			min, minOk := rowSpec[0].(int64)
			max, maxOk := rowSpec[1].(int64)
			if !minOk || !maxOk {
				return nil, fmt.Errorf("random_matrix column random range should be integers, like '[1,14]', got %s", f.args[i].String())
			}
			spec = append(spec, []int64{min, max})
		}
		return randomMatrix(ctx.Rand, numRows.iVal, spec), nil
	case "*":
		a, err := f.argAsNumber(0, ctx)
		if err != nil {
			return nil, fmt.Errorf("in %s: %s", f.String(), err)
		}
		b, err := f.argAsNumber(1, ctx)
		if err != nil {
			return nil, fmt.Errorf("in %s: %s", f.String(), err)
		}

		if a.isDouble || b.isDouble {
			return a.val * b.val, nil
		} else {
			return a.iVal * b.iVal, nil
		}
	case "/":
		a, err := f.argAsNumber(0, ctx)
		if err != nil {
			return nil, fmt.Errorf("in %s: %s", f.String(), err)
		}
		b, err := f.argAsNumber(1, ctx)
		if err != nil {
			return nil, fmt.Errorf("in %s: %s", f.String(), err)
		}

		return a.val / b.val, nil
	case "%":
		a, err := f.argAsNumber(0, ctx)
		if err != nil {
			return nil, fmt.Errorf("in %s: %s", f.String(), err)
		}
		b, err := f.argAsNumber(1, ctx)
		if err != nil {
			return nil, fmt.Errorf("in %s: %s", f.String(), err)
		}

		if a.isDouble {
			return nil, fmt.Errorf("modulo ('%%') needs both sides to be integers, but %s is a float", f.args[0].String())
		}
		if b.isDouble {
			return nil, fmt.Errorf("modulo ('%%') needs both sides to be integers, but %s is a float", f.args[1].String())
		}

		return a.iVal % b.iVal, nil
	case "+":
		a, err := f.argAsNumber(0, ctx)
		if err != nil {
			return nil, fmt.Errorf("in %s: %s", f.String(), err)
		}
		b, err := f.argAsNumber(1, ctx)
		if err != nil {
			return nil, fmt.Errorf("in %s: %s", f.String(), err)
		}

		if a.isDouble || b.isDouble {
			return a.val + b.val, nil
		} else {
			return a.iVal + b.iVal, nil
		}
	case "-":
		a, err := f.argAsNumber(0, ctx)
		if err != nil {
			return nil, fmt.Errorf("in %s: %s", f.String(), err)
		}
		b, err := f.argAsNumber(1, ctx)
		if err != nil {
			return nil, fmt.Errorf("in %s: %s", f.String(), err)
		}

		if a.isDouble || b.isDouble {
			return a.val - b.val, nil
		} else {
			return a.iVal - b.iVal, nil
		}
	default:
		return nil, fmt.Errorf("unknown function: %s", f.String())
	}
}

// Range, inclusive on both bounds to match cypher
func rangeFn(min, max int64) (interface{}, error) {
	out := make([]interface{}, 0, max-min)
	for i := min; i <= max; i++ {
		out = append(out, i)
	}
	return out, nil
}

// Generates a random matrix with the given number of rows.
// Each cell has a random integer value within the range given by columnSpec; each entry in the spec a 2-tuple
func randomMatrix(random *rand.Rand, numRows int64, columnSpec [][]int64) []interface{} {
	out := make([]interface{}, 0, numRows)
	for i := 0; i < int(numRows); i++ {
		row := make([]interface{}, len(columnSpec))
		for col := 0; col < len(columnSpec); col++ {
			min, max := columnSpec[col][0], columnSpec[col][1]
			row[col] = uniformRand(random, min, max)
		}
		out = append(out, row)
	}
	return out
}

func uniformRand(random *rand.Rand, min, max int64) int64 {
	return min + random.Int63n(max-min)
}

const minGaussianParam = 2.0

/* translated from pgbench.c */
func gaussianRand(random *rand.Rand, min, max int64, parameter float64) (int64, error) {
	var stdev float64

	/* abort if parameter is too low, but must really be checked beforehand */
	if parameter < minGaussianParam {
		return 0, fmt.Errorf("random_gaussian 'parameter' argument must be greater than %f", minGaussianParam)
	}

	/*
	 * Get user specified random number from this loop, with -parameter <
	 * stdev <= parameter
	 *
	 * This loop is executed until the number is in the expected range.
	 *
	 * As the minimum parameter is 2.0, the probability of looping is low:
	 * sqrt(-2 ln(r)) <= 2 => r >= e^{-2} ~ 0.135, then when taking the
	 * average sinus multiplier as 2/pi, we have a 8.6% looping probability in
	 * the worst case. For a parameter value of 5.0, the looping probability
	 * is about e^{-5} * 2 / pi ~ 0.43%.
	 */
	for {
		/*
		 * random.Float64() generates [0,1), but for the basic version of the
		 * Box-Muller transform the two uniformly distributed random numbers
		 * are expected in (0, 1] (see
		 * https://en.wikipedia.org/wiki/Box-Muller_transform)
		 */
		rand1 := 1.0 - random.Float64()
		rand2 := 1.0 - random.Float64()

		/* Box-Muller basic form transform */
		sqrtVal := math.Sqrt(-2.0 * math.Log(rand1))

		stdev = sqrtVal * math.Sin(2.0*math.Pi*rand2)

		/*
		 * we may try with cos, but there may be a bias induced if the
		 * previous value fails the test. To be on the safe side, let us try
		 * over.
		 */
		if !(stdev < -parameter || stdev >= parameter) {
			break
		}
	}

	/* stdev is in [-parameter, parameter), normalization to [0,1) */
	randVal := (stdev + parameter) / (parameter * 2.0)

	/* return int64 random number within between min and max */
	return min + int64(float64(max-min+1)*randVal), nil
}

/* translated from pgbench.c */
func ExponentialRand(random *rand.Rand, min, max int64, parameter float64) (int64, error) {
	/* abort if wrong parameter, but must really be checked beforehand */
	if parameter < 0.0 {
		return 0, fmt.Errorf("parameter argument to random_exponential needs to be > 0")
	}
	cut := math.Exp(-parameter)
	/* erand in [0, 1), uniform in (0, 1] */
	uniform := 1.0 - random.Float64()

	/*
	 * inner expression in (cut, 1] (if parameter > 0), rand in [0, 1)
	 */
	if (1.0 - cut) == 0 {
		return 0, fmt.Errorf("random_exponential divide by zero error, please pick a different parameter value")
	}
	randVal := -math.Log(cut+(1.0-cut)*uniform) / parameter
	/* return int64 random number within between min and max */
	return min + int64(float64(max-min+1)*randVal), nil
}

// Hacky first stab at dealing with runtime coercion, refactor as needed
type Number struct {
	isDouble bool
	// Always set
	val float64
	// Only set if isDouble == false
	iVal int64
}

type context struct {
	s scanner.Scanner
	// Next token returned by scanner, or 0
	peek     rune
	peekText string
	done     bool
	err      error
}

func (t *context) Peek() rune {
	if t.peek == 0 {
		t.peek = t.s.Scan()
		t.peekText = t.s.TokenText()
	}
	return t.peek
}

func (t *context) Next() (rune, string) {
	if t.peek != 0 {
		next := t.peek
		nextStr := t.peekText
		t.peek = 0
		if next == scanner.EOF {
			t.done = true
		}
		return next, nextStr
	}
	next := t.s.Scan()
	if next == scanner.EOF {
		t.done = true
	}
	return next, t.s.TokenText()
}

func (t *context) fail(err error) {
	t.done = true
	if t.err != nil {
		return
	}
	t.err = fmt.Errorf("%s (at %s)", err, t.s.Pos().String())
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
