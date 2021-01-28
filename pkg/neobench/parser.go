package neobench

import (
	"fmt"
	"github.com/pkg/errors"
	"math"
	"math/rand"
	"path/filepath"
	"strconv"
	"strings"
	"text/scanner"
	"time"
)

func Parse(filename, script string, weight float64) (Script, error) {
	c := newParseContext(script, filename)

	commands := make([]Command, 0)

	for !c.done {
		tok := c.PeekToken()
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

func metaCommand(c *parseContext) Command {
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
		switch c.PeekToken() {
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
				c.fail(fmt.Errorf("\\sleep command must use 'us', 'ms', or 's' unit argument - or none. got: %s", unitStr))
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

func command(c *parseContext) Command {
	originalWhitespace := c.s.Whitespace
	defer func() {
		c.s.Whitespace = originalWhitespace
	}()
	c.s.Whitespace = 0
	var b strings.Builder
	for tok, content := c.Next(); tok != ';' && tok != scanner.EOF; tok, content = c.Next() {
		b.WriteString(content)
	}
	query := b.String()
	return QueryCommand{
		Query:  query,
		Params: parseParams(query, c.s.Filename),
	}
}

// Extract a list of parameters used in a given query string
func parseParams(query, filename string) []string {
	params := make(map[string]bool)
	c := newParseContext(query, filename)
	for !c.done {
		tok := c.PeekToken()
		if tok == scanner.EOF {
			break
		} else if tok == '$' {
			c.Next()
			if name, err := tryIdent(c); err == nil {
				params[name] = true
			}
		} else if tok == '{' {
			// '{' is ambiguous; we specifically want the pattern ['{', IDENT, '}']
			c.Next()
			name, err := tryIdent(c)
			if err != nil {
				// Not followed by Ident, whatever it is it doesn't look like a param
				continue
			}
			if c.PeekToken() != '}' {
				// "{ IDENT", but the next token isn't }
				continue
			}
			params[name] = true
		}
		c.Next()
	}

	out := make([]string, 0, len(params))
	for k := range params {
		out = append(out, k)
	}
	return out
}

func ident(c *parseContext) string {
	name, err := tryIdent(c)
	if err != nil {
		c.fail(err)
	}
	return name
}

// Try to parse an identifier; if you can't, return an error, don't put context in failure mode
func tryIdent(c *parseContext) (string, error) {
	tok := c.PeekToken()
	if tok == scanner.RawString {
		// backtick-quoted identifier
		_, content := c.Next()
		return content[1 : len(content)-1], nil
	}
	if tok == scanner.Ident {
		_, content := c.Next()
		return content, nil
	}
	return "", fmt.Errorf("expected identifier, got '%s'", scanner.TokenString(tok))
}

func expr(c *parseContext) Expression {
	lhs := term(c)
	for {
		tok := c.PeekToken()
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

func term(c *parseContext) Expression {
	lhs := factor(c)
	for {
		tok := c.PeekToken()
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
		} else if tok == '[' {
			c.Next()
			index := expr(c)
			expect(c, ']')
			lhs = Expression{
				Kind: sliceExpr,
				Payload: SliceExpr{
					src: lhs,
					i:   index,
				},
			}
		} else {
			return lhs
		}
	}
}

func factor(c *parseContext) Expression {
	tok, content := c.Next()
	if tok == scanner.Ident {
		funcName := content
		var args []Expression
		expect(c, '(')
		tok := c.PeekToken()
		for tok != ')' {
			if len(args) > 0 {
				expect(c, ',')
			}
			args = append(args, expr(c))
			if c.done {
				return Expression{}
			}
			tok = c.PeekToken()
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
	} else if tok == scanner.String {
		return Expression{Kind: stringExpr, Payload: content[1 : len(content)-1]}
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
		// To tell the difference between lists and comprehensions, we need to look ahead 2 tokens; we
		// do that by stepping forward and then pushing stuff back
		peek1, peek1Text := c.Next()
		peek2, peek2Text := c.Peek()
		c.Push(peek1, peek1Text) // Undo the Next() call

		// Pattern for list comprehensions
		if peek1 == scanner.Ident && peek2 == scanner.Ident && strings.ToLower(peek2Text) == "in" {
			return listComprehension(c)
		}

		// No, parse literal list
		tok := peek1
		list := make([]Expression, 0)
		for tok != ']' {
			if len(list) > 0 {
				expect(c, ',')
			}
			list = append(list, expr(c))
			if c.done {
				return Expression{}
			}
			tok = c.PeekToken()
		}
		c.Next()
		return Expression{Kind: listExpr, Payload: list}
	} else if tok == '{' {
		out := make(map[string]Expression)

		tok := c.PeekToken()
		for tok != '}' {
			if len(out) > 0 {
				expect(c, ',')
			}
			key := ident(c)
			expect(c, ':')
			value := expr(c)
			out[key] = value
			if c.done {
				return Expression{}
			}
			tok = c.PeekToken()
		}
		c.Next()
		return Expression{Kind: mapExpr, Payload: out}
	} else {
		c.fail(fmt.Errorf("unexpected token, expected Expression: %s", scanner.TokenString(tok)))
		return Expression{}
	}
}

func listComprehension(c *parseContext) Expression {
	itemName := ident(c)
	maybeIn := ident(c)
	if strings.ToLower(maybeIn) != "in" {
		c.fail(fmt.Errorf("don't know what '[ %s %s ..' means, did you mean to add a comma after '%s'",
			itemName, maybeIn, itemName))
		return Expression{}
	}
	srcExpr := expr(c)
	expect(c, '|')
	outExpr := expr(c)
	expect(c, ']')
	return Expression{
		Kind: listCompExpr,
		Payload: ListCompExpr{
			itemName: itemName,
			src:      srcExpr,
			out:      outExpr,
		},
	}
}

func expect(c *parseContext, expected rune) {
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
	// payload string
	stringExpr ExprKind = 3
	// payload map[string]Expression
	mapExpr ExprKind = 4
	// payload []Expression
	listExpr ExprKind = 5
	// payload ListCompExpr
	listCompExpr ExprKind = 6
	// payload CallExpr
	sliceExpr ExprKind = 7
	// payload CallExpr
	callExpr ExprKind = 8
	// payload string (varname)
	varExpr ExprKind = 9
)

func (e ExprKind) String() string {
	return exprKindNames[e]
}

var exprKindNames = []string{
	nullExpr:     "N/A",
	intExpr:      "int",
	floatExpr:    "double",
	stringExpr:   "string",
	mapExpr:      "map",
	listExpr:     "list",
	listCompExpr: "listcomp",
	sliceExpr:    "slice",
	callExpr:     "call",
	varExpr:      "var",
}

type Expression struct {
	Kind    ExprKind
	Payload interface{}
}

func (e Expression) Eval(ctx *ScriptContext) (interface{}, error) {
	switch e.Kind {
	case intExpr, floatExpr, stringExpr:
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
	case mapExpr:
		innerExprs := e.Payload.(map[string]Expression)
		out := make(map[string]interface{})
		for k, innerExpr := range innerExprs {
			exprResult, err := innerExpr.Eval(ctx)
			if err != nil {
				return nil, errors.Wrapf(err, "error when evaluating %s", e)
			}
			out[k] = exprResult
		}
		return out, nil
	case listCompExpr:
		return e.Payload.(ListCompExpr).Eval(ctx)
	case sliceExpr:
		return e.Payload.(SliceExpr).Eval(ctx)
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
	case stringExpr:
		return fmt.Sprintf("\"%s\"", e.Payload)
	case mapExpr, listExpr:
		return fmt.Sprintf("%v", e.Payload)
	case sliceExpr:
		return e.Payload.(SliceExpr).String()
	case listCompExpr:
		return e.Payload.(ListCompExpr).String()
	case callExpr:
		return e.Payload.(CallExpr).String()
	case varExpr:
		return fmt.Sprintf(":%v", e.Payload)
	default:
		return fmt.Sprintf("err(%v)", e.Payload)
	}
}

// Intended to be expanded into a richer slicing system, for now just simple indexing
type SliceExpr struct {
	src Expression
	i   Expression
}

func (s SliceExpr) String() string {
	return fmt.Sprintf("%s[%s]", s.src.String(), s.i.String())
}

func (s SliceExpr) Eval(ctx *ScriptContext) (interface{}, error) {
	srcRaw, err := s.src.Eval(ctx)
	if err != nil {
		return nil, err
	}
	src, ok := srcRaw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("slicing only work on lists, got %v", srcRaw)
	}

	iRaw, err := s.i.Eval(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "in slice %s", s.String())
	}

	iNum, err := asNumber(iRaw)
	if err != nil {
		return nil, errors.Wrapf(err, "expected integer as slice argument in %s", s.String())
	}
	if iNum.isDouble {
		return nil, fmt.Errorf("floats can't be used as indexes in slices, in %s", s.String())
	}
	i := iNum.iVal

	return src[i], nil
}

// [i in range(1,10) | i * 2]
type ListCompExpr struct {
	itemName string
	// Expression that yields a list
	src Expression
	// Evaluated once for each item in src, with item named itemName
	out Expression
}

func (s ListCompExpr) String() string {
	return fmt.Sprintf("[%s in %s | %s]", s.itemName, s.src.String(), s.out.String())
}

func (s ListCompExpr) Eval(ctx *ScriptContext) (interface{}, error) {
	srcRaw, err := s.src.Eval(ctx)
	if err != nil {
		return nil, err
	}
	src, ok := srcRaw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("source in list comprehension must be a list, got %v from %s", src, s.src)
	}

	out := make([]interface{}, len(src))
	innerCtx := ScriptContext{
		Script:    ctx.Script,
		Stderr:    ctx.Stderr,
		Vars:      make(map[string]interface{}),
		Rand:      ctx.Rand,
		CsvLoader: ctx.CsvLoader,
	}
	for k, v := range ctx.Vars {
		innerCtx.Vars[k] = v
	}
	for i := range src {
		innerCtx.Vars[s.itemName] = src[i]
		out[i], err = s.out.Eval(&innerCtx)
		if err != nil {
			return nil, errors.Wrapf(err, "when evaluating %s=%v in %s", s.itemName, src[i], s.String())
		}
	}
	return out, nil
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
	return asNumber(value)
}

func (f CallExpr) argAsString(i int, ctx *ScriptContext) (string, error) {
	if len(f.args) <= i {
		return "", fmt.Errorf("expected at least %d arguments, got %d", i+1, len(f.args))
	}
	value, err := f.args[i].Eval(ctx)
	if err != nil {
		return "", err
	}
	switch value.(type) {
	case string:
		return value.(string), nil
	default:
		return "", fmt.Errorf("expected string, got %s (which is %T)", f.args[i].String(), value)
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
	// TODO: Align with name cypher uses
	case "len":
		if len(f.args) == 0 {
			return nil, fmt.Errorf("len(..) requires an argument")
		}
		rawSrc, err := f.args[0].Eval(ctx)
		if err != nil {
			return nil, errors.Wrapf(err, "in %s", f.String())
		}
		src, ok := rawSrc.([]interface{})
		if !ok {
			return nil, fmt.Errorf("argument to len(..) needs to be a list, in %s", f.String())
		}
		return int64(len(src)), nil
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
	case "csv":
		path, err := f.argAsString(0, ctx)
		if err != nil {
			return nil, errors.Wrap(err, "csv(..) takes string as argument")
		}
		absPath, err := absPath(ctx.Script.Name, path)
		if err != nil {
			return nil, errors.Wrapf(err, "failed resolving path %s relative to %s in %s", path, ctx.Script.Name, f.String())
		}
		return ctx.CsvLoader.Load(absPath)
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

func asNumber(raw interface{}) (Number, error) {
	switch raw.(type) {
	case int64:
		iVal := raw.(int64)
		return Number{isDouble: false, val: float64(iVal), iVal: iVal}, nil
	case float64:
		return Number{isDouble: true, val: raw.(float64)}, nil
	case Number:
		return raw.(Number), nil
	default:
		return Number{}, fmt.Errorf("expected int64 or float64, got %v", raw)
	}
}

type parseToken struct {
	token rune
	text  string
}

type parseContext struct {
	s scanner.Scanner
	// The stack is used for peeking and backtracking;
	// it only comes into play if you call Peek or manually manipulate it.
	// When calling Next(), it first checks (and pops) the stack before it goes
	// to the scanner, `s`.
	stack []parseToken
	done  bool
	err   error
}

func newParseContext(in, name string) *parseContext {
	var s scanner.Scanner
	s.Init(strings.NewReader(in))
	s.Filename = name
	s.Whitespace ^= 1 << '\n' // don't skip newlines

	return &parseContext{
		s: s,
	}
}

func (t *parseContext) Peek() (rune, string) {
	if len(t.stack) == 0 {
		token := t.s.Scan()
		text := t.s.TokenText()
		t.stack = append(t.stack, parseToken{
			token: token,
			text:  text,
		})
	}
	token := t.stack[len(t.stack)-1]
	return token.token, token.text
}

func (t *parseContext) PeekToken() rune {
	token, _ := t.Peek()
	return token
}

// Backtrack, push the given pair onto the stack, effectively stepping backwards
func (t *parseContext) Push(token rune, text string) {
	t.stack = append(t.stack, parseToken{
		token: token,
		text:  text,
	})
}

func (t *parseContext) Next() (rune, string) {
	if len(t.stack) != 0 {
		next := t.stack[len(t.stack)-1]
		t.stack = t.stack[:len(t.stack)-1]
		if next.token == scanner.EOF {
			t.done = true
		}
		return next.token, next.text
	}
	next := t.s.Scan()
	if next == scanner.EOF {
		t.done = true
	}
	return next, t.s.TokenText()
}

func (t *parseContext) fail(err error) {
	t.done = true
	if t.err != nil {
		return
	}
	t.err = fmt.Errorf("%s (at %s)", err, t.s.Pos().String())
}

func absPath(scriptName, path string) (string, error) {
	if strings.HasPrefix(scriptName, "builtin:") {
		// builtin script.. should not be referring to any file system things
		panic(fmt.Sprintf("%s should not be accessing: %s", scriptName, path))
	}
	if filepath.IsAbs(path) {
		return path, nil
	}

	scriptDir := filepath.Dir(scriptName)

	// We normalize the paths so that separate scripts referring to the same csv file hit the same cache slot
	// in CsvLoader.
	return filepath.Abs(filepath.Join(scriptDir, path))
}
