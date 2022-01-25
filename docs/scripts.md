# Neobench custom scripts

([Back to docs overview](overview.md))

Workloads are defined as a collection of one or more `Scripts`.
Each `Script` defines a single transaction to run against the `Target` database.
`Scripts` are a sequence of `Commands`, actions you want neobench to take.

## Example script

As an example, this is the script (defined [here](../pkg/neobench/builtin/ldbc_like.go)) for one of the transactions the built-in LDBC-like workload runs:

```
:set personId random(1, 9892 * $scale)

MATCH (:Person {id: $personId})-[:KNOWS]-(friend),
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
```

It consists of two `Commands`. 
First, a `:set` `Meta Command` sets the `personId` parameter to a random value.
Then a `Query Command` executes a query that uses the generated parameter.

## How to run scripts

You can tell neobench to run scripts either by referencing local file system files (`--file`), or by writing scripts directly on the command line (`--script`).
You can mix-and match `--script` and `--file`, and specify either as many times as you like, each time defining an additional script.
You can actually even mix `--script`, `--file` and `--builtin`, adding your own custom scripts as part of the mix a [builtin workload](builtin.md) runs.

### Specify scripts directly on the command line

```
neobench --script "RETURN 1" --script "RETURN 2"
```

Will define two scripts that each contain one `Query Command` (ie. the query you see defined there), and execute them with equal distribution.

### Specify scripts from the file system

```
neobench --file path/to/workload.script
```

### Specify script weights

When you use the `--file` flag, you can optionally specify a "weight", which is used to determine how often a given script is selected to be ran.
This lets you compose workloads that, for instance, run 10 read transactions for each write transaction.

It's often useful to set the least-frequent script to have a weight of `1`, and then set the others to be higher weights relative to that.
Then you can think of it as "for each time this script is run, run this other script N times".

For instance, to run a `read.script` file 5 times for each time neobench runs a `write.script` file:

```
neobench --file write.script@1 --file read.script@5
```

If you review the code, you'll find that this weight system is how the built-in ldbc-like workload sets the right distribution of scripts to execute.

## Commands

When `Neobench` runs a workload, it will start a transaction and then evaluate a `Script` "inside" the transaction.
The script is evaluated by executing, one at a time, each `Command` the script defines.

Commands are either `Meta Commands` that do something locally inside `neobench`, or `Query Commands` that send off a query.

### Query Commands

`Query Commands` are cypher queries, they end with a semi-colon. 
The simplest `Script` you can run would be just a single `Query Command` with a simple cypher query:

```
RETURN "Hello, World!";
```

You can have as many queries as you like in your script.
They will be executed one at a time, with `neobench` fetching the result of the prior query before executing the next:

```
RETURN "Hello from the first query!";

RETURN "Hello from the second query!";
```

#### Parameter substitution

Neobench will detect if you use parameters in the query. 
If neobench knows about the parameter, it'll include it along when it sends the query. 
You can define parameters wither with the `-D foo=bar` option, or using `Meta Commands`. 

Parameters that are not mentioned in a query are not included in the payload sent to the database.

```
// run with eg neobench -D foo=bar to set $foo to something
RETURN $foo;
```

The above script will send the query `RETURN $foo`, and include the parameter `foo=bar` along with it.

#### Local parameter substitution

Sometimes you want to test how Neo4j handles large sets of different query strings.
You can ask `neobench` to expand parameters locally before sending the query by using double-dollar parameters:

```
// run with eg neobench -D foo=bar to set $foo to something
RETURN $$foo;
```

The above script will send the query `RETURN "bar"` to Neo4j. 

### Meta Commands

Metacommands are executed locally.
They let you do things like define a parameter that changes each time a script is invoked.

Meta commands start with a colon (`:`) and end at the newline.

#### The :set meta command

This lets you set the value of a parameter.
It is re-executed each time `neobench` generates a transaction with the script.

```
:set foo "bar"

RETURN $foo;
```

The above script will set the parameter `foo` to `"bar"` and then include the paramter with the query sent to Neo4j.

The syntax is `:set <parameter-name> <expression>`. There is a broad set of expressions you can use, see further down.

#### The :sleep meta command

This can be used to simulate the client application doing some work while a transaction is open.

```
RETURN "Hello from the first query!";

:sleep 10 s

RETURN "Hello from the second query!";
```

The above script will run the first query, then sleep 10 seconds, then run the second query, all in one transaction.

The following units are available: `s`, `ms`, `us`.

#### The :opt meta command

The `:opt` meta command lets you set options for your script. 
Currently only one option is available, "autommit", which modifies the execution of the script so that each query is ran as an auto-commit transaction.

## Expressions

Expressions are used to generate synthetic data for your queries.
In general, the goal has been that the local expression syntax is identical to the expression syntax of Cypher.
Hence, try writing the expression as you'd do it in cypher first, and you'll likely see that work.

However, the expression engine is much simpler than the actual cypher one, and the available functions are different.
The canonical source of how expressions parse and work is [in the code](../pkg/neobench/parser_test.go).

### Examples

Here's an example expression that picks a random value from a list:

```
:set myList ["Bob", "Angela", "Pete"]
:set myValue $myList[random(0, len($myList))]
```

### Types & literal value syntax

Neobench has the following types:

| Type   | Description        | Syntax Example                          |   |
|--------|--------------------|-----------------------------------------|---|
| String | A text string      | "Hello, world!"                         |   |
| int    | A 64-bit integer   | 1337                                    |   |
| float  | A 64-bit float     | 13.37                                   |   |
| map    | A map / dictionary | {"Hello": {"Name": "World"}, "Age": 99} |   |
| list   | A list             | [1,2, "Hello", ["a", "b"]]              |   |

### Syntax

See the `Types` section above for syntax examples of each type. 

#### Math

```
# Arithmetic
:set o 1 + 1 + 1
:set o 1 - 1 - 1

# Multiplication & division
:set o 1 * 1 * 2
:set o 1 / 2 / 3

# Modulo
:set o 7 % 3
```

#### Function syntax

```
# Call the `random` function and assign the result to `$o`
:set o random( 1, 100 )
```

#### Indexing

You can access entries in lists and maps with the `[<key>]` indexing syntax:

```
:set myList range(1, 100)
:set entry7 myList[7]
```

#### List comprehensions

Neobench supports list comprehensions.
This is a useful for generating arbitrary synthetic datasets.
The syntax is the same as cypher list comprehensions.

```
# Generate a new list from the list range(1,3) outputs, where each entry is multiplied by 1337
:set myList [ i in range(1,3) | $i * 1337 ]

# Generate a list of 10 randomly selected names from a locally loaded CSV
# note that the CSV function caches the loaded file, so you can load large files with low performance impact
:set allNames csv("list-of-names.csv")
:set names [ i in range(1,10) | $allNames[random(0, len($allNames))] ]

# List comprehensions can be arbitrarily nested
:set listOfLists [ i in range(1,10) | [ o in range(1,5) | $o ] ]
```

### Functions

Neobench ships with a set of functions you can use in expressions.

There are more functions than these. Initiailly functions were implemented to match those in `pgbench`.
However, the goal shifted to try to match the functions available in Cypher, to reduce cognitive load.
Currently that means the functions are an in-between mess that should be cleaned up.

#### Math / number functions

| Name      | Description                           | Example   | Example Output |
|-----------|---------------------------------------|-----------|----------------|
| pi()      | Outputs Pi                            | pi()      | 3.14...        |
| abs(v)    | Gives the absolute value of the input | abs(-1.1) | 1.1            |
| int(v)    | Coerces the input `v` to int          | int(1.1)  | 1              |
| double(v) | Coerces the input `v` to float        | double(1) | 1.0            |
| sqrt(v)   | Square root of input                  | sqrt(4)   | 2              |

#### List functions

| Name        | Description                                              | Example         | Example Output  |
|-------------|----------------------------------------------------------|-----------------|-----------------|
| len(v)      | Gives length of input list or dict                       | len([1, 2])     | 2               |
| range(a, b) | Generates a list of incrementing numbers from `a` to `b` | range(1,3)      | [1,2,3]         |
| csv(p)      | Reads CSV file at `p`, relative to script file path      | csv("data.csv") | [ [1,2], [3,4]] |

