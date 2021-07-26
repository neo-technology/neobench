# neobench

Scriptable Neo4j benchmarks. Neobench helps you create and run artificial load to tune your Neo4j deployments.

## Features

- Benchmark throughput and latency
- Output in easy-to-process CSV
- Configurable concurrency
- Allows mixed workloads
- Built-in TPC-B and LDBC SNB benchmarking modes
- Scripting language for arbitrary synthetic load

## Warning: Pre-Release

Please note that this is not yet stable. 
Specifically the command line option naming is likely to change, as is the default workload.

Please do not compare benchmark results from different versions of this tool until - at the earliest - version 1.0.0.

## Installation

### Option 1: Prebuilt binary

You can download download binaries the "assets" section in the latest release [here](https://github.com/jakewins/neobench/releases).

### Option 2: Run via docker

    docker run jjdh/neobench -h

## Usage

Run `neobench -h` for a list of available options.

### Basic usage

    # Run the default, "TPC-B-like", benchmark in latency-testing mode, at a rate of 10 tx/s.
    # And, before running the benchmark, run the built-in dataset populator for TPC-B (--init)
    $ neobench --address neo4j://localhost:7687 --password secret \
        --init \
        --latency --rate 10 

### Running a custom workload

There is no built-in dataset population system for custom workloads. We recommend using your existing database if you 
have one, or writing a separate python script to populate your database. 

Then, write one or more script files, one for each transaction type you want to include, and ask neobench to run it.

    # Pick a random number between 1, 1000, and include that as the $accountId parameter,
    # Then run the specified query. 
    $ echo '
    :set accountId random(1, 1000)
    CREATE (a:Account {aid: $accountId});
    ' > myTransaction.script
    
    $ neobench --address neo4j://localhost:7687 --password secret \
        --file myworkload.script  

## Exit codes

Exit code is 2 for invalid usage.
Exit code is 1 for failure during run. 

## Scripting language

The language is heavily inspired by the scripting language used by pgbench.
See the "Custom Scripts" section in the [pgbench documentation](https://www.postgresql.org/docs/10/pgbench.html) for details and inspiration.

A workload script consists of `commands`. 
Each command is either a Cypher statement or a "meta-command".
Meta-commands start with a colon and end at the newline.
Cypher statements can span multiple lines, and end with a semi-colon.

Meta-commands generally introduce variables. 
The variables are available to subsequent meta-commands and as parameters in your queries. 

Here is a small example with two meta-commands and one query:

    :set numPeople 1000000
    :set personId random(1, $numPeople)
    MATCH (p:Person {id: $personId}) RETURN p;

Scripts are currently ran as a single transaction, though that may change before 1.0.

The following meta-commands are currently supported:

    :set <variable> <expression>
    ex: :set myParam random(1, 1000)
    
    :sleep <expression> <unit>
    ex: :sleep random(0, 60) ms

### Expressions

You use expressions to set variable values in your script files, for instance:

    :set myVar [1, 2, random(1, 10)]

Each time this meta-command executes, `$myVar` will be set to a list of `[1, 2, <random number between 1-10>]`.
If $myVar is used in a subsequent query, the value will be sent to Neo4j as a parameter.

#### Basics

- Integers (`0`, `-10`, `-99999990`)
- Floats (`0.0`, `13.37`)
- Arithmethic (`1 + 1`, `(1 * 3) - 37`)
- Strings (`"hello"`, `""`)
- Lists (`[1,2,3]`)
- Maps (`{a: 1, b: 2}`)

### List comprehensions

Very useful for generating collections of artificial data. 
List comprehensions in neobench use the same syntax as list comprehension in Neo4j:

    [ <variable> in <source> | <expression> ]
    
For example

    [ i in range(1, 10) | { uid: $i, email: $i + "@gmail.com" } ]

### Functions

- abs
  - `abs(v: int) -> int`
  - Get the absolute (positive) value of the given integer.
- csv
  - `csv(path: string) -> list`
  - Load a csv file from disk. Path is relative to the script files location.
- len
  - `len(v: list) -> int`
  - Get number of items in the argument.
- pi
  - `pi() -> float`
  - It's pi!
- random
  - `random(min: int, max: int) -> int`
  - Uniform pseudo-random integer in the given range. Range is inclusive of both parameters.
- random_gaussian
  - `random_gaussian(min: int, max: int, param: float) -> int`
  - Gaussian-distributed random integer in the given range, with param shaping the curve. 
    See pgbenchs documentation for the param value, as this implementation mirrors that.
- random_exponential
  - `random_exponential(min: int, max: int, param: float) -> int`
  - Exponentially-distributed random integer in the given range, with param shaping the curve. 
    See pgbenchs documentation for the param value, as this implementation mirrors that.
- range
  - `range(min: int, max: int) -> list`
  - Generate a list starting at `min` and ending at `max`.
- sqrt
  - `sqrt(v: float) -> float`
  - Get the square root of the argument. 

# Contributions

Minor contributions? Just open a PR. 

Big changes? Please open a ticket first proposing the change, so you don't do a bunch of work that doesn't end up merged.
  
# License

Apache 2
