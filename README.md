# neobench - scriptable Neo4j benchmarks

neobench helps you tune your Neo4j deployment by letting you run custom workloads. 
You can explore how changing the database and server tuning changes throughput and latency.

It is heavily inspired by [pgbench](https://www.postgresql.org/docs/10/pgbench.html), and uses a similar scripting language.
Neobench even ships with a default "tpcb-like" workload!

# Warning: Pre-Release State!

Please note that this is not yet stable. 
Specifically the command line option naming is likely to change, as is the default workload.

Please do not compare benchmark results from different versions of this tool until - at the earliest - version 1.0.0.

# Installation

You can download pre-built binaries from the "assets" section in the latest release [here](https://github.com/jakewins/neobench/releases).

The command is also available in dockerhub, so you can run it directly as a docker workload.
See `docker run jjdh/neobench -h`, usage is identical to the cli command.

Alternatively you can build from source by checking out this repo and running `make`, or even just `go build .` if you'd rather skip integration tests.

# Minimum examples

    # Run the "TPCB-like" workload for 60 seconds against the default url, neo4j://localhost:7687
    # in throughput testing mode and with one single worker / session
    $ neobench -d 60
    
    # Same as above, except measure latency instead of throughput and with concurrent load
    $ neobench --latency --clients 4
    
    # Run a throughput test with a custom workload
    $ cat myworkload.script
    \set accountId random(1, $scale * 1000)
    CREATE (a:Account {aid: $accountId});
    
    $ neobench -w myworkload.script 

# Usage

```
Usage:
  neobench [OPTION]... [DBNAME]

Options:
  -a, --address string          address to connect to, eg. neo4j://mydb:7687 (default "neo4j://localhost:7687")
  -c, --clients int             number of concurrent clients / sessions (default 1)
  -D, --define stringToString   defines variables for workload scripts and query parameters (default [])
  -d, --duration int            seconds to run (default 60)
  -e, --encryption auto         whether to use encryption, auto, `true` or `false` (default "auto")
  -i, --init                    run in initialization mode; if using built-in workloads this creates the initial dataset
  -l, --latency                 run in latency testing more rather than throughput mode
  -o, --output auto             output format, auto, `interactive` or `csv` (default "auto")
  -p, --password string         password (default "neo4j")
  -r, --rate float              in latency mode (see -l) this sets transactions per second, total across all clients (default 1)
  -s, --scale scale             sets the scale variable, impact depends on workload (default 1)
  -u, --user string             username (default "neo4j")
  -w, --workload strings        workload to run, either a builtin: one or a path to a workload script (default [builtin:tpcb-like])
```

# Exit codes

Exit code is 2 for invalid usage.
Exit code is 1 for failure during run. 

# Custom scripts

I aspire to support the same language as pgbench. 
See the "Custom Scripts" section in the [pgbench documentation](https://www.postgresql.org/docs/10/pgbench.html) for details and inspiration.

A workload script consists of `commands`. 
Each command is either a Cypher statement or a "meta-command".
Meta-commands start with a backslash and end at the newline.
Cypher statements can span multiple lines, and end with a semi colon.

Meta-commands generally introduce variables. 
The variables are available to subsequent meta-commands and as parameters in your queries. 

Here is a small example with two meta-commands and one query:

    \set numPeople $scale * 1000000
    \set personId random() * $numPeople
    MATCH (p:Person {id: $personId}) RETURN p;

Scripts are currently ran as a single transaction, though that may change before 1.0.

The following meta-commands are currently supported:

    \set <variable> <expression>
    ex: \set myParam random() * 1000
    
    \sleep <expression> <unit>
    ex: \sleep random() * 60 ms

All expressions supported by pgbench 10 are supported, please see the pgbench docs linked above.

# Contributions

Minor contributions? Just open a PR. 

Big changes? Please open a ticket first proposing the change, so you don't do a bunch of work that doesn't end up merged.
  
# License

Apache 2
