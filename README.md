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

Alternatively you can build from source by checking out this repo and running `make`, or even just `go build .` if you'd rather skip integration tests.

# Minimum examples

    # Run the "TPCB-like" workload for 60 seconds against the default url, bolt://localhost:7687
    # with encryption disabled, measuring throughput for a single worker.
    $ neobench
    
    # Same as above, but measure latency when running at 1 transaction per second
    # and with 4 concurrent clients
    $ neobench -m latency -c 4
    
    # Run a throughput test with a custom workload
    $ cat myworkload.script
    \set accountId random(1, $scale * 1000)
    CREATE (a:Account {aid: $accountId});
    
    $ neobench -w myworkload.script 

# Custom scripts

I aspire to support the same language as pgbench. 
See the "Custom Scripts" section in the [pgbench documentation](https://www.postgresql.org/docs/10/pgbench.html) for details and inspiration.

A workload script consists of `commands`. 
Each command is either a Cypher statement or a "meta-command".
Cypher-statements end with semi-colon, meta-commands end with newline.

Meta-statements generally introduce variables. 
The variables are available to subsequent meta-commands and as parameters in your queries. 

Here is a small example with two meta-commands and one query:

    \set numPeople $scale * 1000000
    \set personId random() * numPeople
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
