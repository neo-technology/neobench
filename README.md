# neobench - scriptable Neo4j benchmarks

neobench helps you tune your Neo4j deployment by letting you run custom workloads. 
You can explor how changing the database and server tuning changes throughput and latency.

It is heavily inspired by pgbench, and uses a similar scripting language.
Neobench even ships with a default "tpcb-like" workload!

# Warning: Pre-Release State!

Please note that this is not yet stable. I intend to change the variable prefix from `:` to `$` to match cypher, and there may be changes to the command line options.

Please do not compare benchmark results from different versions of this tool until - at the earliest - version 1.0.0.

# Installation

You can download pre-built binaries from the "assets" section in the latest release [here](https://github.com/jakewins/neobench/releases).

Alternatively you can build from source by checking out this repo and running `make`, or even just `go build .` if you'd rather skip integration tests.

# Minimum examples

    # Run the "TPCB-like" workload for 60 seconds against the default url, bolt://localhost:7687
    # with encryption disabled, measuring throughput for a single worker.
    $ neobench -e=false
    
    # Same as above, but measure latency when running at 1 transaction per second
    # and with 4 concurrent clients
    $ neobench -e=false -m latency -c 4
    
    # Run a throughput test with a custom workload
    $ cat myworkload.script
    \set accountId random(1, :scale * 1000)
    CREATE (a:Account {aid: $accountId});
    
    $ neobench -e=false -w myworkload.script 

# Custom scripts

We aspire to support the same language as pgbench. 
Currently the `\set` meta-command is supported, along with `*` for multiplication and the `random` function.

See the "Custom Scripts" section in the [pgbench documentation](https://www.postgresql.org/docs/10/pgbench.html) for details and inspiration.

# Contributions

Minor contributions? Just open a PR. 

Big changes? Please open a ticket first proposing the change, so you don't do a bunch of work that doesn't end up merged.
  
# License

Apache 2
