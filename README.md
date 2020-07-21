# neobench - scriptable Neo4j benchmarks

neobench helps you tune your Neo4j deployment by measuring how different workloads perform.

neobench is heavily inspired by pgbench. 
It uses a similar scripting language, and ships with a similar default "tpcb-like" workload.

# Installation

You can download pre-built binaries from the "assets" section in the latest release [here](https://github.com/jakewins/neobench/releases).

Alternatively you can build from source by checking out this repo and running `make`, or even just `go build .` if you'd rather skip integration tests.

# Minimum examples

    # Run the "TPCB-like" workload for 60 seconds against the default url, bolt://localhost:7687
    # with encryption disabled, measuring throughput for a single worker.
    neobench -e=false
    
    # Same as above, but measure latency when running at 1 transaction per second
    # and with 4 concurrent clients
    neobench -e=false -m latency -c 4
    
    # Run a throughput test with a custom workload
    echo "\set accountId random(1,:scale * 1000)
    CREATE (a:Account {aid: \$accountId});" > myworkload.script
    neobench -e=false -w myworkload.script 
  
# Contributions

Minor contributions? Just open a PR. 

Big changes? Please open a ticket first proposing the change, so you don't do a bunch of work that doesn't end up merged.
  
# License

Apache 2
