# Neobench documentation

Neobench is originally a clone of [pgbench](https://www.postgresql.org/docs/14/pgbench.html).
It is a simple program to run database workloads against Neo4j clusters.

Neobench runs a specified mix of generated transactions over and over, and then reports either the latency or the throughput as requested.

Use cases include:

- Optimize your neo4j configuration for a given workload
- Explore the workload itself, testing how changing queries changes performance
- Optimize neo4j code for a given workload

## Basic usage

The following neobench invocation will run the built-in TPC-B-like benchmark, including populating the dataset, for one minute.

    neobench \
      --address neo4j://localhost:7687 \
      --password secret \
      --builtin tpcb-like \
      --init \
      --duration 1m \
      --clients 4
 
## Mental model

### Clients and Scripts

Neobench runs a specified number of `Clients`, each a Go thread. 
The `Clients` each run a loop where they generate transactions against the `Target` database.
What each transaction does is defined in one or more `Scripts`.

### Latency and Throughput

In order to avoid a phenomena called [Coordinated Omission](http://highscalability.com/blog/2015/10/5/your-load-generator-is-probably-lying-to-you-take-the-red-pi.html), Neobench does not let you test both latency and throughput at the same time.
Instead, you can either test total throughput, or you can test latency at some pre-determined throughput.

In other words, you can ask either "How many transactions per second of this workload can my database handle?", or you can ask "What's the latency distribution of this workload if it executes at 10 transactions per second?".

A common use case is to estimate how many transactions per second will arrive from your clients, and then test with neobench that your latency requirements will be met at that throughput.

Throughput mode is the default. Neobench switches to latency mode if you give it the `--latency` flag. You can then set the target throughput with the `--rate` option.

## Flags

```
neobench is a benchmarking tool for Neo4j.

Usage:
  neobench [OPTION]... [DBNAME]

Options:
  -a, --address string               address to connect to (default "neo4j://localhost:7687")
  -b, --builtin strings              built-in workload to run 'tpcb-like' or 'ldbc-like', default is tpcb-like
  -c, --clients int                  number of concurrent clients / sessions (default 1)
  -D, --define stringToString        defines variables for workload scripts and query parameters (default [])
      --driver-debug-logging         enable debug-level logging for the underlying neo4j driver
  -d, --duration duration            duration to run, ex: 15s, 1m, 10h (default 1m0s)
  -e, --encryption auto              whether to use encryption, auto, `true` or `false` (default "auto")
  -f, --file strings                 path to workload script file(s)
  -i, --init                         when running built-in workloads, run their built-in dataset generator first
  -l, --latency                      run in latency testing more rather than throughput mode
      --max-conn-lifetime duration   when connections are older than this, they are ejected from the connection pool (default 1h0m0s)
      --no-check-certificates        disable TLS certificate validation, exposes your credentials to anyone on the network
  -o, --output auto                  output format, auto, `interactive` or `csv` (default "auto")
  -p, --password string              password (default "neo4j")
      --progress duration            interval to report progress, ex: 15s, 1m, 1h (default 10s)
  -r, --rate float                   in latency mode (see -l) sets total transactions per second (default 1)
  -s, --scale scale                  sets the scale variable, impact depends on workload (default 1)
  -S, --script stringArray           script(s) to run, directly specified on the command line
  -u, --user string                  username (default "neo4j")
```



- Neobench is derived from pgbench
- Basic example
- Runs workloads against a specified Neo4j cluster
- Latency vs throughput benchmarking
- Builtin workloads
- Custom workloads
