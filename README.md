
<p align="center"><img src="/demo.gif?raw=true"/></p>

# neobench

Scriptable Neo4j benchmarks. Neobench helps you create and run artificial load to tune your Neo4j deployments.

## Features

- Benchmark throughput and latency
- Output in easy-to-process CSV
- Configurable concurrency
- Allows mixed workloads
- Built-in TPC-B and LDBC SNB benchmarking modes
- Custom workloads using built-in scripting language

## Installation

### Option 1: Prebuilt binary

You can download binaries in the "assets" section in the latest release [here](https://github.com/jakewins/neobench/releases).

### Option 2: Run via docker

Easier to use if you don't want to run against a database running on the host machine.

    docker run jjdh/neobench -h

## Usage

Run `neobench -h` for a list of available options.

    # Run the built-in "TPC-B-like" benchmark in throughput testing mode.
    # Before running the benchmark, run the built-in dataset populator for TPC-B (--init)
    $ neobench --address neo4j://localhost:7687 --password secret \
        --builtin tpcb-like --init

## Documentation

See [the Docs](docs/overview.md) for more details.

## Building & releasing

Build and run integration tests with `make`

Release with `bin/release`

## Issues prior to 1.0

- Functions should be changed to align with cypher functions as much as possible (eg. `greatest`, `least` etc)
- Syntax around parameters and when to use `$` and when not to is confusing in list comprehensions,
  maybe default to never using `$` except in queries? Eg `:set a [ i in range(1,10) | $i ]`; why no $ the first time
  `i` is used, but then `$` when it's used the second time?
- Load generation system should probably be modified to have a single goroutine generate load that goes on a queue
  for workers to execute, rather than each worker generating its own load, because that causes weird blocking issues
- Support dropping transactions / marking them failed when database can't keep up with a set rate
- Warn or crash if user specifies `--rate` without also specifying `--latency`

## Contributions

This project has no current maintainer. 

## License

Apache 2
