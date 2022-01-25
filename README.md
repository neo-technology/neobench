
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

## Contributions

This project has no current maintainer. 

## License

Apache 2
