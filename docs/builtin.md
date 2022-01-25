# Neobench builtin workloads

([Back to docs overview](overview.md))

Neobench includes two built-in workloads. 
They are defined by `Scripts` like any other workload, you can see their definitions [here](../pkg/neobench/builtin/ldbc_like.go) and [here](../pkg/neobench/builtin/tpcb_like.go).

The builtin workloads do have one superpower though: They have dataset population built in.

- **LDBC-like**: A read-only graph workload, simulating the [LDBC SNB](https://ldbcouncil.org/benchmarks/snb/) benchmark.
- **TPC-B-like**: A write-heavy workload, simulating the [TPC B](http://tpc.org/tpcb/default5.asp) benchmark

Which should you use? If you are tuning for improving read load, use LDBC-like, if you're tuning for writes use TPC-B-like.

## Dataset population

Both workloads require a pre-existing dataset in place to run. 
Neobench ships with dataset populators for them.

You ask neobench to initialize the datasets by passing the `--init` flag.
You can optionally also set `--duration 0` to *only* run the dataset populator and not run any workload.

Both populators honor a `--scale <X>` setting, which is a multiplier/coefficient used to decide how big to make the dataset.
The `--scale <X>` setting used to populate must match the `--scale <X>` setting you give to run the workload later.
By default, `--scale` is set to `1`. 
Setting it to `2` will make the dataset roughly twice as large, setting it to `10` roughly 10x as large, and so on.

Example, populate the tpcb-like dataset with scale-factor-2, and then immediately exit.

    neobench \
      --address neo4j://localhost:7687 \
      --password secret \
      --builtin tpcb-like \
      --init \
      --scale 2 \
      --duration 0
      
## Running the builtin workloads

Note that the workloads are, again, just `Scripts` like any you define on your own.
This means they are subject to the same latency/throughput mode config as any other workload, and so on with all other flags configuring how to run the workload.

See the flags documentation in the [docs overview](overview.md).

### LDBC-like

Populate and run ldbc-like workload against db with scale-factor 1, for 10 minutes.
Workload will be single-threaded (`--clients 1` by default) and in throughput mode.

    neobench \
      --address neo4j://localhost:7687 \
      --password secret \
      --builtin ldbc-like \
      --init \
      --scale 1 \
      --duration 10m

### TPC-B-like

Populate and run tpc-b-like workload against db with scale-factor 1, for 10 minutes.
Workload will be single-threaded (`--clients 1` by default) and in throughput mode.

    neobench \
      --address neo4j://localhost:7687 \
      --password secret \
      --builtin tpcb-like \
      --init \
      --scale 1 \
      --duration 10m
