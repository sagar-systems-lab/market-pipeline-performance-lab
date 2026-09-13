# market-pipeline-performance-lab

[![CI](https://github.com/sagar-systems-lab/market-pipeline-performance-lab/actions/workflows/ci.yml/badge.svg)](https://github.com/sagar-systems-lab/market-pipeline-performance-lab/actions/workflows/ci.yml)
[![Sanitizers](https://github.com/sagar-systems-lab/market-pipeline-performance-lab/actions/workflows/sanitizers.yml/badge.svg)](https://github.com/sagar-systems-lab/market-pipeline-performance-lab/actions/workflows/sanitizers.yml)

A C++20 systems lab for studying market-data pipeline behavior under controlled synthetic load.

The project focuses on measurable behavior: throughput, queue pressure, tail latency, backpressure, feed freshness, failover, and saturation. Timed measurement is intentionally silent so terminal rendering and file I/O do not contaminate the hot path.

## What it covers

- normal, bursty, slow-consumer, failover, saturation, and custom workloads
- bounded queue behavior with block, drop-oldest, and coalesce-latest policies
- deterministic primary/backup feed arbitration
- stale-feed detection and recovery hold
- sequence and timestamp regression rejection
- silent latency sampling with p50, p95, p99, p99.9, and max
- saturation sweeps across increasing ingress targets
- neutral comparison of dynamic deque and preallocated ring backends
- JSON and CSV result artifacts
- cross-platform CI with warnings treated as errors
- Linux AddressSanitizer and UndefinedBehaviorSanitizer coverage

This is a synthetic performance lab, not a trading strategy, exchange connector, or production execution system.

## Build

Requirements:

- CMake 3.20+
- a C++20 compiler
- Ninja is optional

### Windows / MSVC

```powershell
cmake -S . -B build -G Ninja `
  -DCMAKE_BUILD_TYPE=Release `
  -DMPPL_WARNINGS_AS_ERRORS=ON

cmake --build build
ctest --test-dir build --output-on-failure
```

### Linux / GCC or Clang

```bash
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DMPPL_WARNINGS_AS_ERRORS=ON

cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

### Sanitized build

```bash
cmake -S . -B build-sanitized \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DMPPL_WARNINGS_AS_ERRORS=ON \
  -DMPPL_ENABLE_SANITIZERS=ON

cmake --build build-sanitized --parallel
ctest --test-dir build-sanitized --output-on-failure
```

## Run the lab

```text
market-pipeline-lab
```

The interactive runner exposes:

```text
[1] Normal traffic
[2] Bursty traffic
[3] Slow consumer
[4] Feed failover
[5] Saturation sweep
[6] Custom scenario
```

Configuration is frozen before measurement. After the pre-run status line, timed measurement remains silent. Reports are serialized only after the run completes.

Example:

```text
Benchmark running - timed measurement is intentionally silent. Please wait...

RUN COMPLETE
JSON: results/run-....json
CSV:  results/run-....csv
```

## Queue comparison

The comparison executable runs the same workload contract against two queue implementations:

```text
market-pipeline-compare
```

Backends:

- `dynamic_deque`
- `preallocated_ring`

The report records measured deltas without assuming a winner in advance.

## Evidence model

Each result separates requested load from observed behavior. Depending on the scenario, reports can include:

- requested and observed ingress rate
- observed processing rate
- target attainment
- generator-limited and backpressure state
- generated, enqueued, processed, dropped, and coalesced counts
- blocked producer waits
- peak queue depth
- event-age percentiles
- queue-residence percentiles
- feed switches and stale/recovery transitions
- failover and restore latency
- per-step saturation evidence

See [docs/methodology.md](docs/methodology.md) for measurement boundaries and [docs/sample-results.md](docs/sample-results.md) for one reference run.

## Tests and CI

The current test suite covers:

- project smoke behavior
- scenario validation
- interactive CLI contracts
- silent benchmark behavior
- feed arbitration
- synthetic feed failover
- saturation sweep
- queue backend comparison
- hostile and boundary conditions

CI builds with GCC, Clang, and MSVC. A separate Linux workflow runs the suite with AddressSanitizer and UndefinedBehaviorSanitizer.

## Interpreting results

Results are machine- and run-specific. They should be read as observations from a controlled synthetic experiment, not as universal latency or throughput claims.

A saturation sweep reports the first tested step that exhibits pressure. If one step is clean and the next is pressured, the useful conclusion is that the transition lies within that tested interval; the higher step is not claimed to be an exact hardware limit.

Single-run backend comparisons are evidence from that run only. They are not used to declare a universally faster data structure.

## Scope

The repository deliberately stays narrow:

- synthetic events only
- no exchange credentials or APIs
- no strategy or order logic
- no production deployment guidance
- no venue-specific tuning
- no claim that local benchmark numbers transfer to another host

The purpose is to make systems behavior observable and reproducible without presenting a production trading stack.

## License

MIT