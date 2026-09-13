# Measurement methodology

This document describes the public measurement contract used by the lab. It intentionally stays at the experiment level rather than prescribing production tuning or deployment decisions.

## Measurement boundary

Each benchmark has a setup phase, an optional warm-up phase, a timed measurement phase, and a reporting phase.

The timed phase excludes terminal rendering and report serialization. The runner prints a status line before measurement and produces result paths only after measurement has finished.

## Clocking and latency

Latency timestamps use `std::chrono::steady_clock`.

Two latency views are reported:

- **event age**: time from synthetic generation to processing
- **queue residence**: time from enqueue to processing

Percentiles are calculated after the timed phase. Sampling uses a configurable stride and a bounded sample budget so measurement storage does not grow without limit.

## Requested vs observed load

Requested ingress and observed ingress are reported separately.

A run does not rewrite an unattained request into an achieved rate. The report also distinguishes generator limitation from producer throttling caused by backpressure.

## Queue pressure

The benchmark records queue depth and policy outcomes. Depending on the selected policy this can include blocked producer waits, dropped events, or coalesced updates.

A high-water mark is part of the queue contract. Reaching that mark is observable evidence of pressure; it is not presented as a diagnosis of an external system.

## Feed failover

The failover workload uses two synthetic feeds. The primary feed is intentionally silenced for a deterministic interval while the backup continues.

The public evidence records state transitions such as:

- primary becoming stale
- selection moving to the backup
- primary recovery
- recovery hold completion
- return to the primary
- rejected sequence or timestamp regressions

No exchange-specific arbitration rules are modeled.

## Saturation sweep

The saturation mode runs a fixed sequence of increasing synthetic ingress targets. Each step is measured independently.

The report identifies the first tested step where pressure is observed through the benchmark's explicit signals, such as backpressure or inability to sustain the target.

This is an interval-finding experiment. If 550k events/s is clean and 700k events/s is pressured, the supported statement is that the transition occurred somewhere in that tested interval. It is not evidence that 700k is an exact machine limit.

## Queue comparison

The queue comparison runs the same scenario against:

- a dynamically growing `std::deque`-based queue
- a preallocated bounded ring implementation

Execution order is selected from the deterministic scenario seed. Results are reported neutrally. No backend is hard-coded as the expected winner.

A single comparison run is useful evidence, but not a universal performance conclusion.

## Reproducibility limits

This is a user-space synthetic benchmark. Results can move with:

- OS scheduling
- compiler and optimization settings
- CPU power state and background load
- timer behavior
- host topology
- concurrent processes

For that reason, reports include basic build and environment metadata and avoid universal latency claims.

## Validation

The project is compiled with strict warnings in CI across GCC, Clang, and MSVC. The test suite also runs under AddressSanitizer and UndefinedBehaviorSanitizer on Linux.

Boundary tests cover invalid configuration, queue policy invariants, shutdown behavior, and fail-closed handling of feed regressions.