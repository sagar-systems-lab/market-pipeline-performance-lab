# Sample results

These numbers are one reference run from a Windows Release build with MSVC 19.44. They demonstrate report structure and expected relationships. They are not universal performance targets.

## Normal-load queue comparison

Scenario:

```text
requested ingress: 250,000 events/s
consumer capacity: 500,000 events/s
queue capacity:    65,536
policy:            block producer
```

Observed:

| Metric | Dynamic deque | Preallocated ring |
| --- | ---: | ---: |
| Observed ingress | 249,999.957/s | 249,999.953/s |
| Observed processing | 249,999.957/s | 249,999.953/s |
| Target attainment | 100.000% | 100.000% |
| Event-age p99 | 2.2 us | 2.2 us |
| Queue-residence p99 | 2.1 us | 2.1 us |
| Peak queue depth | 119 | 26 |
| Event-age max | 390.5 us | 49.3 us |

The measured p99 values were equal in this run. The different maximum values and queue peaks are observations from this run only; they are not used to claim a universal winner.

## Saturation sweep

The reference sweep used a 600,000 events/s consumer capacity.

| Target ingress | Observed ingress | Observed processing | Pressure |
| ---: | ---: | ---: | :---: |
| 250,000/s | 249,999.894/s | 249,999.894/s | no |
| 400,000/s | 399,999.904/s | 399,999.904/s | no |
| 550,000/s | 549,999.889/s | 549,999.623/s | no |
| 700,000/s | 613,947.068/s | 599,999.814/s | yes |

The supported conclusion is that the first observed pressure occurred at the 700k test step and the transition lies in the tested 550k-700k interval.

At pressured steps, the queue reached the configured high-water region and the producer was throttled by backpressure.

## Synthetic feed failover

Reference configuration:

```text
primary outage: 5,000 ms to 10,000 ms
stale threshold: 5 ms
recovery hold:   10 ms
```

Observed state evidence:

```text
feed switches:              2
primary stale transitions:  1
primary recoveries:         1
sequence regressions:       0
failover detection:         5,000 us
primary restore latency:   10,000 us
```

This is deliberately deterministic synthetic behavior. It demonstrates the state machine and reporting contract, not exchange-specific failover behavior.