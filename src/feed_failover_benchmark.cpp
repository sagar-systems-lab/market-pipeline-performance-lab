#include "market_pipeline/benchmark.hpp"

#include "market_pipeline/event.hpp"
#include "market_pipeline/feed_arbitration.hpp"
#include "market_pipeline/failover_workload.hpp"
#include "market_pipeline/queue.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

namespace market_pipeline {
namespace {

struct PhaseCounters {
    std::atomic<std::uint64_t> generated{0};
    std::atomic<std::uint64_t> enqueued{0};
    std::atomic<std::uint64_t> processed{0};
    std::atomic<std::uint64_t> dropped{0};
    std::atomic<std::uint64_t> coalesced{0};
    std::atomic<std::uint64_t> blocked_waits{0};
};

struct FailoverPhaseResult {
    double elapsed_seconds{};
    std::uint64_t generated{};
    std::uint64_t enqueued{};
    std::uint64_t processed{};
    std::uint64_t dropped{};
    std::uint64_t coalesced{};
    std::uint64_t blocked_waits{};
    std::size_t peak_queue_depth{};
    std::vector<std::uint64_t> event_age_ns;
    std::vector<std::uint64_t> queue_residence_ns;
    FeedFailoverSummary failover{};
};

PercentileSummary summarize(
    std::vector<std::uint64_t> values
) {
    if (values.empty()) {
        return {};
    }

    std::sort(
        values.begin(),
        values.end()
    );

    const auto percentile =
        [&](const double q) {
            const double position =
                q *
                static_cast<double>(
                    values.size() - 1
                );

            const auto lower =
                static_cast<std::size_t>(
                    std::floor(position)
                );

            const auto upper =
                static_cast<std::size_t>(
                    std::ceil(position)
                );

            const double fraction =
                position -
                static_cast<double>(lower);

            const double lower_value =
                static_cast<double>(
                    values[lower]
                );

            const double upper_value =
                static_cast<double>(
                    values[upper]
                );

            return (
                lower_value +
                (
                    upper_value -
                    lower_value
                ) *
                    fraction
            ) /
                1000.0;
        };

    return PercentileSummary{
        values.size(),
        percentile(0.50),
        percentile(0.95),
        percentile(0.99),
        percentile(0.999),
        static_cast<double>(
            values.back()
        ) /
            1000.0
    };
}

std::uint64_t positive_microseconds(
    const SteadyClock::duration value
) {
    const auto micros =
        std::chrono::duration_cast<
            std::chrono::microseconds
        >(value).count();

    if (micros <= 0) {
        return 0;
    }

    return static_cast<std::uint64_t>(
        micros
    );
}

std::uint64_t nonnegative_milliseconds(
    const std::chrono::nanoseconds value
) {
    const auto millis =
        std::chrono::duration_cast<
            std::chrono::milliseconds
        >(value).count();

    if (millis <= 0) {
        return 0;
    }

    return static_cast<std::uint64_t>(
        millis
    );
}

FailoverPhaseResult run_failover_phase(
    const ScenarioConfig& config,
    const std::chrono::milliseconds duration,
    const std::uint64_t sample_stride,
    const bool collect_samples
) {
    if (duration.count() <= 0) {
        throw std::invalid_argument(
            "benchmark phase duration must be positive"
        );
    }

    if (sample_stride == 0) {
        throw std::invalid_argument(
            "sample stride must be positive"
        );
    }

    const auto stale_threshold =
        std::chrono::milliseconds{
            config.stale_threshold_ms
        };

    const auto plan =
        make_failover_plan(
            duration,
            stale_threshold
        );

    BoundedEventQueue queue{
        config.queue_capacity,
        config.high_watermark_pct,
        config.backpressure_policy
    };

    PhaseCounters counters{};
    std::atomic_bool stop_requested{false};
    std::atomic_bool start_requested{false};
    std::atomic_uint32_t ready_threads{0};

    SteadyClock::time_point start_time{};

    std::vector<std::uint64_t> event_age_ns;
    std::vector<std::uint64_t> queue_residence_ns;

    std::size_t sample_capacity = 0;
    std::uint64_t effective_sample_stride =
        sample_stride;

    if (collect_samples) {
        constexpr std::size_t kMaximumSamples =
            1'000'000;

        const long double duration_seconds =
            static_cast<long double>(
                duration.count()
            ) /
            1000.0L;

        const long double expected_processed =
            static_cast<long double>(
                config.consumer_capacity
            ) *
            duration_seconds;

        const long double
            samples_at_requested_stride =
                expected_processed /
                static_cast<long double>(
                    sample_stride
                );

        if (
            samples_at_requested_stride >
            static_cast<long double>(
                kMaximumSamples
            )
        ) {
            effective_sample_stride =
                static_cast<std::uint64_t>(
                    std::ceil(
                        expected_processed /
                        static_cast<long double>(
                            kMaximumSamples
                        )
                    )
                );
        }

        const long double expected_samples =
            expected_processed /
                static_cast<long double>(
                    effective_sample_stride
                ) +
            1024.0L;

        sample_capacity =
            static_cast<std::size_t>(
                std::min<long double>(
                    expected_samples,
                    static_cast<long double>(
                        kMaximumSamples
                    )
                )
            );

        sample_capacity =
            std::max<std::size_t>(
                sample_capacity,
                1024
            );

        event_age_ns.reserve(
            sample_capacity
        );

        queue_residence_ns.reserve(
            sample_capacity
        );
    }

    FeedFailoverSummary failover{};

    failover.exercised = true;
    failover.primary_outage_start_ms =
        nonnegative_milliseconds(
            plan.primary_outage_start
        );
    failover.primary_outage_end_ms =
        nonnegative_milliseconds(
            plan.primary_outage_end
        );

    const auto recovery_hold_count =
        plan.recovery_hold.count();

    if (recovery_hold_count > 0) {
        failover.recovery_hold_ms =
            static_cast<std::uint64_t>(
                recovery_hold_count
            );
    }

    BurstSchedule schedule{config};

    std::thread producer{
        [&] {
            FeedArbiter arbiter{
                config.feed_count,
                stale_threshold,
                plan.recovery_hold,
                0
            };

            std::uint64_t primary_sequence = 0;
            std::uint64_t backup_sequence = 0;
            long double credit = 0.0L;

            ready_threads.fetch_add(
                1,
                std::memory_order_release
            );

            while (
                !start_requested.load(
                    std::memory_order_acquire
                )
            ) {
                std::this_thread::yield();
            }

            auto last_budget_time =
                start_time;

            while (
                !stop_requested.load(
                    std::memory_order_relaxed
                )
            ) {
                const auto now =
                    SteadyClock::now();

                const auto elapsed =
                    now - start_time;

                const auto elapsed_ns =
                    std::chrono::duration_cast<
                        std::chrono::nanoseconds
                    >(elapsed);

                const long double delta_seconds =
                    std::chrono::duration<
                        long double
                    >(
                        now -
                        last_budget_time
                    ).count();

                last_budget_time = now;

                const auto target_rate =
                    schedule.target_rate_at(
                        elapsed_ns
                    );

                credit +=
                    static_cast<long double>(
                        target_rate
                    ) *
                    delta_seconds;

                auto due =
                    static_cast<std::uint64_t>(
                        credit
                    );

                if (due == 0) {
                    std::this_thread::yield();
                    continue;
                }

                credit -=
                    static_cast<long double>(
                        due
                    );

                while (
                    due > 0 &&
                    !stop_requested.load(
                        std::memory_order_relaxed
                    )
                ) {
                    const auto observation_time =
                        SteadyClock::now();

                    const auto logical_elapsed =
                        std::chrono::duration_cast<
                            std::chrono::nanoseconds
                        >(
                            observation_time -
                            start_time
                        );

                    counters.generated.fetch_add(
                        1,
                        std::memory_order_relaxed
                    );

                    const bool primary_observed =
                        plan.primary_available(
                            logical_elapsed
                        );

                    MarketEvent primary_event{};
                    MarketEvent backup_event{};

                    if (primary_observed) {
                        primary_event.feed_id = 0;
                        primary_event.sequence =
                            ++primary_sequence;
                        primary_event.generated_at =
                            observation_time;

                        static_cast<void>(
                            arbiter.observe(
                                0,
                                primary_event.sequence,
                                observation_time
                            )
                        );

                        ++failover.
                            raw_primary_observations;
                    }

                    backup_event.feed_id = 1;
                    backup_event.sequence =
                        ++backup_sequence;
                    backup_event.generated_at =
                        observation_time;

                    static_cast<void>(
                        arbiter.observe(
                            1,
                            backup_event.sequence,
                            observation_time
                        )
                    );

                    ++failover.raw_backup_observations;

                    const auto arbitration =
                        arbiter.snapshot(
                            observation_time
                        );

                    MarketEvent selected_event{};
                    bool forward = false;

                    if (
                        arbitration.active_feed &&
                        *arbitration.active_feed == 0 &&
                        primary_observed
                    ) {
                        selected_event =
                            primary_event;
                        forward = true;
                        ++failover.forwarded_primary;

                        if (
                            failover.
                                    failover_detection_us >
                                0 &&
                            failover.
                                    primary_restore_latency_us ==
                                0 &&
                            logical_elapsed >=
                                plan.primary_outage_end
                        ) {
                            failover.
                                primary_restore_latency_us =
                                positive_microseconds(
                                    observation_time -
                                    (
                                        start_time +
                                        plan.
                                            primary_outage_end
                                    )
                                );
                        }
                    } else if (
                        arbitration.active_feed &&
                        *arbitration.active_feed == 1
                    ) {
                        selected_event =
                            backup_event;
                        forward = true;
                        ++failover.forwarded_backup;

                        if (
                            failover.
                                    failover_detection_us ==
                                0 &&
                            logical_elapsed >=
                                plan.primary_outage_start
                        ) {
                            failover.
                                failover_detection_us =
                                positive_microseconds(
                                    observation_time -
                                    (
                                        start_time +
                                        plan.
                                            primary_outage_start
                                    )
                                );
                        }
                    }

                    const std::uint64_t
                        observations_this_update =
                            primary_observed
                                ? 2U
                                : 1U;

                    if (forward) {
                        failover.
                            suppressed_inactive_observations +=
                            observations_this_update -
                            1U;
                    } else {
                        failover.
                            suppressed_inactive_observations +=
                            observations_this_update;

                        ++failover.
                            selection_gap_updates;

                        if (
                            !arbitration.active_feed
                        ) {
                            ++failover.
                                no_trusted_feed_updates;
                        }

                        --due;
                        continue;
                    }

                    const auto outcome =
                        queue.push(
                            selected_event,
                            stop_requested
                        );

                    if (outcome.stopped) {
                        break;
                    }

                    if (outcome.enqueued) {
                        counters.enqueued.fetch_add(
                            1,
                            std::memory_order_relaxed
                        );
                    }

                    if (outcome.coalesced) {
                        counters.coalesced.fetch_add(
                            1,
                            std::memory_order_relaxed
                        );
                    }

                    if (outcome.dropped) {
                        counters.dropped.fetch_add(
                            1,
                            std::memory_order_relaxed
                        );
                    }

                    if (outcome.blocked) {
                        counters.blocked_waits.fetch_add(
                            1,
                            std::memory_order_relaxed
                        );

                        credit = 0.0L;
                        last_budget_time =
                            SteadyClock::now();

                        break;
                    }

                    --due;
                }
            }

            const auto final_snapshot =
                arbiter.snapshot(
                    SteadyClock::now()
                );

            failover.feed_switches =
                final_snapshot.feed_switches;

            failover.untrusted_transitions =
                final_snapshot.
                    untrusted_transitions;

            failover.primary_stale_transitions =
                final_snapshot.
                    primary_stale_transitions;

            failover.primary_recoveries =
                final_snapshot.
                    primary_recoveries;

            failover.sequence_regressions =
                final_snapshot.
                    sequence_regressions;
        }
    };

    std::thread consumer{
        [&] {
            long double credit = 0.0L;

            ready_threads.fetch_add(
                1,
                std::memory_order_release
            );

            while (
                !start_requested.load(
                    std::memory_order_acquire
                )
            ) {
                std::this_thread::yield();
            }

            auto last_budget_time =
                start_time;

            while (
                !stop_requested.load(
                    std::memory_order_relaxed
                )
            ) {
                const auto now =
                    SteadyClock::now();

                const long double delta_seconds =
                    std::chrono::duration<
                        long double
                    >(
                        now -
                        last_budget_time
                    ).count();

                last_budget_time = now;

                credit +=
                    static_cast<long double>(
                        config.consumer_capacity
                    ) *
                    delta_seconds;

                auto due =
                    static_cast<std::uint64_t>(
                        credit
                    );

                if (due == 0) {
                    std::this_thread::yield();
                    continue;
                }

                credit -=
                    static_cast<long double>(
                        due
                    );

                bool queue_empty = false;

                while (
                    due > 0 &&
                    !stop_requested.load(
                        std::memory_order_relaxed
                    )
                ) {
                    MarketEvent event{};

                    if (!queue.try_pop(event)) {
                        queue_empty = true;
                        break;
                    }

                    const auto processed_at =
                        SteadyClock::now();

                    const auto processed_count =
                        counters.processed.fetch_add(
                            1,
                            std::memory_order_relaxed
                        ) +
                        1;

                    if (
                        collect_samples &&
                        processed_count %
                                effective_sample_stride ==
                            0 &&
                        event_age_ns.size() <
                            sample_capacity
                    ) {
                        const auto age =
                            std::chrono::duration_cast<
                                std::chrono::nanoseconds
                            >(
                                processed_at -
                                event.generated_at
                            );

                        const auto residence =
                            std::chrono::duration_cast<
                                std::chrono::nanoseconds
                            >(
                                processed_at -
                                event.enqueued_at
                            );

                        if (
                            age.count() >= 0 &&
                            residence.count() >= 0
                        ) {
                            event_age_ns.push_back(
                                static_cast<
                                    std::uint64_t
                                >(age.count())
                            );

                            queue_residence_ns.
                                push_back(
                                    static_cast<
                                        std::uint64_t
                                    >(
                                        residence.count()
                                    )
                                );
                        }
                    }

                    --due;
                }

                if (queue_empty) {
                    credit = 0.0L;
                }
            }
        }
    };

    while (
        ready_threads.load(
            std::memory_order_acquire
        ) != 2U
    ) {
        std::this_thread::yield();
    }

    start_time =
        SteadyClock::now();

    start_requested.store(
        true,
        std::memory_order_release
    );

    const auto deadline =
        start_time +
        duration;

    std::this_thread::sleep_until(
        deadline
    );

    const auto stop_time =
        SteadyClock::now();

    stop_requested.store(
        true,
        std::memory_order_relaxed
    );

    queue.wake_all();

    producer.join();
    consumer.join();

    const auto queue_snapshot =
        queue.snapshot();

    return FailoverPhaseResult{
        std::chrono::duration<double>(
            stop_time -
            start_time
        ).count(),
        counters.generated.load(
            std::memory_order_relaxed
        ),
        counters.enqueued.load(
            std::memory_order_relaxed
        ),
        counters.processed.load(
            std::memory_order_relaxed
        ),
        counters.dropped.load(
            std::memory_order_relaxed
        ),
        counters.coalesced.load(
            std::memory_order_relaxed
        ),
        counters.blocked_waits.load(
            std::memory_order_relaxed
        ),
        queue_snapshot.peak_size,
        std::move(event_age_ns),
        std::move(queue_residence_ns),
        failover
    };
}

}  // namespace

RunResult run_feed_failover_benchmark(
    const ScenarioConfig& config,
    const BenchmarkOptions& options
) {
    if (
        config.mode !=
        WorkloadMode::FeedFailover
    ) {
        throw std::invalid_argument(
            "feed failover benchmark requires "
            "FeedFailover mode"
        );
    }

    if (
        const auto error =
            validate_scenario(config);
        error.has_value()
    ) {
        throw std::invalid_argument(
            "invalid scenario: " +
            error.value()
        );
    }

    if (options.sample_stride == 0) {
        throw std::invalid_argument(
            "sample stride must be positive"
        );
    }

    if (options.warmup.count() > 0) {
        static_cast<void>(
            run_failover_phase(
                config,
                options.warmup,
                options.sample_stride,
                false
            )
        );
    }

    const auto measurement =
        options.measurement_override.count() >
                0
            ? options.measurement_override
            : std::chrono::duration_cast<
                  std::chrono::milliseconds
              >(
                  std::chrono::seconds{
                      config.run_duration_seconds
                  }
              );

    auto phase =
        run_failover_phase(
            config,
            measurement,
            options.sample_stride,
            true
        );

    BurstSchedule schedule{config};

    const auto elapsed_ns =
        std::chrono::duration_cast<
            std::chrono::nanoseconds
        >(
            std::chrono::duration<double>{
                phase.elapsed_seconds
            }
        );

    const long double requested_events =
        schedule.expected_events(
            elapsed_ns
        );

    const double requested_average_rate =
        phase.elapsed_seconds > 0.0
            ? static_cast<double>(
                  requested_events /
                  phase.elapsed_seconds
              )
            : 0.0;

    const double observed_ingress_rate =
        phase.elapsed_seconds > 0.0
            ? static_cast<double>(
                  phase.generated
              ) /
                  phase.elapsed_seconds
            : 0.0;

    const double observed_processing_rate =
        phase.elapsed_seconds > 0.0
            ? static_cast<double>(
                  phase.processed
              ) /
                  phase.elapsed_seconds
            : 0.0;

    const double target_attainment_pct =
        requested_average_rate > 0.0
            ? observed_ingress_rate *
                  100.0 /
                  requested_average_rate
            : 0.0;

    const bool producer_throttled =
        phase.blocked_waits > 0;

    const bool generator_limited =
        !producer_throttled &&
        target_attainment_pct < 95.0;

    return RunResult{
        phase.elapsed_seconds,
        requested_average_rate,
        observed_ingress_rate,
        observed_processing_rate,
        target_attainment_pct,
        generator_limited,
        producer_throttled,
        phase.generated,
        phase.enqueued,
        phase.processed,
        phase.dropped,
        phase.coalesced,
        phase.blocked_waits,
        phase.peak_queue_depth,
        summarize(
            std::move(
                phase.event_age_ns
            )
        ),
        summarize(
            std::move(
                phase.queue_residence_ns
            )
        ),
        phase.failover
    };
}

}  // namespace market_pipeline