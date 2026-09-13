#include "market_pipeline/benchmark.hpp"

#include "market_pipeline/deque_queue.hpp"
#include "market_pipeline/event.hpp"
#include "market_pipeline/queue.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <limits>
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

struct PhaseResult {
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

template <typename Queue>
PhaseResult run_phase_impl(
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

    Queue queue{
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

    BurstSchedule schedule{config};

    std::thread producer{
        [&] {
            std::vector<std::uint64_t>
                sequences(
                    config.feed_count,
                    0
                );

            std::size_t feed_cursor =
                static_cast<std::size_t>(
                    config.seed %
                    config.feed_count
                );

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
                        std::chrono::
                            duration_cast<
                                std::chrono::
                                    nanoseconds
                            >(elapsed)
                    );

                credit +=
                    static_cast<long double>(
                        target_rate
                    ) *
                    delta_seconds;

                auto due =
                    static_cast<
                        std::uint64_t
                    >(credit);

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
                    MarketEvent event{};

                    event.feed_id =
                        static_cast<
                            std::uint32_t
                        >(feed_cursor);

                    event.sequence =
                        ++sequences[
                            feed_cursor
                        ];

                    event.generated_at =
                        SteadyClock::now();

                    feed_cursor =
                        (
                            feed_cursor +
                            1
                        ) %
                        config.feed_count;

                    counters.generated
                        .fetch_add(
                            1,
                            std::memory_order_relaxed
                        );

                    const auto outcome =
                        queue.push(
                            event,
                            stop_requested
                        );

                    if (outcome.stopped) {
                        break;
                    }

                    if (outcome.enqueued) {
                        counters.enqueued
                            .fetch_add(
                                1,
                                std::memory_order_relaxed
                            );
                    }

                    if (outcome.coalesced) {
                        counters.coalesced
                            .fetch_add(
                                1,
                                std::memory_order_relaxed
                            );
                    }

                    if (outcome.dropped) {
                        counters.dropped
                            .fetch_add(
                                1,
                                std::memory_order_relaxed
                            );
                    }

                    if (outcome.blocked) {
                        counters.blocked_waits
                            .fetch_add(
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
                    static_cast<
                        std::uint64_t
                    >(credit);

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

                    if (
                        !queue.try_pop(
                            event
                        )
                    ) {
                        queue_empty = true;
                        break;
                    }

                    const auto processed_at =
                        SteadyClock::now();

                    const auto
                        processed_count =
                            counters.processed
                                .fetch_add(
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
                            std::chrono::
                                duration_cast<
                                    std::chrono::
                                        nanoseconds
                                >(
                                    processed_at -
                                    event.generated_at
                                );

                        const auto residence =
                            std::chrono::
                                duration_cast<
                                    std::chrono::
                                        nanoseconds
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

                            queue_residence_ns
                                .push_back(
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

    start_time = SteadyClock::now();

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

    const auto snapshot =
        queue.snapshot();

    return PhaseResult{
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
        snapshot.peak_size,
        std::move(event_age_ns),
        std::move(queue_residence_ns)
    };
}

}  // namespace

BurstSchedule::BurstSchedule(
    const ScenarioConfig& config
)
    : base_rate_{
          config.base_ingress_rate
      },
      multiplier_{
          config.burst.multiplier
      },
      duration_ms_{
          config.burst.duration_ms
      },
      runtime_share_pct_{
          config.burst.runtime_share_pct
      } {
}

bool BurstSchedule::active_at(
    const std::chrono::nanoseconds elapsed
) const noexcept {
    if (
        multiplier_ <= 1.0 ||
        duration_ms_ == 0 ||
        runtime_share_pct_ <= 0.0
    ) {
        return false;
    }

    if (runtime_share_pct_ >= 100.0) {
        return true;
    }

    const long double share =
        static_cast<long double>(
            runtime_share_pct_
        ) /
        100.0L;

    const long double cycle_ns =
        static_cast<long double>(
            duration_ms_
        ) *
        1'000'000.0L /
        share;

    const long double position =
        std::fmod(
            static_cast<long double>(
                elapsed.count()
            ),
            cycle_ns
        );

    return
        position <
        static_cast<long double>(
            duration_ms_
        ) *
            1'000'000.0L;
}

std::uint64_t BurstSchedule::target_rate_at(
    const std::chrono::nanoseconds elapsed
) const noexcept {
    if (!active_at(elapsed)) {
        return base_rate_;
    }

    const long double scaled =
        static_cast<long double>(
            base_rate_
        ) *
        static_cast<long double>(
            multiplier_
        );

    const long double maximum =
        static_cast<long double>(
            std::numeric_limits<
                std::uint64_t
            >::max()
        );

    if (scaled >= maximum) {
        return
            std::numeric_limits<
                std::uint64_t
            >::max();
    }

    return static_cast<std::uint64_t>(
        scaled
    );
}

long double BurstSchedule::expected_events(
    const std::chrono::nanoseconds elapsed
) const noexcept {
    const long double total_ns =
        static_cast<long double>(
            elapsed.count()
        );

    if (total_ns <= 0.0L) {
        return 0.0L;
    }

    if (
        multiplier_ <= 1.0 ||
        duration_ms_ == 0 ||
        runtime_share_pct_ <= 0.0
    ) {
        return
            static_cast<long double>(
                base_rate_
            ) *
            total_ns /
            1'000'000'000.0L;
    }

    if (runtime_share_pct_ >= 100.0) {
        return
            static_cast<long double>(
                base_rate_
            ) *
            static_cast<long double>(
                multiplier_
            ) *
            total_ns /
            1'000'000'000.0L;
    }

    const long double burst_ns =
        static_cast<long double>(
            duration_ms_
        ) *
        1'000'000.0L;

    const long double share =
        static_cast<long double>(
            runtime_share_pct_
        ) /
        100.0L;

    const long double cycle_ns =
        burst_ns /
        share;

    const auto full_cycles =
        static_cast<std::uint64_t>(
            total_ns /
            cycle_ns
        );

    const long double remainder_ns =
        total_ns -
        static_cast<long double>(
            full_cycles
        ) *
            cycle_ns;

    const long double total_burst_ns =
        static_cast<long double>(
            full_cycles
        ) *
            burst_ns +
        std::min(
            remainder_ns,
            burst_ns
        );

    const long double normal_ns =
        total_ns -
        total_burst_ns;

    const long double base =
        static_cast<long double>(
            base_rate_
        );

    return (
        normal_ns *
            base +
        total_burst_ns *
            base *
            static_cast<long double>(
                multiplier_
            )
    ) /
        1'000'000'000.0L;
}

RunResult run_benchmark(
    const ScenarioConfig& config,
    const BenchmarkOptions& options
) {
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

    if (
        config.mode ==
        WorkloadMode::SaturationSweep
    ) {
        return run_saturation_sweep_benchmark(
            config,
            options
        );
    }

    if (
        config.mode ==
        WorkloadMode::FeedFailover
    ) {
        return run_feed_failover_benchmark(
            config,
            options
        );
    }

    if (options.warmup.count() > 0) {
        static_cast<void>(
            run_phase_impl<BoundedEventQueue>(
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
        run_phase_impl<BoundedEventQueue>(
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

    const long double
        requested_events =
            schedule.expected_events(
                elapsed_ns
            );

    const double
        requested_average_rate =
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

    const double
        observed_processing_rate =
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
        )
    };
}


RunResult run_benchmark_with_backend(
    const ScenarioConfig& config,
    const QueueBackend backend,
    const BenchmarkOptions& options
) {
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

    const auto run_phase_for_backend =
        [&](const std::chrono::milliseconds duration,
            const bool collect_samples) {
            switch (backend) {
            case QueueBackend::DynamicDeque:
                return
                    run_phase_impl<
                        DequeEventQueue
                    >(
                        config,
                        duration,
                        options.sample_stride,
                        collect_samples
                    );

            case QueueBackend::PreallocatedRing:
                return
                    run_phase_impl<
                        BoundedEventQueue
                    >(
                        config,
                        duration,
                        options.sample_stride,
                        collect_samples
                    );
            }

            throw std::invalid_argument(
                "unknown queue backend"
            );
        };

    if (options.warmup.count() > 0) {
        static_cast<void>(
            run_phase_for_backend(
                options.warmup,
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
        run_phase_for_backend(
            measurement,
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
        )
    };
}

std::string_view to_string(
    const QueueBackend backend
) noexcept {
    switch (backend) {
    case QueueBackend::DynamicDeque:
        return "dynamic_deque";

    case QueueBackend::PreallocatedRing:
        return "preallocated_ring";
    }

    return "unknown";
}

}  // namespace market_pipeline
