#pragma once

#include "market_pipeline/scenario.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>

namespace market_pipeline {

struct PercentileSummary {
    std::size_t samples{};
    double p50_us{};
    double p95_us{};
    double p99_us{};
    double p999_us{};
    double max_us{};
};

struct FeedFailoverSummary {
    bool exercised{false};

    std::uint64_t primary_outage_start_ms{};
    std::uint64_t primary_outage_end_ms{};
    std::uint64_t recovery_hold_ms{};

    std::uint64_t raw_primary_observations{};
    std::uint64_t raw_backup_observations{};
    std::uint64_t forwarded_primary{};
    std::uint64_t forwarded_backup{};
    std::uint64_t suppressed_inactive_observations{};
    std::uint64_t selection_gap_updates{};
    std::uint64_t no_trusted_feed_updates{};

    std::uint64_t feed_switches{};
    std::uint64_t untrusted_transitions{};
    std::uint64_t primary_stale_transitions{};
    std::uint64_t primary_recoveries{};
    std::uint64_t sequence_regressions{};

    std::uint64_t failover_detection_us{};
    std::uint64_t primary_restore_latency_us{};
};

struct BenchmarkOptions {
    std::chrono::milliseconds warmup{500};
    std::chrono::milliseconds measurement_override{0};
    std::uint64_t sample_stride{64};
};

struct RunResult {
    double measurement_seconds{};
    double requested_average_ingress_rate{};
    double observed_ingress_rate{};
    double observed_processing_rate{};
    double target_attainment_pct{};
    bool generator_limited{};
    bool producer_throttled_by_backpressure{};

    std::uint64_t generated{};
    std::uint64_t enqueued{};
    std::uint64_t processed{};
    std::uint64_t dropped{};
    std::uint64_t coalesced{};
    std::uint64_t blocked_waits{};
    std::size_t peak_queue_depth{};

    PercentileSummary event_age{};
    PercentileSummary queue_residence{};
    FeedFailoverSummary feed_failover{};
};

class BurstSchedule {
public:
    explicit BurstSchedule(
        const ScenarioConfig& config
    );

    [[nodiscard]]
    bool active_at(
        std::chrono::nanoseconds elapsed
    ) const noexcept;

    [[nodiscard]]
    std::uint64_t target_rate_at(
        std::chrono::nanoseconds elapsed
    ) const noexcept;

    [[nodiscard]]
    long double expected_events(
        std::chrono::nanoseconds elapsed
    ) const noexcept;

private:
    std::uint64_t base_rate_;
    double multiplier_;
    std::uint32_t duration_ms_;
    double runtime_share_pct_;
};

[[nodiscard]]
RunResult run_benchmark(
    const ScenarioConfig& config,
    const BenchmarkOptions& options = {}
);

[[nodiscard]]
RunResult run_feed_failover_benchmark(
    const ScenarioConfig& config,
    const BenchmarkOptions& options = {}
);

}  // namespace market_pipeline