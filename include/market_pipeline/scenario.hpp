#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace market_pipeline {

enum class WorkloadMode {
    Normal,
    Bursty,
    SlowConsumer,
    FeedFailover,
    SaturationSweep,
    Custom
};

enum class BackpressurePolicy {
    BlockProducer,
    DropOldest,
    CoalesceLatest
};

struct BurstConfig {
    double multiplier{1.0};
    std::uint32_t duration_ms{0};
    double runtime_share_pct{0.0};
};

struct ScenarioConfig {
    WorkloadMode mode{WorkloadMode::Normal};

    std::size_t feed_count{2};

    std::uint64_t base_ingress_rate{250'000};
    std::uint64_t consumer_capacity{500'000};

    std::size_t queue_capacity{65'536};
    std::uint32_t high_watermark_pct{80};

    BurstConfig burst{};

    std::uint32_t stale_threshold_ms{5};
    BackpressurePolicy backpressure_policy{
        BackpressurePolicy::BlockProducer
    };

    std::uint32_t run_duration_seconds{15};
    std::uint64_t seed{1337};
};

[[nodiscard]]
ScenarioConfig preset_for(WorkloadMode mode);

[[nodiscard]]
std::optional<std::string> validate_scenario(
    const ScenarioConfig& config
);

[[nodiscard]]
std::string_view to_string(WorkloadMode mode) noexcept;

[[nodiscard]]
std::string_view to_string(BackpressurePolicy policy) noexcept;

}  // namespace market_pipeline