#include "market_pipeline/scenario.hpp"

#include <cmath>

namespace market_pipeline {

ScenarioConfig preset_for(const WorkloadMode mode) {
    ScenarioConfig config{};
    config.mode = mode;

    switch (mode) {
    case WorkloadMode::Normal:
        break;

    case WorkloadMode::Bursty:
        config.base_ingress_rate = 250'000;
        config.consumer_capacity = 600'000;
        config.burst.multiplier = 4.0;
        config.burst.duration_ms = 150;
        config.burst.runtime_share_pct = 20.0;
        config.backpressure_policy =
            BackpressurePolicy::CoalesceLatest;
        break;

    case WorkloadMode::SlowConsumer:
        config.base_ingress_rate = 500'000;
        config.consumer_capacity = 250'000;
        config.high_watermark_pct = 70;
        config.backpressure_policy =
            BackpressurePolicy::BlockProducer;
        break;

    case WorkloadMode::FeedFailover:
        config.feed_count = 2;
        config.base_ingress_rate = 250'000;
        config.consumer_capacity = 500'000;
        config.stale_threshold_ms = 5;
        break;

    case WorkloadMode::SaturationSweep:
        config.base_ingress_rate = 250'000;
        config.consumer_capacity = 600'000;
        config.run_duration_seconds = 30;
        break;

    case WorkloadMode::Custom:
        break;
    }

    return config;
}

std::optional<std::string> validate_scenario(
    const ScenarioConfig& config
) {
    if (config.feed_count == 0) {
        return "feed count must be greater than zero";
    }

    if (config.base_ingress_rate == 0) {
        return "base ingress rate must be greater than zero";
    }

    if (config.consumer_capacity == 0) {
        return "consumer capacity must be greater than zero";
    }

    if (config.queue_capacity == 0) {
        return "queue capacity must be greater than zero";
    }

    if (
        config.high_watermark_pct == 0 ||
        config.high_watermark_pct > 100
    ) {
        return "high-water mark must be between 1 and 100 percent";
    }

    if (
        !std::isfinite(config.burst.multiplier) ||
        config.burst.multiplier < 1.0
    ) {
        return "burst multiplier must be finite and at least 1.0";
    }

    if (
        !std::isfinite(config.burst.runtime_share_pct) ||
        config.burst.runtime_share_pct < 0.0 ||
        config.burst.runtime_share_pct > 100.0
    ) {
        return "burst runtime share must be finite and between 0 and 100 percent";
    }

    if (
        config.burst.multiplier > 1.0 &&
        config.burst.duration_ms == 0
    ) {
        return "active burst requires a non-zero duration";
    }

    if (
        config.burst.multiplier > 1.0 &&
        config.burst.runtime_share_pct == 0.0
    ) {
        return "active burst requires a non-zero runtime share";
    }

    if (
        config.mode == WorkloadMode::FeedFailover &&
        config.feed_count < 2
    ) {
        return "feed failover requires at least two feeds";
    }

    if (config.stale_threshold_ms == 0) {
        return "stale threshold must be greater than zero";
    }

    if (config.run_duration_seconds == 0) {
        return "run duration must be greater than zero";
    }

    return std::nullopt;
}

std::string_view to_string(const WorkloadMode mode) noexcept {
    switch (mode) {
    case WorkloadMode::Normal:
        return "Normal traffic";
    case WorkloadMode::Bursty:
        return "Bursty traffic";
    case WorkloadMode::SlowConsumer:
        return "Slow consumer";
    case WorkloadMode::FeedFailover:
        return "Feed failover";
    case WorkloadMode::SaturationSweep:
        return "Saturation sweep";
    case WorkloadMode::Custom:
        return "Custom scenario";
    }

    return "Unknown";
}

std::string_view to_string(
    const BackpressurePolicy policy
) noexcept {
    switch (policy) {
    case BackpressurePolicy::BlockProducer:
        return "Block producer";
    case BackpressurePolicy::DropOldest:
        return "Drop oldest";
    case BackpressurePolicy::CoalesceLatest:
        return "Coalesce latest";
    }

    return "Unknown";
}

}  // namespace market_pipeline