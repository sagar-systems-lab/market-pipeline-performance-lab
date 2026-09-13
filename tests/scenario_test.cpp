#include "market_pipeline/scenario.hpp"

#include <iostream>
#include <string_view>

namespace {

int failures = 0;

void expect(
    const bool condition,
    const std::string_view message
) {
    if (!condition) {
        std::cerr
            << "FAIL: "
            << message
            << '\n';

        ++failures;
    }
}

}  // namespace

int main() {
    using market_pipeline::BackpressurePolicy;
    using market_pipeline::ScenarioConfig;
    using market_pipeline::WorkloadMode;

    const auto normal =
        market_pipeline::preset_for(
            WorkloadMode::Normal
        );

    expect(
        !market_pipeline::validate_scenario(normal)
             .has_value(),
        "normal preset should validate"
    );

    const auto burst =
        market_pipeline::preset_for(
            WorkloadMode::Bursty
        );

    expect(
        burst.burst.multiplier > 1.0,
        "bursty preset should configure a burst"
    );

    expect(
        burst.backpressure_policy ==
            BackpressurePolicy::CoalesceLatest,
        "bursty preset should expose coalescing"
    );

    const auto failover =
        market_pipeline::preset_for(
            WorkloadMode::FeedFailover
        );

    expect(
        failover.feed_count >= 2,
        "feed failover requires multiple feeds"
    );

    ScenarioConfig invalid{};

    invalid.feed_count = 0;

    expect(
        market_pipeline::validate_scenario(invalid)
            .has_value(),
        "zero feeds should be rejected"
    );

    invalid = ScenarioConfig{};
    invalid.high_watermark_pct = 101;

    expect(
        market_pipeline::validate_scenario(invalid)
            .has_value(),
        "high-water mark above 100 should be rejected"
    );

    invalid = ScenarioConfig{};
    invalid.consumer_capacity = 0;

    expect(
        market_pipeline::validate_scenario(invalid)
            .has_value(),
        "zero consumer capacity should be rejected"
    );

    invalid = ScenarioConfig{};
    invalid.burst.multiplier = 0.5;

    expect(
        market_pipeline::validate_scenario(invalid)
            .has_value(),
        "burst multiplier below 1 should be rejected"
    );

    return failures == 0 ? 0 : 1;
}