#include "market_pipeline/benchmark.hpp"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>

namespace market_pipeline {
namespace {

constexpr std::size_t kSweepSteps = 8;

std::uint64_t saturating_add(
    const std::uint64_t left,
    const std::uint64_t right
) noexcept {
    const auto maximum =
        std::numeric_limits<
            std::uint64_t
        >::max();

    if (right > maximum - left) {
        return maximum;
    }

    return left + right;
}

std::vector<std::uint64_t>
make_targets(
    const ScenarioConfig& config
) {
    std::vector<std::uint64_t> targets;
    targets.reserve(kSweepSteps);

    const auto increment =
        std::max<std::uint64_t>(
            1,
            config.consumer_capacity / 4
        );

    auto target =
        config.base_ingress_rate;

    for (
        std::size_t index = 0;
        index < kSweepSteps;
        ++index
    ) {
        targets.push_back(target);

        target =
            saturating_add(
                target,
                increment
            );
    }

    return targets;
}

bool pressure_observed(
    const RunResult& result
) noexcept {
    return
        result.generator_limited ||
        result.
            producer_throttled_by_backpressure ||
        result.dropped > 0 ||
        result.coalesced > 0 ||
        result.blocked_waits > 0;
}

}  // namespace

RunResult run_saturation_sweep_benchmark(
    const ScenarioConfig& config,
    const BenchmarkOptions& options
) {
    if (
        config.mode !=
        WorkloadMode::SaturationSweep
    ) {
        throw std::invalid_argument(
            "saturation sweep requires "
            "SaturationSweep mode"
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

    const auto targets =
        make_targets(config);

    const auto default_step_duration =
        std::max<std::uint64_t>(
            250,
            (
                static_cast<std::uint64_t>(
                    config.run_duration_seconds
                ) *
                1000U
            ) /
                static_cast<std::uint64_t>(
                    kSweepSteps
                )
        );

    const auto step_duration =
        options.measurement_override.count() >
                0
            ? options.measurement_override
            : std::chrono::milliseconds{
                  static_cast<
                      std::chrono::milliseconds::rep
                  >(default_step_duration)
              };

    SaturationSweepSummary summary{};
    summary.exercised = true;
    summary.steps.reserve(
        targets.size()
    );

    RunResult final_step{};

    for (
        std::size_t index = 0;
        index < targets.size();
        ++index
    ) {
        ScenarioConfig step_config =
            config;

        step_config.mode =
            WorkloadMode::Normal;

        step_config.base_ingress_rate =
            targets[index];

        step_config.burst.multiplier = 1.0;
        step_config.burst.duration_ms = 0;
        step_config.burst.runtime_share_pct =
            0.0;

        BenchmarkOptions step_options =
            options;

        step_options.measurement_override =
            step_duration;

        const auto step_result =
            run_benchmark(
                step_config,
                step_options
            );

        const bool pressured =
            pressure_observed(
                step_result
            );

        if (
            pressured &&
            summary.first_pressure_step < 0
        ) {
            summary.first_pressure_step =
                static_cast<std::int64_t>(
                    index
                );

            summary.
                first_pressure_target_rate =
                    targets[index];
        }

        summary.steps.push_back(
            SaturationStepResult{
                index,
                targets[index],
                step_result.
                    measurement_seconds,
                step_result.
                    observed_ingress_rate,
                step_result.
                    observed_processing_rate,
                step_result.
                    target_attainment_pct,
                step_result.
                    generator_limited,
                step_result.
                    producer_throttled_by_backpressure,
                pressured,
                step_result.generated,
                step_result.processed,
                step_result.dropped,
                step_result.coalesced,
                step_result.blocked_waits,
                step_result.
                    peak_queue_depth,
                step_result.event_age,
                step_result.queue_residence
            }
        );

        final_step = step_result;
    }

    final_step.saturation_sweep =
        std::move(summary);

    return final_step;
}

}  // namespace market_pipeline