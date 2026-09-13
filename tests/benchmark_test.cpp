#include "market_pipeline/benchmark.hpp"
#include "market_pipeline/queue.hpp"
#include "market_pipeline/report.hpp"
#include "market_pipeline/scenario.hpp"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <iostream>

int main() {
    using namespace std::chrono_literals;

    auto burst =
        market_pipeline::preset_for(
            market_pipeline::
                WorkloadMode::Bursty
        );

    market_pipeline::BurstSchedule schedule{
        burst
    };

    if (!schedule.active_at(0ns)) {
        std::cerr
            << "burst must be active at phase start\n";
        return 1;
    }

    if (
        schedule.target_rate_at(0ns) <=
        burst.base_ingress_rate
    ) {
        std::cerr
            << "burst target must exceed base rate\n";
        return 1;
    }

    auto queue_config = burst;
    queue_config.queue_capacity = 4;
    queue_config.high_watermark_pct = 50;
    queue_config.backpressure_policy =
        market_pipeline::
            BackpressurePolicy::
                CoalesceLatest;

    market_pipeline::BoundedEventQueue queue{
        queue_config.queue_capacity,
        queue_config.high_watermark_pct,
        queue_config.backpressure_policy
    };

    std::atomic_bool stop{false};

    market_pipeline::MarketEvent first{};
    first.feed_id = 0;

    market_pipeline::MarketEvent second{};
    second.feed_id = 1;

    market_pipeline::MarketEvent replacement{};
    replacement.feed_id = 0;
    replacement.sequence = 2;

    const auto a =
        queue.push(
            first,
            stop
        );

    const auto b =
        queue.push(
            second,
            stop
        );

    const auto c =
        queue.push(
            replacement,
            stop
        );

    if (
        !a.enqueued ||
        !b.enqueued ||
        !c.coalesced ||
        queue.snapshot().size != 2
    ) {
        std::cerr
            << "coalescing contract failed\n";
        return 1;
    }

    auto config =
        market_pipeline::preset_for(
            market_pipeline::
                WorkloadMode::Normal
        );

    config.base_ingress_rate = 10'000;
    config.consumer_capacity = 20'000;
    config.queue_capacity = 1024;
    config.run_duration_seconds = 1;

    market_pipeline::BenchmarkOptions
        options{};

    options.warmup = 10ms;
    options.measurement_override =
        100ms;
    options.sample_stride = 4;

    const auto result =
        market_pipeline::run_benchmark(
            config,
            options
        );

    if (
        result.generated == 0 ||
        result.processed == 0
    ) {
        std::cerr
            << "silent benchmark produced no work\n";
        return 1;
    }

    const auto output =
        std::filesystem::
            temp_directory_path() /
        "mppl-report-test";

    std::filesystem::remove_all(
        output
    );

    const auto reports =
        market_pipeline::write_reports(
            config,
            result,
            output
        );

    if (
        !std::filesystem::exists(
            reports.json_path
        ) ||
        !std::filesystem::exists(
            reports.csv_path
        )
    ) {
        std::cerr
            << "report artifacts missing\n";
        return 1;
    }

    std::filesystem::remove_all(
        output
    );

    return 0;
}