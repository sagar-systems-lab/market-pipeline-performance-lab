#include "market_pipeline/benchmark.hpp"
#include "market_pipeline/report.hpp"
#include "market_pipeline/scenario.hpp"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>

namespace {

using namespace std::chrono_literals;

int failures = 0;

void expect(
    const bool condition,
    const char* message
) {
    if (!condition) {
        std::cerr
            << "FAIL: "
            << message
            << '\n';

        ++failures;
    }
}

std::string read_text(
    const std::filesystem::path& path
) {
    std::ifstream input{
        path,
        std::ios::binary
    };

    if (!input) {
        return {};
    }

    return std::string{
        std::istreambuf_iterator<char>{
            input
        },
        std::istreambuf_iterator<char>{}
    };
}

}  // namespace

int main() {
    auto config =
        market_pipeline::preset_for(
            market_pipeline::
                WorkloadMode::FeedFailover
        );

    config.base_ingress_rate = 20'000;
    config.consumer_capacity = 40'000;
    config.queue_capacity = 4096;
    config.high_watermark_pct = 80;
    config.stale_threshold_ms = 5;
    config.run_duration_seconds = 1;

    market_pipeline::BenchmarkOptions
        options{};

    options.warmup = 100ms;
    options.measurement_override =
        300ms;
    options.sample_stride = 4;

    const auto result =
        market_pipeline::run_benchmark(
            config,
            options
        );

    const auto& failover =
        result.feed_failover;

    expect(
        failover.exercised,
        "feed failover path was exercised"
    );

    expect(
        failover.raw_primary_observations > 0,
        "primary observations were generated"
    );

    expect(
        failover.raw_backup_observations >
            failover.raw_primary_observations,
        "backup remains live during primary outage"
    );

    expect(
        failover.forwarded_primary > 0,
        "primary forwarded before and after outage"
    );

    expect(
        failover.forwarded_backup > 0,
        "backup forwarded during primary outage"
    );

    expect(
        failover.feed_switches >= 2,
        "primary-backup-primary switch sequence occurred"
    );

    expect(
        failover.primary_stale_transitions >= 1,
        "primary stale transition recorded"
    );

    expect(
        failover.primary_recoveries >= 1,
        "primary recovery recorded"
    );

    expect(
        failover.sequence_regressions == 0,
        "no sequence regression occurred"
    );

    expect(
        failover.failover_detection_us > 0,
        "failover detection latency measured"
    );

    expect(
        failover.primary_restore_latency_us > 0,
        "primary restore latency measured"
    );

    expect(
        failover.selection_gap_updates > 0,
        "stale-detection gap is observable"
    );

    expect(
        result.generated > result.enqueued,
        "not every logical update is forwardable during failover"
    );

    const auto output =
        std::filesystem::
            temp_directory_path() /
        "mppl-feed-failover-report-test";

    std::filesystem::remove_all(
        output
    );

    const auto reports =
        market_pipeline::write_reports(
            config,
            result,
            output
        );

    const auto json =
        read_text(
            reports.json_path
        );

    const auto csv =
        read_text(
            reports.csv_path
        );

    expect(
        json.find(
            "\"feed_arbitration\""
        ) != std::string::npos,
        "JSON contains feed arbitration evidence"
    );

    expect(
        json.find(
            "\"failover_detection_us\""
        ) != std::string::npos,
        "JSON contains failover detection latency"
    );

    expect(
        csv.find(
            "failover_exercised"
        ) != std::string::npos,
        "CSV contains failover columns"
    );

    std::filesystem::remove_all(
        output
    );

    if (failures != 0) {
        std::cerr
            << failures
            << " feed failover checks failed\n";

        return 1;
    }

    return 0;
}