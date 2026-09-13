#include "market_pipeline/pipeline_comparison.hpp"
#include "market_pipeline/report.hpp"
#include "market_pipeline/scenario.hpp"

#include <chrono>
#include <cmath>
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
                WorkloadMode::Normal
        );

    config.base_ingress_rate = 20'000;
    config.consumer_capacity = 40'000;
    config.queue_capacity = 2048;
    config.high_watermark_pct = 80;
    config.run_duration_seconds = 1;
    config.seed = 1337;

    market_pipeline::BenchmarkOptions
        options{};

    options.warmup = 10ms;
    options.measurement_override =
        100ms;
    options.sample_stride = 4;

    const auto result =
        market_pipeline::
            run_pipeline_comparison(
                config,
                options
            );

    expect(
        result.dynamic_deque.generated > 0,
        "deque backend generated work"
    );

    expect(
        result.preallocated_ring.generated > 0,
        "ring backend generated work"
    );

    expect(
        result.dynamic_deque.processed > 0,
        "deque backend processed work"
    );

    expect(
        result.preallocated_ring.processed > 0,
        "ring backend processed work"
    );

    expect(
        result.dynamic_deque.
                target_attainment_pct >
            80.0,
        "deque reaches low-load target"
    );

    expect(
        result.preallocated_ring.
                target_attainment_pct >
            80.0,
        "ring reaches low-load target"
    );

    expect(
        std::isfinite(
            result.
                ring_vs_deque_ingress_rate_pct
        ),
        "ingress delta is finite"
    );

    expect(
        std::isfinite(
            result.
                ring_vs_deque_processing_rate_pct
        ),
        "processing delta is finite"
    );

    expect(
        std::isfinite(
            result.
                ring_vs_deque_event_age_p99_pct
        ),
        "event-age delta is finite"
    );

    expect(
        std::isfinite(
            result.
                ring_vs_deque_queue_residence_p99_pct
        ),
        "queue-residence delta is finite"
    );

    const auto output =
        std::filesystem::
            temp_directory_path() /
        "mppl-pipeline-comparison-test";

    std::filesystem::remove_all(
        output
    );

    const auto reports =
        market_pipeline::
            write_comparison_reports(
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
            "\"dynamic_deque\""
        ) != std::string::npos,
        "JSON contains deque backend"
    );

    expect(
        json.find(
            "\"preallocated_ring\""
        ) != std::string::npos,
        "JSON contains ring backend"
    );

    expect(
        json.find(
            "\"ring_vs_deque_delta_pct\""
        ) != std::string::npos,
        "JSON contains neutral deltas"
    );

    expect(
        csv.find(
            "dynamic_deque"
        ) != std::string::npos &&
        csv.find(
            "preallocated_ring"
        ) != std::string::npos,
        "CSV contains both backends"
    );

    std::filesystem::remove_all(
        output
    );

    if (failures != 0) {
        std::cerr
            << failures
            << " pipeline comparison checks failed\n";

        return 1;
    }

    return 0;
}