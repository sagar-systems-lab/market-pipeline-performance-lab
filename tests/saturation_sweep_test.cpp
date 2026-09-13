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
                WorkloadMode::SaturationSweep
        );

    config.base_ingress_rate = 5'000;
    config.consumer_capacity = 20'000;
    config.queue_capacity = 128;
    config.high_watermark_pct = 50;
    config.backpressure_policy =
        market_pipeline::
            BackpressurePolicy::
                BlockProducer;
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

    const auto& sweep =
        result.saturation_sweep;

    expect(
        sweep.exercised,
        "saturation sweep path exercised"
    );

    expect(
        sweep.steps.size() == 8,
        "saturation sweep contains eight steps"
    );

    if (!sweep.steps.empty()) {
        expect(
            sweep.steps.front().target_rate ==
                config.base_ingress_rate,
            "first sweep target equals configured base rate"
        );
    }

    for (
        std::size_t index = 1;
        index < sweep.steps.size();
        ++index
    ) {
        expect(
            sweep.steps[index].target_rate >
                sweep.steps[index - 1].
                    target_rate,
            "sweep targets strictly increase"
        );
    }

    expect(
        sweep.first_pressure_step >= 0,
        "pressure boundary detected"
    );

    expect(
        sweep.first_pressure_target_rate >
            0,
        "pressure boundary target recorded"
    );

    bool saw_pressure = false;
    bool saw_clean_step = false;

    for (const auto& step : sweep.steps) {
        saw_pressure =
            saw_pressure ||
            step.pressure_observed;

        saw_clean_step =
            saw_clean_step ||
            !step.pressure_observed;
    }

    expect(
        saw_pressure,
        "at least one step shows pressure"
    );

    expect(
        saw_clean_step,
        "at least one step remains below pressure"
    );

    const auto output =
        std::filesystem::
            temp_directory_path() /
        "mppl-saturation-report-test";

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
            "\"saturation_sweep\""
        ) != std::string::npos,
        "JSON contains saturation evidence"
    );

    expect(
        json.find(
            "\"first_pressure_target_rate\""
        ) != std::string::npos,
        "JSON contains pressure boundary"
    );

    expect(
        csv.find(
            "step_index,target_rate"
        ) != std::string::npos,
        "CSV contains per-step sweep columns"
    );

    std::filesystem::remove_all(
        output
    );

    if (failures != 0) {
        std::cerr
            << failures
            << " saturation sweep checks failed\n";

        return 1;
    }

    return 0;
}