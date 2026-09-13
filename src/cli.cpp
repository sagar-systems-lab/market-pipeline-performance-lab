#include "market_pipeline/cli.hpp"

#include <cstddef>
#include <cstdint>
#include <istream>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>

namespace market_pipeline {
namespace {

template <typename T>
bool parse_number(const std::string& text, T& result) {
    if constexpr (std::is_unsigned_v<T>) {
        if (!text.empty() && text.front() == '-') {
            return false;
        }
    }

    std::istringstream stream{text};
    T parsed{};

    stream >> parsed;

    if (!stream) {
        return false;
    }

    stream >> std::ws;

    if (!stream.eof()) {
        return false;
    }

    result = parsed;
    return true;
}

template <typename T>
T prompt_number(
    std::istream& input,
    std::ostream& output,
    const std::string& label,
    const T default_value
) {
    while (true) {
        output << label << " [" << default_value << "]: ";

        std::string line;

        if (!std::getline(input, line)) {
            throw std::runtime_error("input stream closed");
        }

        if (line.empty()) {
            return default_value;
        }

        T parsed{};

        if (parse_number(line, parsed)) {
            return parsed;
        }

        output << "Invalid value. Try again.\n";
    }
}

WorkloadMode prompt_workload(
    std::istream& input,
    std::ostream& output
) {
    output
        << "\nChoose workload mode:\n\n"
        << "[1] Normal traffic\n"
        << "[2] Bursty traffic\n"
        << "[3] Slow consumer\n"
        << "[4] Feed failover\n"
        << "[5] Saturation sweep\n"
        << "[6] Custom scenario\n\n";

    while (true) {
        const auto choice =
            prompt_number<std::uint32_t>(
                input,
                output,
                ">",
                1
            );

        switch (choice) {
        case 1:
            return WorkloadMode::Normal;
        case 2:
            return WorkloadMode::Bursty;
        case 3:
            return WorkloadMode::SlowConsumer;
        case 4:
            return WorkloadMode::FeedFailover;
        case 5:
            return WorkloadMode::SaturationSweep;
        case 6:
            return WorkloadMode::Custom;
        default:
            output << "Choose a value from 1 to 6.\n";
            break;
        }
    }
}

BackpressurePolicy prompt_backpressure(
    std::istream& input,
    std::ostream& output,
    const BackpressurePolicy default_policy
) {
    output
        << "\nBackpressure policy:\n"
        << "[1] Block producer\n"
        << "[2] Drop oldest\n"
        << "[3] Coalesce latest\n";

    std::uint32_t default_choice = 1;

    switch (default_policy) {
    case BackpressurePolicy::BlockProducer:
        default_choice = 1;
        break;
    case BackpressurePolicy::DropOldest:
        default_choice = 2;
        break;
    case BackpressurePolicy::CoalesceLatest:
        default_choice = 3;
        break;
    }

    while (true) {
        const auto choice =
            prompt_number<std::uint32_t>(
                input,
                output,
                ">",
                default_choice
            );

        switch (choice) {
        case 1:
            return BackpressurePolicy::BlockProducer;
        case 2:
            return BackpressurePolicy::DropOldest;
        case 3:
            return BackpressurePolicy::CoalesceLatest;
        default:
            output << "Choose a value from 1 to 3.\n";
            break;
        }
    }
}

ScenarioConfig prompt_custom(
    std::istream& input,
    std::ostream& output
) {
    auto config = preset_for(WorkloadMode::Custom);

    output << "\nCustom scenario configuration\n\n";

    config.feed_count =
        prompt_number<std::size_t>(
            input,
            output,
            "Synthetic feeds",
            config.feed_count
        );

    config.base_ingress_rate =
        prompt_number<std::uint64_t>(
            input,
            output,
            "Base ingress rate (messages/sec)",
            config.base_ingress_rate
        );

    config.consumer_capacity =
        prompt_number<std::uint64_t>(
            input,
            output,
            "Consumer capacity (messages/sec)",
            config.consumer_capacity
        );

    config.queue_capacity =
        prompt_number<std::size_t>(
            input,
            output,
            "Queue capacity",
            config.queue_capacity
        );

    config.high_watermark_pct =
        prompt_number<std::uint32_t>(
            input,
            output,
            "Backpressure high-water mark (%)",
            config.high_watermark_pct
        );

    config.burst.multiplier =
        prompt_number<double>(
            input,
            output,
            "Burst multiplier",
            config.burst.multiplier
        );

    config.burst.duration_ms =
        prompt_number<std::uint32_t>(
            input,
            output,
            "Burst duration (ms)",
            config.burst.duration_ms
        );

    config.burst.runtime_share_pct =
        prompt_number<double>(
            input,
            output,
            "Burst share of runtime (%)",
            config.burst.runtime_share_pct
        );

    config.stale_threshold_ms =
        prompt_number<std::uint32_t>(
            input,
            output,
            "Feed stale threshold (ms)",
            config.stale_threshold_ms
        );

    config.backpressure_policy =
        prompt_backpressure(
            input,
            output,
            config.backpressure_policy
        );

    config.run_duration_seconds =
        prompt_number<std::uint32_t>(
            input,
            output,
            "Run duration (seconds)",
            config.run_duration_seconds
        );

    config.seed =
        prompt_number<std::uint64_t>(
            input,
            output,
            "Deterministic seed",
            config.seed
        );

    return config;
}

}  // namespace

ScenarioConfig run_interactive_setup(
    std::istream& input,
    std::ostream& output
) {
    const auto mode = prompt_workload(input, output);

    ScenarioConfig config =
        mode == WorkloadMode::Custom
            ? prompt_custom(input, output)
            : preset_for(mode);

    const auto validation_error =
        validate_scenario(config);

    if (validation_error.has_value()) {
        throw std::runtime_error(
            "invalid scenario: " +
            validation_error.value()
        );
    }

    return config;
}

void print_scenario_summary(
    const ScenarioConfig& config,
    std::ostream& output
) {
    output
        << "\nScenario accepted\n"
        << "-----------------\n"
        << "mode:              "
        << to_string(config.mode) << '\n'
        << "feeds:             "
        << config.feed_count << '\n'
        << "base ingress:      "
        << config.base_ingress_rate
        << " msg/s\n"
        << "consumer capacity: "
        << config.consumer_capacity
        << " msg/s\n"
        << "queue capacity:    "
        << config.queue_capacity << '\n'
        << "high-water mark:   "
        << config.high_watermark_pct
        << "%\n"
        << "burst multiplier:  "
        << config.burst.multiplier
        << "x\n"
        << "burst duration:    "
        << config.burst.duration_ms
        << " ms\n"
        << "burst runtime:     "
        << config.burst.runtime_share_pct
        << "%\n"
        << "stale threshold:   "
        << config.stale_threshold_ms
        << " ms\n"
        << "backpressure:      "
        << to_string(
               config.backpressure_policy
           )
        << '\n'
        << "duration:          "
        << config.run_duration_seconds
        << " s\n"
        << "seed:              "
        << config.seed << '\n'
        << "status:            configuration valid\n";
}

}  // namespace market_pipeline