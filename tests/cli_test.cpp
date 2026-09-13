#include "market_pipeline/cli.hpp"
#include "market_pipeline/scenario.hpp"

#include <iostream>
#include <sstream>

int main() {
    std::istringstream input{
        "6\n"
        "2\n"
        "400000\n"
        "500000\n"
        "32768\n"
        "70\n"
        "7\n"
        "300\n"
        "35\n"
        "5\n"
        "3\n"
        "20\n"
        "777\n"
    };

    std::ostringstream output;

    const auto config =
        market_pipeline::run_interactive_setup(
            input,
            output
        );

    if (
        config.mode !=
        market_pipeline::WorkloadMode::Custom
    ) {
        std::cerr << "custom mode not selected\n";
        return 1;
    }

    if (config.feed_count != 2) {
        std::cerr << "unexpected feed count\n";
        return 1;
    }

    if (config.base_ingress_rate != 400'000) {
        std::cerr << "unexpected ingress rate\n";
        return 1;
    }

    if (config.consumer_capacity != 500'000) {
        std::cerr << "unexpected consumer capacity\n";
        return 1;
    }

    if (config.queue_capacity != 32'768) {
        std::cerr << "unexpected queue capacity\n";
        return 1;
    }

    if (config.high_watermark_pct != 70) {
        std::cerr << "unexpected high-water mark\n";
        return 1;
    }

    if (config.burst.multiplier != 7.0) {
        std::cerr << "unexpected burst multiplier\n";
        return 1;
    }

    if (config.burst.duration_ms != 300) {
        std::cerr << "unexpected burst duration\n";
        return 1;
    }

    if (config.burst.runtime_share_pct != 35.0) {
        std::cerr << "unexpected burst runtime share\n";
        return 1;
    }

    if (
        config.backpressure_policy !=
        market_pipeline::BackpressurePolicy::CoalesceLatest
    ) {
        std::cerr << "unexpected backpressure policy\n";
        return 1;
    }

    if (config.run_duration_seconds != 20) {
        std::cerr << "unexpected duration\n";
        return 1;
    }

    if (config.seed != 777) {
        std::cerr << "unexpected seed\n";
        return 1;
    }

    return 0;
}