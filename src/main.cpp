#include "market_pipeline/benchmark.hpp"
#include "market_pipeline/cli.hpp"
#include "market_pipeline/report.hpp"
#include "market_pipeline/version.hpp"

#include <exception>
#include <iostream>

int main() {
    std::cout
        << "Market Pipeline Performance Lab\n"
        << "version: "
        << market_pipeline::kVersion
        << "\n";

    try {
        const auto config =
            market_pipeline::
                run_interactive_setup(
                    std::cin,
                    std::cout
                );

        std::cout
            << "\nBenchmark running - timed measurement is intentionally silent. "
               "Please wait...\n"
            << std::flush;

        const auto result =
            market_pipeline::
                run_benchmark(
                    config
                );

        const auto reports =
            market_pipeline::
                write_reports(
                    config,
                    result
                );

        std::cout
            << "\nRUN COMPLETE\n"
            << "JSON: "
            << reports.json_path.string()
            << '\n'
            << "CSV:  "
            << reports.csv_path.string()
            << '\n';

        return 0;
    } catch (const std::exception& error) {
        std::cerr
            << "error: "
            << error.what()
            << '\n';

        return 1;
    }
}