#include "market_pipeline/cli.hpp"
#include "market_pipeline/pipeline_comparison.hpp"
#include "market_pipeline/report.hpp"
#include "market_pipeline/version.hpp"

#include <exception>
#include <iostream>

int main() {
    std::cout
        << "Market Pipeline Queue Comparison\n"
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
            << "\nComparison running - timed measurements are intentionally silent. "
               "Please wait...\n"
            << std::flush;

        const auto result =
            market_pipeline::
                run_pipeline_comparison(
                    config
                );

        const auto reports =
            market_pipeline::
                write_comparison_reports(
                    config,
                    result
                );

        std::cout
            << "\nCOMPARISON COMPLETE\n"
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