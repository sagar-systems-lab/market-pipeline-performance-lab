#include "market_pipeline/cli.hpp"
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
            market_pipeline::run_interactive_setup(
                std::cin,
                std::cout
            );

        market_pipeline::print_scenario_summary(
            config,
            std::cout
        );
    } catch (const std::exception& error) {
        std::cerr
            << "error: "
            << error.what()
            << '\n';

        return 1;
    }

    return 0;
}