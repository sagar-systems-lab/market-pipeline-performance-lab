#pragma once

#include "market_pipeline/scenario.hpp"

#include <iosfwd>

namespace market_pipeline {

[[nodiscard]]
ScenarioConfig run_interactive_setup(
    std::istream& input,
    std::ostream& output
);

void print_scenario_summary(
    const ScenarioConfig& config,
    std::ostream& output
);

}  // namespace market_pipeline