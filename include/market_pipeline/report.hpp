#pragma once

#include "market_pipeline/benchmark.hpp"
#include "market_pipeline/scenario.hpp"

#include <filesystem>
#include <string>

namespace market_pipeline {

struct ReportPaths {
    std::string run_id;
    std::filesystem::path json_path;
    std::filesystem::path csv_path;
};

[[nodiscard]]
ReportPaths write_reports(
    const ScenarioConfig& config,
    const RunResult& result,
    const std::filesystem::path& output_directory = "results"
);

}  // namespace market_pipeline