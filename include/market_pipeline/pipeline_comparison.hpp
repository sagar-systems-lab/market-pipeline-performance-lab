#pragma once

#include "market_pipeline/benchmark.hpp"
#include "market_pipeline/scenario.hpp"

namespace market_pipeline {

struct PipelineComparisonResult {
    QueueBackend first_backend{
        QueueBackend::DynamicDeque
    };

    RunResult dynamic_deque{};
    RunResult preallocated_ring{};

    double ring_vs_deque_ingress_rate_pct{};
    double ring_vs_deque_processing_rate_pct{};
    double ring_vs_deque_event_age_p99_pct{};
    double ring_vs_deque_queue_residence_p99_pct{};
};

[[nodiscard]]
PipelineComparisonResult run_pipeline_comparison(
    const ScenarioConfig& config,
    const BenchmarkOptions& options = {}
);

}  // namespace market_pipeline