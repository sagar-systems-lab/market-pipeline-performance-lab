#include "market_pipeline/pipeline_comparison.hpp"

#include <stdexcept>
#include <utility>

namespace market_pipeline {
namespace {

double relative_change_pct(
    const double candidate,
    const double baseline
) noexcept {
    if (baseline == 0.0) {
        return 0.0;
    }

    return
        (
            candidate -
            baseline
        ) *
        100.0 /
        baseline;
}

bool unsupported_special_mode(
    const WorkloadMode mode
) noexcept {
    return
        mode == WorkloadMode::FeedFailover ||
        mode == WorkloadMode::SaturationSweep;
}

}  // namespace

PipelineComparisonResult run_pipeline_comparison(
    const ScenarioConfig& config,
    const BenchmarkOptions& options
) {
    if (
        unsupported_special_mode(
            config.mode
        )
    ) {
        throw std::invalid_argument(
            "pipeline comparison accepts "
            "normal, bursty, slow-consumer, "
            "or custom workloads"
        );
    }

    PipelineComparisonResult result{};

    result.first_backend =
        (config.seed & 1U) == 0U
            ? QueueBackend::DynamicDeque
            : QueueBackend::PreallocatedRing;

    if (
        result.first_backend ==
        QueueBackend::DynamicDeque
    ) {
        result.dynamic_deque =
            run_benchmark_with_backend(
                config,
                QueueBackend::DynamicDeque,
                options
            );

        result.preallocated_ring =
            run_benchmark_with_backend(
                config,
                QueueBackend::PreallocatedRing,
                options
            );
    } else {
        result.preallocated_ring =
            run_benchmark_with_backend(
                config,
                QueueBackend::PreallocatedRing,
                options
            );

        result.dynamic_deque =
            run_benchmark_with_backend(
                config,
                QueueBackend::DynamicDeque,
                options
            );
    }

    result.ring_vs_deque_ingress_rate_pct =
        relative_change_pct(
            result.preallocated_ring.
                observed_ingress_rate,
            result.dynamic_deque.
                observed_ingress_rate
        );

    result.ring_vs_deque_processing_rate_pct =
        relative_change_pct(
            result.preallocated_ring.
                observed_processing_rate,
            result.dynamic_deque.
                observed_processing_rate
        );

    result.ring_vs_deque_event_age_p99_pct =
        relative_change_pct(
            result.preallocated_ring.
                event_age.p99_us,
            result.dynamic_deque.
                event_age.p99_us
        );

    result.
        ring_vs_deque_queue_residence_p99_pct =
            relative_change_pct(
                result.preallocated_ring.
                    queue_residence.p99_us,
                result.dynamic_deque.
                    queue_residence.p99_us
            );

    return result;
}

}  // namespace market_pipeline