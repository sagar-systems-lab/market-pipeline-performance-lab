#include "market_pipeline/failover_workload.hpp"

#include <stdexcept>

namespace market_pipeline {

bool FailoverPlan::primary_available(
    const std::chrono::nanoseconds elapsed
) const noexcept {
    return
        elapsed < primary_outage_start ||
        elapsed >= primary_outage_end;
}

FailoverPlan make_failover_plan(
    const std::chrono::milliseconds duration,
    const std::chrono::milliseconds stale_threshold
) {
    if (duration.count() <= 0) {
        throw std::invalid_argument(
            "failover duration must be positive"
        );
    }

    if (stale_threshold.count() <= 0) {
        throw std::invalid_argument(
            "stale threshold must be positive"
        );
    }

    const auto total =
        std::chrono::duration_cast<
            std::chrono::nanoseconds
        >(duration);

    const auto stale =
        std::chrono::duration_cast<
            std::chrono::nanoseconds
        >(stale_threshold);

    const auto recovery =
        stale_threshold * 2;

    const auto recovery_ns =
        std::chrono::duration_cast<
            std::chrono::nanoseconds
        >(recovery);

    const auto outage_start =
        total / 3;

    const auto outage_end =
        (total * 2) / 3;

    if (
        outage_start <= stale * 2 ||
        outage_end <= outage_start ||
        total - outage_end <=
            recovery_ns + stale
    ) {
        throw std::invalid_argument(
            "measurement window is too short "
            "for deterministic failover"
        );
    }

    return FailoverPlan{
        outage_start,
        outage_end,
        recovery
    };
}

}  // namespace market_pipeline