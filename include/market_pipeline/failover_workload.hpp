#pragma once

#include <chrono>

namespace market_pipeline {

struct FailoverPlan {
    std::chrono::nanoseconds primary_outage_start{};
    std::chrono::nanoseconds primary_outage_end{};
    std::chrono::milliseconds recovery_hold{};

    [[nodiscard]]
    bool primary_available(
        std::chrono::nanoseconds elapsed
    ) const noexcept;
};

[[nodiscard]]
FailoverPlan make_failover_plan(
    std::chrono::milliseconds duration,
    std::chrono::milliseconds stale_threshold
);

}  // namespace market_pipeline