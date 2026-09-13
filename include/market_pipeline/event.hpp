#pragma once

#include <chrono>
#include <cstdint>

namespace market_pipeline {

using SteadyClock = std::chrono::steady_clock;

struct MarketEvent {
    std::uint32_t feed_id{};
    std::uint64_t sequence{};
    SteadyClock::time_point generated_at{};
    SteadyClock::time_point enqueued_at{};
};

}  // namespace market_pipeline