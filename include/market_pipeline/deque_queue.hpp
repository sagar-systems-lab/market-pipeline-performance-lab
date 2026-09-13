#pragma once

#include "market_pipeline/queue.hpp"

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>

namespace market_pipeline {

class DequeEventQueue {
public:
    DequeEventQueue(
        std::size_t capacity,
        std::uint32_t high_watermark_pct,
        BackpressurePolicy policy
    );

    [[nodiscard]]
    PushOutcome push(
        MarketEvent event,
        const std::atomic_bool& stop_requested
    );

    [[nodiscard]]
    bool try_pop(MarketEvent& event);

    [[nodiscard]]
    QueueSnapshot snapshot() const;

    void wake_all();

private:
    void update_peak_unlocked() noexcept;

    std::size_t capacity_;
    std::size_t high_watermark_;
    BackpressurePolicy policy_;

    mutable std::mutex mutex_;
    std::condition_variable pressure_relieved_;
    std::deque<MarketEvent> queue_;
    std::size_t peak_size_{0};
};

}  // namespace market_pipeline