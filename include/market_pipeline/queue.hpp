#pragma once

#include "market_pipeline/event.hpp"
#include "market_pipeline/scenario.hpp"

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <vector>

namespace market_pipeline {

struct PushOutcome {
    bool enqueued{false};
    bool coalesced{false};
    bool dropped{false};
    bool blocked{false};
    bool stopped{false};
};

struct QueueSnapshot {
    std::size_t size{};
    std::size_t capacity{};
    std::size_t high_watermark{};
    std::size_t peak_size{};
};

class BoundedEventQueue {
public:
    BoundedEventQueue(
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
    [[nodiscard]]
    std::size_t physical_index(
        std::size_t logical_offset
    ) const noexcept;

    void drop_front_unlocked() noexcept;
    void update_peak_unlocked() noexcept;

    std::size_t capacity_;
    std::size_t high_watermark_;
    BackpressurePolicy policy_;
    std::vector<MarketEvent> storage_;

    mutable std::mutex mutex_;
    std::condition_variable pressure_relieved_;
    std::size_t head_{0};
    std::size_t size_{0};
    std::size_t peak_size_{0};
};

}  // namespace market_pipeline