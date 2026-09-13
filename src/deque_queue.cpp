#include "market_pipeline/deque_queue.hpp"

#include <algorithm>
#include <stdexcept>

namespace market_pipeline {

DequeEventQueue::DequeEventQueue(
    const std::size_t capacity,
    const std::uint32_t high_watermark_pct,
    const BackpressurePolicy policy
)
    : capacity_{capacity},
      high_watermark_{std::max<std::size_t>(
          1,
          (
              capacity *
              static_cast<std::size_t>(
                  high_watermark_pct
              )
          ) /
              100
      )},
      policy_{policy} {
    if (capacity_ == 0) {
        throw std::invalid_argument(
            "queue capacity must be non-zero"
        );
    }

    if (
        high_watermark_pct == 0 ||
        high_watermark_pct > 100
    ) {
        throw std::invalid_argument(
            "high-water mark must be between 1 and 100 percent"
        );
    }
}

void DequeEventQueue::update_peak_unlocked() noexcept {
    peak_size_ = std::max(
        peak_size_,
        queue_.size()
    );
}

PushOutcome DequeEventQueue::push(
    MarketEvent event,
    const std::atomic_bool& stop_requested
) {
    PushOutcome outcome{};
    std::unique_lock lock{mutex_};

    if (
        policy_ ==
        BackpressurePolicy::BlockProducer
    ) {
        if (queue_.size() >= high_watermark_) {
            outcome.blocked = true;
        }

        pressure_relieved_.wait(
            lock,
            [&] {
                return
                    queue_.size() <
                        high_watermark_ ||
                    stop_requested.load(
                        std::memory_order_relaxed
                    );
            }
        );

        if (
            stop_requested.load(
                std::memory_order_relaxed
            )
        ) {
            outcome.stopped = true;
            return outcome;
        }
    }

    event.enqueued_at = SteadyClock::now();

    if (
        policy_ ==
            BackpressurePolicy::DropOldest &&
        queue_.size() >= high_watermark_
    ) {
        if (!queue_.empty()) {
            queue_.pop_front();
            outcome.dropped = true;
        }
    }

    if (
        policy_ ==
            BackpressurePolicy::CoalesceLatest &&
        queue_.size() >= high_watermark_
    ) {
        const auto match =
            std::find_if(
                queue_.rbegin(),
                queue_.rend(),
                [&](const MarketEvent& queued) {
                    return
                        queued.feed_id ==
                        event.feed_id;
                }
            );

        if (match != queue_.rend()) {
            *match = event;
            outcome.coalesced = true;
            return outcome;
        }
    }

    if (queue_.size() >= capacity_) {
        queue_.pop_front();
        outcome.dropped = true;
    }

    queue_.push_back(event);
    update_peak_unlocked();
    outcome.enqueued = true;

    return outcome;
}

bool DequeEventQueue::try_pop(
    MarketEvent& event
) {
    {
        std::lock_guard lock{mutex_};

        if (queue_.empty()) {
            return false;
        }

        event = queue_.front();
        queue_.pop_front();
    }

    pressure_relieved_.notify_one();
    return true;
}

QueueSnapshot DequeEventQueue::snapshot() const {
    std::lock_guard lock{mutex_};

    return QueueSnapshot{
        queue_.size(),
        capacity_,
        high_watermark_,
        peak_size_
    };
}

void DequeEventQueue::wake_all() {
    pressure_relieved_.notify_all();
}

}  // namespace market_pipeline