#include "market_pipeline/queue.hpp"

#include <algorithm>
#include <stdexcept>

namespace market_pipeline {

BoundedEventQueue::BoundedEventQueue(
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
      policy_{policy},
      storage_(capacity) {
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

std::size_t BoundedEventQueue::physical_index(
    const std::size_t logical_offset
) const noexcept {
    return (head_ + logical_offset) % capacity_;
}

void BoundedEventQueue::drop_front_unlocked() noexcept {
    if (size_ == 0) {
        return;
    }

    head_ = (head_ + 1) % capacity_;
    --size_;
}

void BoundedEventQueue::update_peak_unlocked() noexcept {
    peak_size_ = std::max(
        peak_size_,
        size_
    );
}

PushOutcome BoundedEventQueue::push(
    MarketEvent event,
    const std::atomic_bool& stop_requested
) {
    PushOutcome outcome{};
    std::unique_lock lock{mutex_};

    if (
        policy_ ==
        BackpressurePolicy::BlockProducer
    ) {
        if (size_ >= high_watermark_) {
            outcome.blocked = true;
        }

        pressure_relieved_.wait(
            lock,
            [&] {
                return
                    size_ < high_watermark_ ||
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
        size_ >= high_watermark_
    ) {
        drop_front_unlocked();
        outcome.dropped = true;
    }

    if (
        policy_ ==
            BackpressurePolicy::CoalesceLatest &&
        size_ >= high_watermark_
    ) {
        for (
            std::size_t offset = 0;
            offset < size_;
            ++offset
        ) {
            const auto reverse_offset =
                size_ - 1 - offset;

            auto& queued =
                storage_[
                    physical_index(
                        reverse_offset
                    )
                ];

            if (
                queued.feed_id ==
                event.feed_id
            ) {
                queued = event;
                outcome.coalesced = true;
                return outcome;
            }
        }
    }

    if (size_ >= capacity_) {
        drop_front_unlocked();
        outcome.dropped = true;
    }

    storage_[physical_index(size_)] = event;
    ++size_;
    update_peak_unlocked();
    outcome.enqueued = true;

    return outcome;
}

bool BoundedEventQueue::try_pop(
    MarketEvent& event
) {
    {
        std::lock_guard lock{mutex_};

        if (size_ == 0) {
            return false;
        }

        event = storage_[head_];
        drop_front_unlocked();
    }

    pressure_relieved_.notify_one();
    return true;
}

QueueSnapshot BoundedEventQueue::snapshot() const {
    std::lock_guard lock{mutex_};

    return QueueSnapshot{
        size_,
        capacity_,
        high_watermark_,
        peak_size_
    };
}

void BoundedEventQueue::wake_all() {
    pressure_relieved_.notify_all();
}

}  // namespace market_pipeline