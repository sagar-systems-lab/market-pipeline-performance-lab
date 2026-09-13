#include "market_pipeline/deque_queue.hpp"
#include "market_pipeline/feed_arbitration.hpp"
#include "market_pipeline/queue.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <iostream>
#include <stdexcept>

namespace {

using namespace std::chrono_literals;

int failures = 0;

void expect(
    const bool condition,
    const char* message
) {
    if (!condition) {
        std::cerr
            << "FAIL: "
            << message
            << '\n';

        ++failures;
    }
}

template <typename Exception, typename Function>
void expect_throws(
    Function&& function,
    const char* message
) {
    try {
        function();

        std::cerr
            << "FAIL: "
            << message
            << " (no exception)\n";

        ++failures;
    } catch (const Exception&) {
    } catch (const std::exception& error) {
        std::cerr
            << "FAIL: "
            << message
            << " (wrong exception: "
            << error.what()
            << ")\n";

        ++failures;
    } catch (...) {
        std::cerr
            << "FAIL: "
            << message
            << " (non-standard exception)\n";

        ++failures;
    }
}

market_pipeline::MarketEvent event(
    const std::uint32_t feed,
    const std::uint64_t sequence
) {
    market_pipeline::MarketEvent value{};
    value.feed_id = feed;
    value.sequence = sequence;
    value.generated_at =
        market_pipeline::SteadyClock::now();

    return value;
}

void queue_constructor_boundaries() {
    using market_pipeline::BackpressurePolicy;
    using market_pipeline::BoundedEventQueue;
    using market_pipeline::DequeEventQueue;

    expect_throws<std::invalid_argument>(
        [] {
            BoundedEventQueue queue{
                0,
                80,
                BackpressurePolicy::BlockProducer
            };

            static_cast<void>(queue);
        },
        "ring rejects zero capacity"
    );

    expect_throws<std::invalid_argument>(
        [] {
            BoundedEventQueue queue{
                8,
                0,
                BackpressurePolicy::BlockProducer
            };

            static_cast<void>(queue);
        },
        "ring rejects zero high-water mark"
    );

    expect_throws<std::invalid_argument>(
        [] {
            BoundedEventQueue queue{
                8,
                101,
                BackpressurePolicy::BlockProducer
            };

            static_cast<void>(queue);
        },
        "ring rejects high-water mark above 100"
    );

    expect_throws<std::invalid_argument>(
        [] {
            DequeEventQueue queue{
                0,
                80,
                BackpressurePolicy::BlockProducer
            };

            static_cast<void>(queue);
        },
        "deque rejects zero capacity"
    );

    expect_throws<std::invalid_argument>(
        [] {
            DequeEventQueue queue{
                8,
                101,
                BackpressurePolicy::BlockProducer
            };

            static_cast<void>(queue);
        },
        "deque rejects invalid high-water mark"
    );
}

void drop_oldest_boundary() {
    using market_pipeline::BackpressurePolicy;
    using market_pipeline::BoundedEventQueue;

    std::atomic_bool stop{false};

    BoundedEventQueue queue{
        4,
        50,
        BackpressurePolicy::DropOldest
    };

    static_cast<void>(
        queue.push(
            event(0, 1),
            stop
        )
    );

    static_cast<void>(
        queue.push(
            event(0, 2),
            stop
        )
    );

    const auto third =
        queue.push(
            event(0, 3),
            stop
        );

    expect(
        third.enqueued,
        "drop-oldest replacement is enqueued"
    );

    expect(
        third.dropped,
        "drop-oldest reports eviction at high-water mark"
    );

    const auto snapshot =
        queue.snapshot();

    expect(
        snapshot.size == 2,
        "drop-oldest preserves bounded high-water size"
    );

    market_pipeline::MarketEvent first{};

    expect(
        queue.try_pop(first),
        "drop-oldest queue remains readable"
    );

    expect(
        first.sequence == 2,
        "drop-oldest evicts the oldest event"
    );
}

void coalesce_latest_boundary() {
    using market_pipeline::BackpressurePolicy;
    using market_pipeline::BoundedEventQueue;

    std::atomic_bool stop{false};

    BoundedEventQueue queue{
        4,
        50,
        BackpressurePolicy::CoalesceLatest
    };

    static_cast<void>(
        queue.push(
            event(0, 1),
            stop
        )
    );

    static_cast<void>(
        queue.push(
            event(1, 1),
            stop
        )
    );

    const auto replacement =
        queue.push(
            event(0, 2),
            stop
        );

    expect(
        replacement.coalesced,
        "coalesce-latest reports replacement"
    );

    expect(
        !replacement.enqueued,
        "coalesce-latest does not grow queue on replacement"
    );

    const auto snapshot =
        queue.snapshot();

    expect(
        snapshot.size == 2,
        "coalesce-latest preserves queue size"
    );

    market_pipeline::MarketEvent first{};

    expect(
        queue.try_pop(first),
        "coalesced queue remains readable"
    );

    expect(
        first.feed_id == 0 &&
        first.sequence == 2,
        "coalesce-latest keeps newest event for matching feed"
    );
}

void stopped_blocking_push() {
    using market_pipeline::BackpressurePolicy;
    using market_pipeline::BoundedEventQueue;

    std::atomic_bool stop{false};

    BoundedEventQueue queue{
        4,
        50,
        BackpressurePolicy::BlockProducer
    };

    static_cast<void>(
        queue.push(
            event(0, 1),
            stop
        )
    );

    static_cast<void>(
        queue.push(
            event(0, 2),
            stop
        )
    );

    stop.store(
        true,
        std::memory_order_relaxed
    );

    const auto stopped =
        queue.push(
            event(0, 3),
            stop
        );

    expect(
        stopped.stopped,
        "blocked producer exits when stop is requested"
    );

    expect(
        !stopped.enqueued,
        "stopped push cannot enqueue"
    );
}

void arbiter_constructor_boundaries() {
    using market_pipeline::FeedArbiter;

    expect_throws<std::invalid_argument>(
        [] {
            FeedArbiter arbiter{
                0,
                5ms,
                10ms
            };

            static_cast<void>(arbiter);
        },
        "arbiter rejects zero feeds"
    );

    expect_throws<std::invalid_argument>(
        [] {
            FeedArbiter arbiter{
                2,
                0ms,
                10ms
            };

            static_cast<void>(arbiter);
        },
        "arbiter rejects zero stale threshold"
    );

    expect_throws<std::invalid_argument>(
        [] {
            FeedArbiter arbiter{
                2,
                5ms,
                -1ms
            };

            static_cast<void>(arbiter);
        },
        "arbiter rejects negative recovery hold"
    );

    expect_throws<std::invalid_argument>(
        [] {
            FeedArbiter arbiter{
                2,
                5ms,
                10ms,
                2
            };

            static_cast<void>(arbiter);
        },
        "arbiter rejects out-of-range primary"
    );
}

void timestamp_regression_is_fail_closed() {
    using market_pipeline::FeedArbiter;
    using market_pipeline::SteadyClock;

    const SteadyClock::time_point origin{};

    FeedArbiter arbiter{
        2,
        5ms,
        10ms
    };

    const auto first =
        arbiter.observe(
            0,
            1,
            origin + 10ms
        );

    expect(
        first.accepted,
        "initial timestamp accepted"
    );

    const auto regressed =
        arbiter.observe(
            0,
            2,
            origin + 9ms
        );

    expect(
        !regressed.accepted,
        "regressing timestamp rejected"
    );

    expect(
        regressed.timestamp_regression,
        "timestamp regression classified"
    );

    expect(
        !regressed.sequence_regression,
        "timestamp-only regression is not misclassified"
    );

    const auto snapshot =
        arbiter.snapshot(
            origin + 10ms
        );

    expect(
        snapshot.timestamp_regressions == 1,
        "timestamp regression counter increments"
    );

    const auto retry =
        arbiter.observe(
            0,
            2,
            origin + 11ms
        );

    expect(
        retry.accepted,
        "rejected timestamp does not mutate accepted sequence state"
    );
}

void dual_regression_is_accounted() {
    using market_pipeline::FeedArbiter;
    using market_pipeline::SteadyClock;

    const SteadyClock::time_point origin{};

    FeedArbiter arbiter{
        2,
        5ms,
        10ms
    };

    static_cast<void>(
        arbiter.observe(
            0,
            10,
            origin + 10ms
        )
    );

    const auto rejected =
        arbiter.observe(
            0,
            10,
            origin + 9ms
        );

    expect(
        !rejected.accepted,
        "dual regression rejected"
    );

    expect(
        rejected.sequence_regression &&
        rejected.timestamp_regression,
        "dual regression classifies both violations"
    );

    const auto snapshot =
        arbiter.snapshot(
            origin + 10ms
        );

    expect(
        snapshot.sequence_regressions == 1,
        "sequence regression counter increments"
    );

    expect(
        snapshot.timestamp_regressions == 1,
        "timestamp regression counter increments for dual violation"
    );
}

}  // namespace

int main() {
    queue_constructor_boundaries();
    drop_oldest_boundary();
    coalesce_latest_boundary();
    stopped_blocking_push();
    arbiter_constructor_boundaries();
    timestamp_regression_is_fail_closed();
    dual_regression_is_accounted();

    if (failures != 0) {
        std::cerr
            << failures
            << " hardening checks failed\n";

        return 1;
    }

    return 0;
}