#include "market_pipeline/feed_arbitration.hpp"

#include <chrono>
#include <iostream>

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

}  // namespace

int main() {
    using market_pipeline::FeedArbiter;
    using market_pipeline::FeedTrustState;
    using market_pipeline::SteadyClock;

    const SteadyClock::time_point origin{};

    FeedArbiter arbiter{
        2,
        5ms,
        10ms,
        0
    };

    const auto primary_first =
        arbiter.observe(
            0,
            1,
            origin
        );

    const auto backup_first =
        arbiter.observe(
            1,
            1,
            origin
        );

    expect(
        primary_first.accepted,
        "primary first observation accepted"
    );

    expect(
        backup_first.accepted,
        "backup first observation accepted"
    );

    expect(
        arbiter.should_forward(
            0,
            origin
        ),
        "fresh primary selected"
    );

    static_cast<void>(
        arbiter.observe(
            1,
            2,
            origin + 6ms
        )
    );

    expect(
        arbiter.state_of(
            0,
            origin + 6ms
        ) == FeedTrustState::Stale,
        "primary becomes stale"
    );

    expect(
        arbiter.should_forward(
            1,
            origin + 6ms
        ),
        "fresh backup selected after primary stales"
    );

    static_cast<void>(
        arbiter.observe(
            0,
            2,
            origin + 7ms
        )
    );

    expect(
        arbiter.should_forward(
            1,
            origin + 7ms
        ),
        "recovered primary is held before promotion"
    );

    static_cast<void>(
        arbiter.observe(
            1,
            3,
            origin + 12ms
        )
    );

    static_cast<void>(
        arbiter.observe(
            0,
            3,
            origin + 12ms
        )
    );

    expect(
        arbiter.should_forward(
            1,
            origin + 12ms
        ),
        "primary still held before recovery window completes"
    );

    static_cast<void>(
        arbiter.observe(
            1,
            4,
            origin + 17ms
        )
    );

    static_cast<void>(
        arbiter.observe(
            0,
            4,
            origin + 17ms
        )
    );

    expect(
        arbiter.should_forward(
            0,
            origin + 17ms
        ),
        "primary promoted after continuous recovery hold"
    );

    const auto regressed =
        arbiter.observe(
            0,
            4,
            origin + 19ms
        );

    expect(
        !regressed.accepted &&
            regressed.sequence_regression,
        "sequence regression rejected"
    );

    const auto stable_snapshot =
        arbiter.snapshot(
            origin + 19ms
        );

    expect(
        stable_snapshot.feed_switches == 2,
        "primary-backup-primary counts two switches"
    );

    expect(
        stable_snapshot.primary_stale_transitions == 1,
        "primary stale transition counted once"
    );

    expect(
        stable_snapshot.primary_recoveries == 1,
        "primary recovery counted once"
    );

    expect(
        stable_snapshot.sequence_regressions == 1,
        "sequence regression counted once"
    );

    const auto untrusted_snapshot =
        arbiter.snapshot(
            origin + 30ms
        );

    expect(
        !untrusted_snapshot.active_feed.has_value(),
        "both stale produces no trusted active feed"
    );

    expect(
        untrusted_snapshot.untrusted_transitions == 1,
        "trusted-to-untrusted transition counted once"
    );

    FeedArbiter startup{
        2,
        5ms,
        10ms,
        0
    };

    static_cast<void>(
        startup.observe(
            1,
            1,
            origin
        )
    );

    expect(
        startup.should_forward(
            1,
            origin
        ),
        "backup may serve before primary is ever observed"
    );

    static_cast<void>(
        startup.observe(
            0,
            1,
            origin + 1ms
        )
    );

    expect(
        startup.should_forward(
            0,
            origin + 1ms
        ),
        "initial primary observation is immediately preferred"
    );

    if (failures != 0) {
        std::cerr
            << failures
            << " feed arbitration checks failed\n";

        return 1;
    }

    return 0;
}