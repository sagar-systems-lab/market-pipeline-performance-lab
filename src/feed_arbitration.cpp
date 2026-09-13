#include "market_pipeline/feed_arbitration.hpp"

#include <stdexcept>

namespace market_pipeline {

FeedArbiter::FeedArbiter(
    const std::size_t feed_count,
    const std::chrono::milliseconds stale_threshold,
    const std::chrono::milliseconds recovery_hold,
    const std::size_t primary_feed
)
    : feeds_(feed_count),
      stale_threshold_(stale_threshold),
      recovery_hold_(recovery_hold),
      primary_feed_(primary_feed) {
    if (feed_count == 0) {
        throw std::invalid_argument(
            "feed count must be non-zero"
        );
    }

    if (primary_feed_ >= feed_count) {
        throw std::invalid_argument(
            "primary feed index is out of range"
        );
    }

    if (stale_threshold_.count() <= 0) {
        throw std::invalid_argument(
            "stale threshold must be positive"
        );
    }

    if (recovery_hold_.count() < 0) {
        throw std::invalid_argument(
            "recovery hold cannot be negative"
        );
    }
}

bool FeedArbiter::is_fresh(
    const std::size_t feed_id,
    const SteadyClock::time_point now
) const {
    const auto& feed = feeds_.at(feed_id);

    if (!feed.seen) {
        return false;
    }

    return
        now >= feed.last_seen &&
        now - feed.last_seen <=
            stale_threshold_;
}

bool FeedArbiter::primary_eligible(
    const SteadyClock::time_point now
) const {
    if (!is_fresh(primary_feed_, now)) {
        return false;
    }

    if (!primary_has_been_stale_) {
        return true;
    }

    if (!primary_recovery_since_) {
        return false;
    }

    return
        now >= *primary_recovery_since_ &&
        now - *primary_recovery_since_ >=
            recovery_hold_;
}

std::optional<std::size_t>
FeedArbiter::first_fresh_backup(
    const SteadyClock::time_point now
) const {
    for (
        std::size_t index = 0;
        index < feeds_.size();
        ++index
    ) {
        if (
            index != primary_feed_ &&
            is_fresh(index, now)
        ) {
            return index;
        }
    }

    return std::nullopt;
}

void FeedArbiter::transition_active(
    const std::optional<std::size_t> next
) {
    if (active_feed_ == next) {
        return;
    }

    if (active_feed_ && next) {
        ++feed_switches_;
    }

    if (
        trusted_state_established_ &&
        active_feed_ &&
        !next
    ) {
        ++untrusted_transitions_;
    }

    active_feed_ = next;

    if (next) {
        trusted_state_established_ = true;
    }
}

FeedObservation FeedArbiter::observe(
    const std::size_t feed_id,
    const std::uint64_t sequence,
    const SteadyClock::time_point now
) {
    if (feed_id >= feeds_.size()) {
        throw std::out_of_range(
            "feed index is out of range"
        );
    }

    auto& feed = feeds_[feed_id];

    if (
        feed.seen &&
        sequence <= feed.last_sequence
    ) {
        ++sequence_regressions_;

        return FeedObservation{
            false,
            true
        };
    }

    const bool was_stale =
        feed.seen &&
        (
            now < feed.last_seen ||
            now - feed.last_seen >
                stale_threshold_
        );

    feed.seen = true;
    feed.last_sequence = sequence;
    feed.last_seen = now;
    feed.trust = FeedTrustState::Fresh;

    if (
        feed_id == primary_feed_ &&
        primary_has_been_stale_ &&
        was_stale
    ) {
        primary_recovery_since_ = now;
        ++primary_recoveries_;
    }

    return FeedObservation{
        true,
        false
    };
}

void FeedArbiter::evaluate(
    const SteadyClock::time_point now
) {
    for (
        std::size_t index = 0;
        index < feeds_.size();
        ++index
    ) {
        auto& feed = feeds_[index];

        if (!feed.seen) {
            feed.trust =
                FeedTrustState::Unseen;
            continue;
        }

        const bool fresh =
            is_fresh(index, now);

        if (
            !fresh &&
            feed.trust !=
                FeedTrustState::Stale
        ) {
            feed.trust =
                FeedTrustState::Stale;

            if (index == primary_feed_) {
                ++primary_stale_transitions_;
                primary_has_been_stale_ = true;
                primary_recovery_since_.reset();
            }
        } else if (fresh) {
            feed.trust =
                FeedTrustState::Fresh;
        }
    }

    if (
        active_feed_ &&
        is_fresh(*active_feed_, now)
    ) {
        if (
            *active_feed_ != primary_feed_ &&
            primary_eligible(now)
        ) {
            transition_active(primary_feed_);
        }

        return;
    }

    if (primary_eligible(now)) {
        transition_active(primary_feed_);
        return;
    }

    transition_active(
        first_fresh_backup(now)
    );
}

bool FeedArbiter::should_forward(
    const std::size_t feed_id,
    const SteadyClock::time_point now
) {
    if (feed_id >= feeds_.size()) {
        throw std::out_of_range(
            "feed index is out of range"
        );
    }

    evaluate(now);

    return
        active_feed_.has_value() &&
        *active_feed_ == feed_id;
}

FeedTrustState FeedArbiter::state_of(
    const std::size_t feed_id,
    const SteadyClock::time_point now
) {
    if (feed_id >= feeds_.size()) {
        throw std::out_of_range(
            "feed index is out of range"
        );
    }

    evaluate(now);
    return feeds_[feed_id].trust;
}

FeedArbitrationSnapshot FeedArbiter::snapshot(
    const SteadyClock::time_point now
) {
    evaluate(now);

    return FeedArbitrationSnapshot{
        active_feed_,
        primary_feed_,
        feed_switches_,
        untrusted_transitions_,
        primary_stale_transitions_,
        primary_recoveries_,
        sequence_regressions_
    };
}

}  // namespace market_pipeline