#pragma once

#include "market_pipeline/event.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace market_pipeline {

enum class FeedTrustState {
    Unseen,
    Fresh,
    Stale
};

struct FeedObservation {
    bool accepted{false};
    bool sequence_regression{false};
};

struct FeedArbitrationSnapshot {
    std::optional<std::size_t> active_feed;
    std::size_t primary_feed{};
    std::uint64_t feed_switches{};
    std::uint64_t untrusted_transitions{};
    std::uint64_t primary_stale_transitions{};
    std::uint64_t primary_recoveries{};
    std::uint64_t sequence_regressions{};
};

class FeedArbiter {
public:
    FeedArbiter(
        std::size_t feed_count,
        std::chrono::milliseconds stale_threshold,
        std::chrono::milliseconds recovery_hold,
        std::size_t primary_feed = 0
    );

    [[nodiscard]]
    FeedObservation observe(
        std::size_t feed_id,
        std::uint64_t sequence,
        SteadyClock::time_point now
    );

    void evaluate(
        SteadyClock::time_point now
    );

    [[nodiscard]]
    bool should_forward(
        std::size_t feed_id,
        SteadyClock::time_point now
    );

    [[nodiscard]]
    FeedTrustState state_of(
        std::size_t feed_id,
        SteadyClock::time_point now
    );

    [[nodiscard]]
    FeedArbitrationSnapshot snapshot(
        SteadyClock::time_point now
    );

private:
    struct FeedState {
        bool seen{false};
        std::uint64_t last_sequence{};
        SteadyClock::time_point last_seen{};
        FeedTrustState trust{FeedTrustState::Unseen};
    };

    [[nodiscard]]
    bool is_fresh(
        std::size_t feed_id,
        SteadyClock::time_point now
    ) const;

    [[nodiscard]]
    bool primary_eligible(
        SteadyClock::time_point now
    ) const;

    [[nodiscard]]
    std::optional<std::size_t>
    first_fresh_backup(
        SteadyClock::time_point now
    ) const;

    void transition_active(
        std::optional<std::size_t> next
    );

    std::vector<FeedState> feeds_;
    std::chrono::milliseconds stale_threshold_;
    std::chrono::milliseconds recovery_hold_;
    std::size_t primary_feed_;

    std::optional<std::size_t> active_feed_;
    std::optional<SteadyClock::time_point>
        primary_recovery_since_;

    bool primary_has_been_stale_{false};
    bool trusted_state_established_{false};

    std::uint64_t feed_switches_{0};
    std::uint64_t untrusted_transitions_{0};
    std::uint64_t primary_stale_transitions_{0};
    std::uint64_t primary_recoveries_{0};
    std::uint64_t sequence_regressions_{0};
};

}  // namespace market_pipeline