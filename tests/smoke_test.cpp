#include "market_pipeline/version.hpp"

#include <iostream>
#include <string_view>

int main() {
    constexpr std::string_view expected_version{"0.1.0"};
    constexpr std::string_view expected_status{"ready"};

    if (market_pipeline::kVersion != expected_version) {
        std::cerr << "unexpected version\n";
        return 1;
    }

    if (market_pipeline::status() != expected_status) {
        std::cerr << "unexpected status\n";
        return 1;
    }

    return 0;
}