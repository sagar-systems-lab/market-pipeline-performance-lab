#pragma once

#include <string_view>

namespace market_pipeline {

inline constexpr std::string_view kVersion{"0.1.0"};

[[nodiscard]]
std::string_view status() noexcept;

}  // namespace market_pipeline