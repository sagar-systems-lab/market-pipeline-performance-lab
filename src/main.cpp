#include "market_pipeline/version.hpp"

#include <iostream>

int main() {
    std::cout
        << "Market Pipeline Performance Lab\n"
        << "version: " << market_pipeline::kVersion << '\n'
        << "status: " << market_pipeline::status() << '\n';

    return 0;
}