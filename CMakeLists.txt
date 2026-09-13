cmake_minimum_required(VERSION 3.20)

project(
    market_pipeline_performance_lab
    VERSION 0.1.0
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(
    MPPL_WARNINGS_AS_ERRORS
    "Treat compiler warnings as errors"
    OFF
)

function(mppl_enable_warnings target)
    if(MSVC)
        target_compile_options(
            ${target}
            PRIVATE
                /W4
                /permissive-
                /Zc:__cplusplus
        )

        if(MPPL_WARNINGS_AS_ERRORS)
            target_compile_options(
                ${target}
                PRIVATE
                    /WX
            )
        endif()
    else()
        target_compile_options(
            ${target}
            PRIVATE
                -Wall
                -Wextra
                -Wpedantic
                -Wconversion
                -Wshadow
        )

        if(MPPL_WARNINGS_AS_ERRORS)
            target_compile_options(
                ${target}
                PRIVATE
                    -Werror
            )
        endif()
    endif()
endfunction()

add_library(
    market_pipeline_core
    src/version.cpp
    src/scenario.cpp
    src/cli.cpp
    src/queue.cpp
    src/benchmark.cpp
    src/report.cpp
)

target_include_directories(
    market_pipeline_core
    PUBLIC
        ${PROJECT_SOURCE_DIR}/include
)

mppl_enable_warnings(
    market_pipeline_core
)

add_executable(
    market_pipeline_lab
    src/main.cpp
)

set_target_properties(
    market_pipeline_lab
    PROPERTIES
        OUTPUT_NAME "market-pipeline-lab"
)

target_link_libraries(
    market_pipeline_lab
    PRIVATE
        market_pipeline_core
)

mppl_enable_warnings(
    market_pipeline_lab
)

include(CTest)

if(BUILD_TESTING)
    add_executable(
        market_pipeline_smoke_tests
        tests/smoke_test.cpp
    )

    target_link_libraries(
        market_pipeline_smoke_tests
        PRIVATE
            market_pipeline_core
    )

    mppl_enable_warnings(
        market_pipeline_smoke_tests
    )

    add_test(
        NAME project_smoke
        COMMAND market_pipeline_smoke_tests
    )

    add_executable(
        market_pipeline_scenario_tests
        tests/scenario_test.cpp
    )

    target_link_libraries(
        market_pipeline_scenario_tests
        PRIVATE
            market_pipeline_core
    )

    mppl_enable_warnings(
        market_pipeline_scenario_tests
    )

    add_test(
        NAME scenario_contract
        COMMAND market_pipeline_scenario_tests
    )

    add_executable(
        market_pipeline_cli_tests
        tests/cli_test.cpp
    )

    target_link_libraries(
        market_pipeline_cli_tests
        PRIVATE
            market_pipeline_core
    )

    mppl_enable_warnings(
        market_pipeline_cli_tests
    )

    add_test(
        NAME interactive_cli_contract
        COMMAND market_pipeline_cli_tests
    )

    add_executable(
        market_pipeline_benchmark_tests
        tests/benchmark_test.cpp
    )

    target_link_libraries(
        market_pipeline_benchmark_tests
        PRIVATE
            market_pipeline_core
    )

    mppl_enable_warnings(
        market_pipeline_benchmark_tests
    )

    add_test(
        NAME silent_benchmark_contract
        COMMAND market_pipeline_benchmark_tests
    )
endif()