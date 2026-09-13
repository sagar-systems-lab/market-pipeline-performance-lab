#include "market_pipeline/report.hpp"

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>

namespace market_pipeline {
namespace {

std::string compiler_name() {
#if defined(_MSC_VER)
    return
        "MSVC " +
        std::to_string(_MSC_VER);
#elif defined(__clang__)
    return
        "Clang " +
        std::to_string(__clang_major__) +
        "." +
        std::to_string(__clang_minor__) +
        "." +
        std::to_string(
            __clang_patchlevel__
        );
#elif defined(__GNUC__)
    return
        "GCC " +
        std::to_string(__GNUC__) +
        "." +
        std::to_string(__GNUC_MINOR__) +
        "." +
        std::to_string(
            __GNUC_PATCHLEVEL__
        );
#else
    return "Unknown";
#endif
}

std::string os_name() {
#if defined(_WIN32)
    return "Windows";
#elif defined(__linux__)
    return "Linux";
#elif defined(__APPLE__)
    return "macOS";
#else
    return "Unknown";
#endif
}

std::string build_type() {
#if defined(NDEBUG)
    return "Release";
#else
    return "Debug";
#endif
}

std::string make_run_id(
    const std::uint64_t seed
) {
    const auto epoch_ns =
        std::chrono::duration_cast<
            std::chrono::nanoseconds
        >(
            std::chrono::system_clock::
                now()
                    .time_since_epoch()
        ).count();

    std::ostringstream stream;

    stream
        << "run-"
        << epoch_ns
        << '-'
        << seed;

    return stream.str();
}

void write_percentiles(
    std::ostream& output,
    const char* name,
    const PercentileSummary& summary,
    const bool trailing_comma
) {
    output
        << "    \""
        << name
        << "\": {\n"
        << "      \"samples\": "
        << summary.samples
        << ",\n"
        << "      \"p50_us\": "
        << summary.p50_us
        << ",\n"
        << "      \"p95_us\": "
        << summary.p95_us
        << ",\n"
        << "      \"p99_us\": "
        << summary.p99_us
        << ",\n"
        << "      \"p99_9_us\": "
        << summary.p999_us
        << ",\n"
        << "      \"max_us\": "
        << summary.max_us
        << "\n"
        << "    }"
        << (
            trailing_comma
                ? ","
                : ""
        )
        << "\n";
}

}  // namespace

ReportPaths write_reports(
    const ScenarioConfig& config,
    const RunResult& result,
    const std::filesystem::path&
        output_directory
) {
    std::filesystem::create_directories(
        output_directory
    );

    const auto run_id =
        make_run_id(
            config.seed
        );

    const auto json_path =
        output_directory /
        (run_id + ".json");

    const auto csv_path =
        output_directory /
        (run_id + ".csv");

    std::ofstream json{
        json_path,
        std::ios::binary
    };

    if (!json) {
        throw std::runtime_error(
            "unable to open JSON report"
        );
    }

    json
        << std::fixed
        << std::setprecision(3);

    json
        << "{\n"
        << "  \"schema\": "
           "\"market-pipeline-performance-lab.v1\",\n"
        << "  \"run_id\": \""
        << run_id
        << "\",\n"
        << "  \"scenario\": {\n"
        << "    \"mode\": \""
        << to_string(config.mode)
        << "\",\n"
        << "    \"feed_count\": "
        << config.feed_count
        << ",\n"
        << "    \"base_ingress_rate\": "
        << config.base_ingress_rate
        << ",\n"
        << "    \"consumer_capacity\": "
        << config.consumer_capacity
        << ",\n"
        << "    \"queue_capacity\": "
        << config.queue_capacity
        << ",\n"
        << "    \"high_watermark_pct\": "
        << config.high_watermark_pct
        << ",\n"
        << "    \"burst_multiplier\": "
        << config.burst.multiplier
        << ",\n"
        << "    \"burst_duration_ms\": "
        << config.burst.duration_ms
        << ",\n"
        << "    \"burst_runtime_share_pct\": "
        << config.burst.runtime_share_pct
        << ",\n"
        << "    \"stale_threshold_ms\": "
        << config.stale_threshold_ms
        << ",\n"
        << "    \"backpressure_policy\": \""
        << to_string(
               config.backpressure_policy
           )
        << "\",\n"
        << "    \"run_duration_seconds\": "
        << config.run_duration_seconds
        << ",\n"
        << "    \"seed\": "
        << config.seed
        << "\n"
        << "  },\n"
        << "  \"observed\": {\n"
        << "    \"measurement_seconds\": "
        << result.measurement_seconds
        << ",\n"
        << "    \"requested_average_ingress_rate\": "
        << result.requested_average_ingress_rate
        << ",\n"
        << "    \"observed_ingress_rate\": "
        << result.observed_ingress_rate
        << ",\n"
        << "    \"observed_processing_rate\": "
        << result.observed_processing_rate
        << ",\n"
        << "    \"target_attainment_pct\": "
        << result.target_attainment_pct
        << ",\n"
        << "    \"generator_limited\": "
        << (
            result.generator_limited
                ? "true"
                : "false"
        )
        << ",\n"
        << "    \"producer_throttled_by_backpressure\": "
        << (
            result.
                producer_throttled_by_backpressure
                ? "true"
                : "false"
        )
        << ",\n"
        << "    \"generated\": "
        << result.generated
        << ",\n"
        << "    \"enqueued\": "
        << result.enqueued
        << ",\n"
        << "    \"processed\": "
        << result.processed
        << ",\n"
        << "    \"dropped\": "
        << result.dropped
        << ",\n"
        << "    \"coalesced\": "
        << result.coalesced
        << ",\n"
        << "    \"blocked_waits\": "
        << result.blocked_waits
        << ",\n"
        << "    \"peak_queue_depth\": "
        << result.peak_queue_depth
        << "\n"
        << "  },\n"
        << "  \"latency\": {\n";

    write_percentiles(
        json,
        "event_age",
        result.event_age,
        true
    );

    write_percentiles(
        json,
        "queue_residence",
        result.queue_residence,
        false
    );

    json
        << "  },\n"
        << "  \"environment\": {\n"
        << "    \"os\": \""
        << os_name()
        << "\",\n"
        << "    \"compiler\": \""
        << compiler_name()
        << "\",\n"
        << "    \"build_type\": \""
        << build_type()
        << "\",\n"
        << "    \"cpp_standard\": "
        << __cplusplus
        << ",\n"
        << "    \"logical_concurrency\": "
        << std::thread::
               hardware_concurrency()
        << "\n"
        << "  }\n"
        << "}\n";

    json.close();

    if (!json) {
        throw std::runtime_error(
            "failed while writing JSON report"
        );
    }

    std::ofstream csv{
        csv_path,
        std::ios::binary
    };

    if (!csv) {
        throw std::runtime_error(
            "unable to open CSV report"
        );
    }

    csv
        << std::fixed
        << std::setprecision(3);

    csv
        << "run_id,mode,"
           "requested_avg_ingress_rate,"
           "observed_ingress_rate,"
           "observed_processing_rate,"
           "target_attainment_pct,"
           "generator_limited,"
           "producer_throttled_by_backpressure,"
           "generated,enqueued,processed,"
           "dropped,coalesced,blocked_waits,"
           "peak_queue_depth,"
           "event_age_p50_us,"
           "event_age_p95_us,"
           "event_age_p99_us,"
           "event_age_p999_us,"
           "event_age_max_us,"
           "queue_residence_p50_us,"
           "queue_residence_p95_us,"
           "queue_residence_p99_us,"
           "queue_residence_p999_us,"
           "queue_residence_max_us\n";

    csv
        << run_id
        << ','
        << to_string(config.mode)
        << ','
        << result.requested_average_ingress_rate
        << ','
        << result.observed_ingress_rate
        << ','
        << result.observed_processing_rate
        << ','
        << result.target_attainment_pct
        << ','
        << (
            result.generator_limited
                ? 1
                : 0
        )
        << ','
        << (
            result.
                producer_throttled_by_backpressure
                ? 1
                : 0
        )
        << ','
        << result.generated
        << ','
        << result.enqueued
        << ','
        << result.processed
        << ','
        << result.dropped
        << ','
        << result.coalesced
        << ','
        << result.blocked_waits
        << ','
        << result.peak_queue_depth
        << ','
        << result.event_age.p50_us
        << ','
        << result.event_age.p95_us
        << ','
        << result.event_age.p99_us
        << ','
        << result.event_age.p999_us
        << ','
        << result.event_age.max_us
        << ','
        << result.queue_residence.p50_us
        << ','
        << result.queue_residence.p95_us
        << ','
        << result.queue_residence.p99_us
        << ','
        << result.queue_residence.p999_us
        << ','
        << result.queue_residence.max_us
        << '\n';

    csv.close();

    if (!csv) {
        throw std::runtime_error(
            "failed while writing CSV report"
        );
    }

    return ReportPaths{
        run_id,
        json_path,
        csv_path
    };
}

}  // namespace market_pipeline