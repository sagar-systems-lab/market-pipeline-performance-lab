#include "market_pipeline/report.hpp"

#include "market_pipeline/pipeline_comparison.hpp"

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
        << "comparison-"
        << epoch_ns
        << '-'
        << seed;

    return stream.str();
}

void write_backend_json(
    std::ostream& output,
    const QueueBackend backend,
    const RunResult& result,
    const bool trailing_comma
) {
    output
        << "    \""
        << to_string(backend)
        << "\": {\n"
        << "      \"measurement_seconds\": "
        << result.measurement_seconds
        << ",\n"
        << "      \"requested_average_ingress_rate\": "
        << result.requested_average_ingress_rate
        << ",\n"
        << "      \"observed_ingress_rate\": "
        << result.observed_ingress_rate
        << ",\n"
        << "      \"observed_processing_rate\": "
        << result.observed_processing_rate
        << ",\n"
        << "      \"target_attainment_pct\": "
        << result.target_attainment_pct
        << ",\n"
        << "      \"generator_limited\": "
        << (
            result.generator_limited
                ? "true"
                : "false"
        )
        << ",\n"
        << "      \"producer_throttled_by_backpressure\": "
        << (
            result.
                producer_throttled_by_backpressure
                ? "true"
                : "false"
        )
        << ",\n"
        << "      \"generated\": "
        << result.generated
        << ",\n"
        << "      \"processed\": "
        << result.processed
        << ",\n"
        << "      \"dropped\": "
        << result.dropped
        << ",\n"
        << "      \"coalesced\": "
        << result.coalesced
        << ",\n"
        << "      \"blocked_waits\": "
        << result.blocked_waits
        << ",\n"
        << "      \"peak_queue_depth\": "
        << result.peak_queue_depth
        << ",\n"
        << "      \"event_age_p50_us\": "
        << result.event_age.p50_us
        << ",\n"
        << "      \"event_age_p95_us\": "
        << result.event_age.p95_us
        << ",\n"
        << "      \"event_age_p99_us\": "
        << result.event_age.p99_us
        << ",\n"
        << "      \"event_age_p99_9_us\": "
        << result.event_age.p999_us
        << ",\n"
        << "      \"event_age_max_us\": "
        << result.event_age.max_us
        << ",\n"
        << "      \"queue_residence_p50_us\": "
        << result.queue_residence.p50_us
        << ",\n"
        << "      \"queue_residence_p95_us\": "
        << result.queue_residence.p95_us
        << ",\n"
        << "      \"queue_residence_p99_us\": "
        << result.queue_residence.p99_us
        << ",\n"
        << "      \"queue_residence_p99_9_us\": "
        << result.queue_residence.p999_us
        << ",\n"
        << "      \"queue_residence_max_us\": "
        << result.queue_residence.max_us
        << "\n"
        << "    }"
        << (
            trailing_comma
                ? ","
                : ""
        )
        << '\n';
}

void write_backend_csv(
    std::ostream& output,
    const std::string& run_id,
    const QueueBackend backend,
    const RunResult& result
) {
    output
        << run_id
        << ','
        << to_string(backend)
        << ','
        << result.measurement_seconds
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
}

}  // namespace

ReportPaths write_comparison_reports(
    const ScenarioConfig& config,
    const PipelineComparisonResult& result,
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
            "unable to open comparison JSON report"
        );
    }

    json
        << std::fixed
        << std::setprecision(3)
        << "{\n"
        << "  \"schema\": "
           "\"market-pipeline-queue-comparison.v1\",\n"
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
        << "    \"backpressure_policy\": \""
        << to_string(
               config.backpressure_policy
           )
        << "\",\n"
        << "    \"seed\": "
        << config.seed
        << "\n"
        << "  },\n"
        << "  \"execution_order_first_backend\": \""
        << to_string(
               result.first_backend
           )
        << "\",\n"
        << "  \"backends\": {\n";

    write_backend_json(
        json,
        QueueBackend::DynamicDeque,
        result.dynamic_deque,
        true
    );

    write_backend_json(
        json,
        QueueBackend::PreallocatedRing,
        result.preallocated_ring,
        false
    );

    json
        << "  },\n"
        << "  \"ring_vs_deque_delta_pct\": {\n"
        << "    \"observed_ingress_rate\": "
        << result.
               ring_vs_deque_ingress_rate_pct
        << ",\n"
        << "    \"observed_processing_rate\": "
        << result.
               ring_vs_deque_processing_rate_pct
        << ",\n"
        << "    \"event_age_p99\": "
        << result.
               ring_vs_deque_event_age_p99_pct
        << ",\n"
        << "    \"queue_residence_p99\": "
        << result.
               ring_vs_deque_queue_residence_p99_pct
        << "\n"
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
            "failed while writing comparison JSON report"
        );
    }

    std::ofstream csv{
        csv_path,
        std::ios::binary
    };

    if (!csv) {
        throw std::runtime_error(
            "unable to open comparison CSV report"
        );
    }

    csv
        << std::fixed
        << std::setprecision(3)
        << "run_id,backend,"
           "measurement_seconds,"
           "requested_average_ingress_rate,"
           "observed_ingress_rate,"
           "observed_processing_rate,"
           "target_attainment_pct,"
           "generator_limited,"
           "producer_throttled_by_backpressure,"
           "generated,processed,dropped,"
           "coalesced,blocked_waits,"
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

    write_backend_csv(
        csv,
        run_id,
        QueueBackend::DynamicDeque,
        result.dynamic_deque
    );

    write_backend_csv(
        csv,
        run_id,
        QueueBackend::PreallocatedRing,
        result.preallocated_ring
    );

    csv.close();

    if (!csv) {
        throw std::runtime_error(
            "failed while writing comparison CSV report"
        );
    }

    return ReportPaths{
        run_id,
        json_path,
        csv_path
    };
}

}  // namespace market_pipeline