#include <hfile/writer.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#if !defined(_WIN32) && !defined(_WIN64)
#include <csignal>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace fs = std::filesystem;

namespace {

struct Options {
    std::string mode;
    fs::path    output_dir;
    bool        verify_no_corrupt_files{false};
    size_t      rows{200000};
    size_t      value_size{4096};
    int         poll_timeout_ms{5000};
};

void print_usage() {
    std::cerr
        << "Usage: hfile-chaos --mode <kill-during-write|disk-full-sim> "
        << "--output-dir <dir> [--verify-no-corrupt-files] [--rows N] "
        << "[--value-size N] [--poll-timeout-ms N]\n";
}

std::optional<Options> parse_args(int argc, char** argv) {
    Options opts;
    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];
        auto next_value = [&](const char* name) -> std::optional<std::string> {
            if (i + 1 >= argc) {
                std::cerr << "Missing value for " << name << "\n";
                return std::nullopt;
            }
            return std::string(argv[++i]);
        };

        if (arg == "--mode") {
            auto v = next_value("--mode");
            if (!v) return std::nullopt;
            opts.mode = *v;
        } else if (arg == "--output-dir") {
            auto v = next_value("--output-dir");
            if (!v) return std::nullopt;
            opts.output_dir = *v;
        } else if (arg == "--rows") {
            auto v = next_value("--rows");
            if (!v) return std::nullopt;
            opts.rows = static_cast<size_t>(std::stoull(*v));
        } else if (arg == "--value-size") {
            auto v = next_value("--value-size");
            if (!v) return std::nullopt;
            opts.value_size = static_cast<size_t>(std::stoull(*v));
        } else if (arg == "--poll-timeout-ms") {
            auto v = next_value("--poll-timeout-ms");
            if (!v) return std::nullopt;
            opts.poll_timeout_ms = std::stoi(*v);
        } else if (arg == "--verify-no-corrupt-files") {
            opts.verify_no_corrupt_files = true;
        } else {
            std::cerr << "Unknown argument: " << arg << "\n";
            return std::nullopt;
        }
    }

    if (opts.mode.empty() || opts.output_dir.empty()) return std::nullopt;
    return opts;
}

std::string row_key_for(size_t i) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "row_%012zu", i);
    return std::string(buf);
}

hfile::Status write_workload(const fs::path& hfile_path,
                             size_t rows,
                             size_t value_size,
                             const hfile::WriterOptions& options,
                             bool slow_mode) {
    auto [writer, status] = hfile::HFileWriter::builder()
        .set_path(hfile_path.string())
        .set_column_family(options.column_family)
        .set_compression(options.compression)
        .set_block_size(options.block_size)
        .set_data_block_encoding(options.data_block_encoding)
        .set_bloom_type(options.bloom_type)
        .set_sort_mode(options.sort_mode)
        .set_fsync_policy(options.fsync_policy)
        .set_error_policy(options.error_policy)
        .set_max_error_count(options.max_error_count)
        .set_max_row_key_bytes(options.max_row_key_bytes)
        .set_max_value_bytes(options.max_value_bytes)
        .set_max_memory(options.max_memory_bytes)
        .set_min_free_disk(options.min_free_disk_bytes)
        .set_disk_check_interval(options.disk_check_interval_bytes)
        .build();
    if (!status.ok()) return status;

    std::vector<uint8_t> family{'c', 'f'};
    std::vector<uint8_t> qualifier{'q'};
    std::vector<uint8_t> value(value_size, 'x');

    for (size_t i = 0; i < rows; ++i) {
        auto row_str = row_key_for(i);
        std::vector<uint8_t> row(row_str.begin(), row_str.end());
        auto append_status = writer->append(
            row, family, qualifier,
            static_cast<int64_t>(rows - i),
            value);
        if (!append_status.ok()) return append_status;
        if (slow_mode && (i % 32) == 0)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    return writer->finish();
}

bool verify_output_is_clean(const fs::path& output_dir, const fs::path& final_path) {
    if (fs::exists(final_path)) {
        std::cerr << "Visible final file exists unexpectedly: " << final_path << "\n";
        return false;
    }
    if (!fs::exists(output_dir)) return true;

    for (const auto& entry : fs::recursive_directory_iterator(output_dir)) {
        if (!entry.is_regular_file()) continue;
        if (entry.path().parent_path().filename() == ".tmp") continue;
        std::cerr << "Unexpected visible file remains: " << entry.path() << "\n";
        return false;
    }
    return true;
}

bool verify_recovery_write(const Options& opts, const fs::path& output_dir) {
    fs::path recovered_path = output_dir / "recovered.hfile";
    hfile::WriterOptions wo;
    wo.column_family = "cf";
    wo.sort_mode = hfile::WriterOptions::SortMode::PreSortedVerified;
    wo.fsync_policy = hfile::FsyncPolicy::Safe;
    wo.error_policy = hfile::ErrorPolicy::Strict;
    auto status = write_workload(recovered_path, 128, opts.value_size, wo, false);
    if (!status.ok()) {
        std::cerr << "Recovery write failed: " << status.message() << "\n";
        return false;
    }
    if (!fs::exists(recovered_path)) {
        std::cerr << "Recovery write did not create final file\n";
        return false;
    }
    return true;
}

int run_disk_full_sim(const Options& opts) {
    std::error_code ec;
    fs::remove_all(opts.output_dir, ec);
    fs::create_directories(opts.output_dir, ec);
    if (ec) {
        std::cerr << "Failed to create output dir: " << ec.message() << "\n";
        return 2;
    }

    fs::path hfile_path = opts.output_dir / "disk-full.hfile";
    hfile::WriterOptions wo;
    wo.column_family = "cf";
    wo.sort_mode = hfile::WriterOptions::SortMode::PreSortedVerified;
    wo.fsync_policy = hfile::FsyncPolicy::Safe;
    wo.error_policy = hfile::ErrorPolicy::Strict;
    wo.min_free_disk_bytes = std::numeric_limits<size_t>::max() / 2;
    wo.disk_check_interval_bytes = 1;

    auto status = write_workload(hfile_path, 1, opts.value_size, wo, false);
    if (status.ok() || status.message().find("DISK_SPACE_EXHAUSTED") == std::string::npos) {
        std::cerr << "Expected DISK_SPACE_EXHAUSTED, got: "
                  << (status.ok() ? "OK" : status.message()) << "\n";
        return 3;
    }

    if (opts.verify_no_corrupt_files &&
        !verify_output_is_clean(opts.output_dir, hfile_path)) {
        return 4;
    }

    fs::remove_all(opts.output_dir, ec);
    std::cout << "disk-full-sim passed\n";
    return 0;
}

#if !defined(_WIN32) && !defined(_WIN64)
int run_kill_during_write(const Options& opts) {
    std::error_code ec;
    fs::remove_all(opts.output_dir, ec);
    fs::create_directories(opts.output_dir, ec);
    if (ec) {
        std::cerr << "Failed to create output dir: " << ec.message() << "\n";
        return 2;
    }

    fs::path hfile_path = opts.output_dir / "kill-during-write.hfile";
    pid_t pid = ::fork();
    if (pid < 0) {
        std::perror("fork");
        return 2;
    }

    if (pid == 0) {
        hfile::WriterOptions wo;
        wo.column_family = "cf";
        wo.sort_mode = hfile::WriterOptions::SortMode::PreSortedVerified;
        wo.fsync_policy = hfile::FsyncPolicy::Safe;
        wo.error_policy = hfile::ErrorPolicy::Strict;
        auto status = write_workload(hfile_path, opts.rows, opts.value_size, wo, true);
        _exit(status.ok() ? 0 : 1);
    }

    fs::path tmp_dir = opts.output_dir / ".tmp";
    bool observed_temp_growth = false;
    int elapsed_ms = 0;
    while (elapsed_ms < opts.poll_timeout_ms) {
        int child_status = 0;
        pid_t wait_result = ::waitpid(pid, &child_status, WNOHANG);
        if (wait_result == pid) {
            std::cerr << "Writer process exited before chaos injection\n";
            return 3;
        }

        if (fs::exists(tmp_dir)) {
            for (const auto& entry : fs::directory_iterator(tmp_dir)) {
                if (!entry.is_regular_file()) continue;
                auto size = entry.file_size(ec);
                if (!ec && size > 0) {
                    observed_temp_growth = true;
                    break;
                }
            }
        }
        if (observed_temp_growth) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        elapsed_ms += 10;
    }

    if (!observed_temp_growth) {
        ::kill(pid, SIGKILL);
        ::waitpid(pid, nullptr, 0);
        std::cerr << "Timed out waiting for temp file growth\n";
        return 4;
    }

    ::kill(pid, SIGKILL);
    ::waitpid(pid, nullptr, 0);

    if (opts.verify_no_corrupt_files &&
        !verify_output_is_clean(opts.output_dir, hfile_path)) {
        return 5;
    }

    fs::remove_all(opts.output_dir / ".tmp", ec);
    if (ec) {
        std::cerr << "Failed to clean temp directory: " << ec.message() << "\n";
        return 6;
    }
    if (!verify_recovery_write(opts, opts.output_dir))
        return 7;
    fs::remove_all(opts.output_dir, ec);
    std::cout << "kill-during-write passed\n";
    return 0;
}
#endif

}  // namespace

int main(int argc, char** argv) {
    auto opts = parse_args(argc, argv);
    if (!opts) {
        print_usage();
        return 1;
    }

    if (opts->mode == "disk-full-sim")
        return run_disk_full_sim(*opts);

    if (opts->mode == "kill-during-write") {
#if defined(_WIN32) || defined(_WIN64)
        std::cerr << "kill-during-write is unsupported on Windows\n";
        return 2;
#else
        return run_kill_during_write(*opts);
#endif
    }

    std::cerr << "Unsupported mode: " << opts->mode << "\n";
    print_usage();
    return 1;
}
