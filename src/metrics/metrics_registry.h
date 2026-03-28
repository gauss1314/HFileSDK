#pragma once

#include <hfile/status.h>
#include <atomic>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <algorithm>
#include <functional>
#include <chrono>
#include <cmath>
#include <memory>
#include <thread>

namespace hfile {

// ─── Snapshot types (returned by MetricsRegistry::snapshot()) ────────────────

struct HistogramSnapshot {
    int64_t count{0};
    double  sum{0};
    double  min{0};
    double  max{0};
    double  p50{0};   // median
    double  p95{0};
    double  p99{0};
};

struct MetricsSnapshot {
    std::unordered_map<std::string, int64_t>         counters;
    std::unordered_map<std::string, double>           gauges;
    std::unordered_map<std::string, HistogramSnapshot> histograms;

    int64_t counter(const std::string& name) const {
        auto it = counters.find(name);
        return it != counters.end() ? it->second : 0;
    }
    double gauge(const std::string& name) const {
        auto it = gauges.find(name);
        return it != gauges.end() ? it->second : 0.0;
    }
};

// ─── MetricsRegistry ─────────────────────────────────────────────────────────

/// Lightweight metrics registry — no external dependencies.
///
/// Metric types:
///   Counter   — monotonically increasing int64 (KV count, bytes written)
///   Gauge     — current double value (memory used, open files)
///   Histogram — distribution of durations / sizes (latency, block size)
///
/// All operations are thread-safe.
class MetricsRegistry {
public:
    MetricsRegistry()  = default;

    // Non-copyable
    MetricsRegistry(const MetricsRegistry&)            = delete;
    MetricsRegistry& operator=(const MetricsRegistry&) = delete;

    // ── Counter ──────────────────────────────────────────────────────────────

    void increment(const std::string& name, int64_t delta = 1) noexcept {
        get_counter(name).fetch_add(delta, std::memory_order_relaxed);
    }

    int64_t counter(const std::string& name) const noexcept {
        std::lock_guard<std::mutex> lk{mu_};
        auto it = counters_.find(name);
        return it != counters_.end()
            ? it->second->load(std::memory_order_relaxed) : 0;
    }

    // ── Gauge ─────────────────────────────────────────────────────────────────

    void set_gauge(const std::string& name, double value) noexcept {
        std::lock_guard<std::mutex> lk{mu_};
        gauges_[name] = value;
    }

    void add_gauge(const std::string& name, double delta) noexcept {
        std::lock_guard<std::mutex> lk{mu_};
        gauges_[name] += delta;
    }

    double gauge(const std::string& name) const noexcept {
        std::lock_guard<std::mutex> lk{mu_};
        auto it = gauges_.find(name);
        return it != gauges_.end() ? it->second : 0.0;
    }

    // ── Histogram ────────────────────────────────────────────────────────────

    void observe(const std::string& name, double value) noexcept {
        std::lock_guard<std::mutex> lk{mu_};
        auto& h = histograms_[name];
        h.push_back(value);
    }

    HistogramSnapshot histogram(const std::string& name) const noexcept {
        std::lock_guard<std::mutex> lk{mu_};
        auto it = histograms_.find(name);
        if (it == histograms_.end() || it->second.empty()) return {};
        std::vector<double> sorted = it->second;
        std::sort(sorted.begin(), sorted.end());
        HistogramSnapshot s;
        s.count = static_cast<int64_t>(sorted.size());
        s.min   = sorted.front();
        s.max   = sorted.back();
        double sum = 0; for (double v : sorted) sum += v;
        s.sum   = sum;
        auto pct = [&](double p) {
            size_t idx = static_cast<size_t>(
                std::ceil(p * sorted.size()) - 1);
            idx = std::min(idx, sorted.size() - 1);
            return sorted[idx];
        };
        s.p50 = pct(0.50);
        s.p95 = pct(0.95);
        s.p99 = pct(0.99);
        return s;
    }

    // ── Snapshot ─────────────────────────────────────────────────────────────

    MetricsSnapshot snapshot() const {
        MetricsSnapshot snap;
        std::lock_guard<std::mutex> lk{mu_};
        for (const auto& [name, ctr] : counters_)
            snap.counters[name] = ctr->load(std::memory_order_relaxed);
        snap.gauges = gauges_;
        for (const auto& [name, vals] : histograms_)
            snap.histograms[name] = histogram_unlocked(vals);
        return snap;
    }

    // ── Periodic report callback ──────────────────────────────────────────────

    using ReportCallback = std::function<void(const MetricsSnapshot&)>;

    /// Register a callback invoked every `interval` seconds.
    /// Spawns a background thread; stopped when the registry is destroyed.
    void set_report_callback(ReportCallback cb,
                              std::chrono::seconds interval) {
        report_cb_       = std::move(cb);
        report_interval_ = interval;
        report_running_  = true;
        report_thread_   = std::thread([this] {
            while (report_running_) {
                std::this_thread::sleep_for(report_interval_);
                if (report_running_ && report_cb_)
                    report_cb_(snapshot());
            }
        });
    }

    void stop_report_thread() {
        report_running_ = false;
        if (report_thread_.joinable()) report_thread_.join();
    }

    ~MetricsRegistry() { stop_report_thread(); }

private:
    std::atomic<int64_t>& get_counter(const std::string& name) {
        std::lock_guard<std::mutex> lk{mu_};
        auto it = counters_.find(name);
        if (it != counters_.end()) return *it->second;
        auto ptr = std::make_unique<std::atomic<int64_t>>(0);
        auto& ref = *ptr;
        counters_.emplace(name, std::move(ptr));
        return ref;
    }

    static HistogramSnapshot histogram_unlocked(const std::vector<double>& vals) {
        if (vals.empty()) return {};
        std::vector<double> s = vals;
        std::sort(s.begin(), s.end());
        HistogramSnapshot h;
        h.count = static_cast<int64_t>(s.size());
        h.min = s.front(); h.max = s.back();
        double sum = 0; for (double v : s) sum += v; h.sum = sum;
        auto pct = [&](double p) {
            size_t idx = static_cast<size_t>(std::ceil(p*s.size()) - 1);
            return s[std::min(idx, s.size()-1)];
        };
        h.p50 = pct(0.50); h.p95 = pct(0.95); h.p99 = pct(0.99);
        return h;
    }

    mutable std::mutex mu_;
    std::unordered_map<std::string, std::unique_ptr<std::atomic<int64_t>>> counters_;
    std::unordered_map<std::string, double>               gauges_;
    std::unordered_map<std::string, std::vector<double>>  histograms_;

    ReportCallback           report_cb_;
    std::chrono::seconds     report_interval_{60};
    std::atomic<bool>        report_running_{false};
    std::thread              report_thread_;
};

// ─── RAII timer helper ────────────────────────────────────────────────────────

/// Records elapsed microseconds into a histogram on destruction.
struct ScopedTimer {
    ScopedTimer(MetricsRegistry& reg, std::string name)
        : reg_{reg}, name_{std::move(name)}
        , start_{std::chrono::steady_clock::now()} {}

    ~ScopedTimer() {
        auto elapsed = std::chrono::steady_clock::now() - start_;
        double us = std::chrono::duration<double, std::micro>(elapsed).count();
        reg_.observe(name_, us);
    }

private:
    MetricsRegistry&                           reg_;
    std::string                                name_;
    std::chrono::steady_clock::time_point      start_;
};

// ─── Well-known metric name constants ────────────────────────────────────────

namespace metrics {
// Counters
inline constexpr const char* kKVWritten      = "hfile.kv.written.count";
inline constexpr const char* kKVBytes        = "hfile.kv.written.bytes";
inline constexpr const char* kKVSkipped      = "hfile.kv.skipped.count";
inline constexpr const char* kBlocksWritten  = "hfile.blocks.written.count";
inline constexpr const char* kFilesCompleted = "hfile.files.completed.count";
inline constexpr const char* kFilesFailed    = "hfile.files.failed.count";
// Gauges
inline constexpr const char* kMemoryUsed     = "hfile.memory.used.bytes";
inline constexpr const char* kMemoryPeak     = "hfile.memory.peak.bytes";
inline constexpr const char* kDiskTempBytes  = "hfile.disk.temp.bytes";
inline constexpr const char* kOpenFiles      = "hfile.io.open_files.count";
// Histograms
inline constexpr const char* kEncodeLatUs    = "hfile.encode.latency_us";
inline constexpr const char* kCompressLatUs  = "hfile.compress.latency_us";
inline constexpr const char* kIOWriteLatUs   = "hfile.io.write.latency_us";
inline constexpr const char* kFsyncLatUs     = "hfile.io.fsync.latency_us";
inline constexpr const char* kBatchLatMs     = "hfile.batch.process.latency_ms";
} // namespace metrics

} // namespace hfile
