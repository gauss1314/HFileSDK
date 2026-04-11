#include <hfile/bulk_load_writer.h>
#include <hfile/writer.h>

#include "arrow/arrow_to_kv_converter.h"
#include "partition/cf_grouper.h"
#include "metrics/metrics_registry.h"

#include <filesystem>
#include <unordered_map>
#include <sstream>
#include <iomanip>
#include <mutex>
#include <thread>
#include <vector>
#include <queue>
#include <condition_variable>
#include <functional>
#include <future>
#include <atomic>
#include <cstdio>

namespace hfile {

namespace fs = std::filesystem;

namespace log {
static void info (const std::string& m){ fprintf(stderr,"[INFO]  hfile: %s\n",m.c_str()); }
static void warn (const std::string& m){ fprintf(stderr,"[WARN]  hfile: %s\n",m.c_str()); }
static void error(const std::string& m){ fprintf(stderr,"[ERROR] hfile: %s\n",m.c_str()); }
}

// ─── Thread pool ─────────────────────────────────────────────────────────────

class ThreadPool {
public:
    explicit ThreadPool(int n) : stop_{false} {
        for (int i = 0; i < n; ++i)
            workers_.emplace_back([this] {
                for (;;) {
                    std::function<void()> task;
                    { std::unique_lock<std::mutex> lk(mu_);
                      cv_.wait(lk, [this]{ return stop_ || !tasks_.empty(); });
                      if (stop_ && tasks_.empty()) return;
                      task = std::move(tasks_.front()); tasks_.pop(); }
                    task();
                }
            });
    }
    ~ThreadPool() {
        { std::lock_guard<std::mutex> lk(mu_); stop_ = true; }
        cv_.notify_all();
        for (auto& w : workers_) w.join();
    }
    template<typename F>
    std::future<void> submit(F&& f) {
        auto t = std::make_shared<std::packaged_task<void()>>(std::forward<F>(f));
        auto fut = t->get_future();
        { std::lock_guard<std::mutex> lk(mu_); tasks_.emplace([t]{ (*t)(); }); }
        cv_.notify_one();
        return fut;
    }
private:
    std::vector<std::thread>       workers_;
    std::queue<std::function<void()>> tasks_;
    std::mutex                     mu_;
    std::condition_variable        cv_;
    bool                           stop_;
};

// ─── WriterKey ────────────────────────────────────────────────────────────────

struct WriterKey {
    std::string cf; int region;
    bool operator==(const WriterKey& o) const noexcept {
        return cf == o.cf && region == o.region;
    }
};
struct WriterKeyHash {
    size_t operator()(const WriterKey& k) const noexcept {
        return std::hash<std::string>{}(k.cf) ^ (std::hash<int>{}(k.region) << 16);
    }
};

struct ActiveWriter {
    std::unique_ptr<HFileWriter> writer;
    std::string                  rel_path;
    uint64_t                     last_use{0};
    bool                         has_last_kv{false};
    OwnedKeyValue                last_kv;
};

static Status validate_batch_kv(const KeyValue& kv, const WriterOptions& opts) {
    if (kv.row.empty())
        return Status::InvalidArg("ROW_KEY_EMPTY: row key must not be empty");
    if (kv.row.size() > opts.max_row_key_bytes)
        return Status::InvalidArg("ROW_KEY_TOO_LONG");
    if (kv.value.size() > opts.max_value_bytes)
        return Status::InvalidArg("VALUE_TOO_LARGE");
    if (kv.timestamp < 0)
        return Status::InvalidArg("NEGATIVE_TIMESTAMP");
    return Status::OK();
}

// ─── BulkLoadWriterImpl ───────────────────────────────────────────────────────

class BulkLoadWriterImpl {
public:
    BulkLoadWriterImpl(std::string                        table_name,
                       std::vector<std::string>           cfs,
                       std::string                        output_dir,
                       std::unique_ptr<RegionPartitioner> partitioner,
                       WriterOptions                      opts,
                       int                                parallelism,
                       std::function<void(const ProgressInfo&)> progress_cb,
                       std::chrono::seconds               progress_interval)
        : table_name_{std::move(table_name)},
          column_families_{std::move(cfs)},
          output_dir_{std::move(output_dir)},
          partitioner_{std::move(partitioner)},
          opts_{std::move(opts)},
          parallelism_{parallelism},
          progress_cb_{std::move(progress_cb)},
          progress_interval_{progress_interval},
          start_time_{std::chrono::steady_clock::now()} {

        for (const auto& cf : column_families_)
            cf_grouper_.register_cf(cf);
        cf_grouper_.rebuild_list();

        fs::create_directories(output_dir_);
        for (const auto& cf : column_families_)
            fs::create_directories(fs::path(output_dir_) / cf);

        // Start progress reporter thread if callback provided
        if (progress_cb_) {
            progress_running_ = true;
            progress_thread_ = std::thread([this] {
                while (progress_running_) {
                    std::this_thread::sleep_for(progress_interval_);
                    if (progress_running_ && progress_cb_) {
                        try {
                            progress_cb_(make_progress_info());
                        } catch (const std::exception& e) {
                            log::error("Progress callback failed: " + std::string(e.what()));
                        } catch (...) {
                            log::error("Progress callback failed: unknown exception");
                        }
                    }
                }
            });
        }

        log::info("BulkLoad started: dir=" + output_dir_ +
                  " cfs=" + std::to_string(column_families_.size()) +
                  " parallelism=" + std::to_string(parallelism_));
    }

    ~BulkLoadWriterImpl() {
        progress_running_ = false;
        if (progress_thread_.joinable()) progress_thread_.join();
    }

    Status write_kv(const KeyValue& kv) {
        std::string_view fam(
            reinterpret_cast<const char*>(kv.family.data()), kv.family.size());
        if (!cf_grouper_.has_cf(fam))
            return Status::InvalidArg("Unknown column family: " + std::string(fam));

        int region = partitioner_->region_for(kv.row);
        WriterKey wk{std::string(fam), region};

        std::lock_guard<std::mutex> lk(writers_mu_);
        auto it = writers_.find(wk);
        if (it == writers_.end()) {
            HFILE_RETURN_IF_ERROR(ensure_writer_slot_locked(wk));
            auto [w, rel_path, s] = open_writer(wk.cf, region);
            if (!s.ok()) return s;
            ActiveWriter state;
            state.writer = std::move(w);
            state.rel_path = std::move(rel_path);
            state.last_use = ++use_clock_;
            it = writers_.emplace(wk, std::move(state)).first;
        }
        it->second.last_use = ++use_clock_;
        auto s = it->second.writer->append(kv);
        if (s.ok()) {
            kv_written_.fetch_add(1, std::memory_order_relaxed);
            bytes_written_.fetch_add(kv.encoded_size(), std::memory_order_relaxed);
            it->second.last_kv.row.assign(kv.row.begin(), kv.row.end());
            it->second.last_kv.family.assign(kv.family.begin(), kv.family.end());
            it->second.last_kv.qualifier.assign(kv.qualifier.begin(), kv.qualifier.end());
            it->second.last_kv.timestamp = kv.timestamp;
            it->second.last_kv.key_type = kv.key_type;
            it->second.has_last_kv = true;
        } else {
            kv_skipped_.fetch_add(1, std::memory_order_relaxed);
            log::warn("KV skipped: " + s.message());
            // Respect error policy: SkipRow = swallow; Strict = propagate
            if (opts_.error_policy == ErrorPolicy::Strict) return s;
        }
        return Status::OK();
    }

    Status write_batch(const ::arrow::RecordBatch& batch, MappingMode mode) {
        auto start = std::chrono::steady_clock::now();
        std::vector<OwnedKeyValue> staged;
        if (opts_.error_policy == ErrorPolicy::SkipBatch)
            staged.reserve(static_cast<size_t>(std::max<int64_t>(0, batch.num_rows())));
        auto cb = [this, &staged](const KeyValue& kv) {
            if (opts_.error_policy == ErrorPolicy::SkipBatch) {
                OwnedKeyValue owned;
                owned.row.assign(kv.row.begin(), kv.row.end());
                owned.family.assign(kv.family.begin(), kv.family.end());
                owned.qualifier.assign(kv.qualifier.begin(), kv.qualifier.end());
                owned.timestamp = kv.timestamp;
                owned.key_type = kv.key_type;
                owned.value.assign(kv.value.begin(), kv.value.end());
                owned.tags.assign(kv.tags.begin(), kv.tags.end());
                owned.memstore_ts = kv.memstore_ts;
                staged.push_back(std::move(owned));
                return Status::OK();
            }
            return write_kv(kv);
        };
        Status s;
        switch (mode) {
        case MappingMode::WideTable: {
            arrow_convert::WideTableConfig cfg;
            cfg.column_family = column_families_.empty() ? "cf" : column_families_[0];
            s = arrow_convert::ArrowToKVConverter::convert_wide_table(batch, cfg, cb);
            break;
        }
        case MappingMode::TallTable:
            s = arrow_convert::ArrowToKVConverter::convert_tall_table(
                batch, arrow_convert::TallTableConfig{}, cb);
            break;
        case MappingMode::RawKV:
            s = arrow_convert::ArrowToKVConverter::convert_raw_kv(
                batch, "key", "value", cb);
            break;
        }
        batches_processed_.fetch_add(1, std::memory_order_relaxed);
        total_rows_.fetch_add(static_cast<int64_t>(batch.num_rows()),
                              std::memory_order_relaxed);
        if (!s.ok()) {
            if (opts_.error_policy == ErrorPolicy::SkipBatch) {
                kv_skipped_.fetch_add(batch.num_rows(), std::memory_order_relaxed);
                return Status::OK();
            }
            return s;
        }

        if (opts_.error_policy == ErrorPolicy::SkipBatch) {
            auto validation = prevalidate_batch(staged);
            if (!validation.ok()) {
                kv_skipped_.fetch_add(batch.num_rows(), std::memory_order_relaxed);
                log::warn("Batch skipped: " + validation.message());
                return Status::OK();
            }
        }

        for (const auto& kv : staged) {
            auto ws = write_kv(kv.as_view());
            if (!ws.ok()) return ws;
        }
        return Status::OK();
    }

    std::pair<BulkLoadResult, Status> finish() {
        // Stop progress reporter
        progress_running_ = false;
        if (progress_thread_.joinable()) progress_thread_.join();

        BulkLoadResult result;
        result.staging_dir = output_dir_;
        result.total_rows  = static_cast<uint64_t>(
            total_rows_.load(std::memory_order_relaxed));
        result.skipped_rows = static_cast<uint64_t>(
            kv_skipped_.load(std::memory_order_relaxed));
        result.files = completed_files_;
        result.failed_files = failed_files_;
        result.total_entries = completed_entries_;
        result.total_bytes = completed_bytes_;

        if (writers_.empty()) {
            result.elapsed = elapsed_ms();
            if (!result.failed_files.empty() && !result.files.empty())
                return {result, Status::IoError("PARTIAL_SUCCESS: " +
                    std::to_string(result.failed_files.size()) + " files failed")};
            if (!result.failed_files.empty())
                return {result, first_finish_error_.ok() ? Status::IoError("All files failed")
                                                         : first_finish_error_};
            return {result, Status::OK()};
        }

        std::vector<std::tuple<WriterKey, HFileWriter*, std::string>> work;
        {
            std::lock_guard<std::mutex> lk(writers_mu_);
            work.reserve(writers_.size());
            for (auto& [wk, w] : writers_)
                work.emplace_back(wk, w.writer.get(), w.rel_path);
        }

        const int n_threads = std::max(1, std::min(parallelism_,
                                                    static_cast<int>(work.size())));
        std::vector<Status>   statuses(work.size(), Status::OK());
        std::vector<uint64_t> entry_counts(work.size(), 0);

        if (n_threads == 1) {
            for (size_t i = 0; i < work.size(); ++i) {
                statuses[i]     = std::get<1>(work[i])->finish();
                entry_counts[i] = std::get<1>(work[i])->entry_count();
            }
        } else {
            ThreadPool pool(n_threads);
            std::vector<std::future<void>> futs;
            futs.reserve(work.size());
            for (size_t i = 0; i < work.size(); ++i) {
                futs.push_back(pool.submit([i, &work, &statuses, &entry_counts] {
                    statuses[i]     = std::get<1>(work[i])->finish();
                    entry_counts[i] = std::get<1>(work[i])->entry_count();
                }));
            }
            for (auto& f : futs) f.get();
        }

        // Collect succeeded and failed files separately (PARTIAL_SUCCESS support)
        Status first_error;
        for (size_t i = 0; i < work.size(); ++i) {
            const std::string& rel = std::get<2>(work[i]);
            if (statuses[i].ok()) {
                result.files.push_back(rel);
                result.total_entries += entry_counts[i];

                // Use the actual on-disk file size (compressed, post-finish).
                // Avoid bytes_written_ which is: (a) uncompressed cell bytes, not
                // HFile bytes, and (b) a global counter so adding it N times in a
                // loop overcounts by N×.
                fs::path file_path = fs::path(output_dir_) / rel;
                std::error_code ec;
                auto fsz = fs::file_size(file_path, ec);
                if (!ec) result.total_bytes += static_cast<uint64_t>(fsz);
                ++completed_files_count_;
            } else {
                result.failed_files.push_back(rel);
                log::error("HFile failed: " + rel + " — " + statuses[i].message());
                if (first_error.ok()) first_error = statuses[i];  // capture first failure
            }
        }

        {
            std::lock_guard<std::mutex> lk(writers_mu_);
            writers_.clear();
        }
        result.elapsed = elapsed_ms();

        log::info("BulkLoad finished: files=" + std::to_string(result.files.size()) +
                  " failed=" + std::to_string(result.failed_files.size()) +
                  " kvs=" + std::to_string(result.total_entries) +
                  " skipped=" + std::to_string(result.skipped_rows) +
                  " elapsed=" + std::to_string(result.elapsed.count()) + "ms");

        // Return PARTIAL_SUCCESS if some files failed but some succeeded
        if (!result.failed_files.empty() && !result.files.empty())
            return {result, Status::IoError("PARTIAL_SUCCESS: " +
                std::to_string(result.failed_files.size()) + " files failed")};
        if (!result.failed_files.empty())
            return {result, first_error.ok() ? Status::IoError("All files failed")
                                              : first_error};
        return {result, Status::OK()};
    }

private:
    Status prevalidate_batch(const std::vector<OwnedKeyValue>& staged) {
        std::unordered_map<WriterKey, OwnedKeyValue, WriterKeyHash> simulated_last;
        {
            std::lock_guard<std::mutex> lk(writers_mu_);
            for (const auto& [wk, state] : writers_) {
                if (state.has_last_kv)
                    simulated_last.emplace(wk, state.last_kv);
            }
        }

        for (const auto& kv : staged) {
            HFILE_RETURN_IF_ERROR(validate_batch_kv(kv.as_view(), opts_));
            std::string_view fam(
                reinterpret_cast<const char*>(kv.family.data()), kv.family.size());
            if (!cf_grouper_.has_cf(fam))
                return Status::InvalidArg("Unknown column family: " + std::string(fam));
            int region = partitioner_->region_for(kv.row);
            WriterKey wk{std::string(fam), region};
            auto it = simulated_last.find(wk);
            if (it != simulated_last.end() &&
                compare_keys(kv.as_view(), it->second.as_view()) <= 0)
                return Status::InvalidArg("SORT_ORDER_VIOLATION: batch KV out of order");
            simulated_last[wk] = kv;
        }
        return Status::OK();
    }

    Status ensure_writer_slot_locked(const WriterKey& incoming) {
        if (opts_.max_open_files == 0 || writers_.size() < opts_.max_open_files)
            return Status::OK();
        auto victim = writers_.end();
        for (auto it = writers_.begin(); it != writers_.end(); ++it) {
            if (victim == writers_.end() || it->second.last_use < victim->second.last_use)
                victim = it;
        }
        if (victim == writers_.end()) return Status::OK();
        auto status = victim->second.writer->finish();
        uint64_t entries = victim->second.writer->entry_count();
        std::string rel_path = victim->second.rel_path;
        writers_.erase(victim);
        if (!status.ok()) {
            failed_files_.push_back(rel_path);
            if (first_finish_error_.ok()) first_finish_error_ = status;
            return status;
        }
        completed_files_.push_back(rel_path);
        completed_entries_ += entries;
        fs::path file_path = fs::path(output_dir_) / rel_path;
        std::error_code ec;
        auto fsz = fs::file_size(file_path, ec);
        if (!ec) completed_bytes_ += static_cast<uint64_t>(fsz);
        ++completed_files_count_;
        return Status::OK();
    }

    std::string make_rel_path(const std::string& cf, int region) {
        WriterKey wk{cf, region};
        uint32_t seq = next_file_seq_[wk]++;
        std::ostringstream name;
        name << cf << "/hfile_region_"
             << std::setw(4) << std::setfill('0') << region;
        if (seq > 0)
            name << "_" << std::setw(4) << std::setfill('0') << seq;
        name << ".hfile";
        return name.str();
    }

    std::tuple<std::unique_ptr<HFileWriter>, std::string, Status>
    open_writer(const std::string& cf, int region) {
        std::string rel_path = make_rel_path(cf, region);
        fs::path file_path = fs::path(output_dir_) / rel_path;

        WriterOptions cf_opts       = opts_;
        cf_opts.column_family       = cf;
        cf_opts.sort_mode           = WriterOptions::SortMode::PreSortedVerified;
        auto error_callback = [this, cf_opts](const RowError& err) {
            if (cf_opts.error_policy == ErrorPolicy::SkipRow) {
                kv_skipped_.fetch_add(1, std::memory_order_relaxed);
            }
            if (cf_opts.error_callback) {
                cf_opts.error_callback(err);
            }
        };

        auto [writer, status] = HFileWriter::builder()
            .set_path(file_path.string())
            .set_column_family(cf)
            .set_compression(cf_opts.compression)
            .set_block_size(cf_opts.block_size)
            .set_data_block_encoding(cf_opts.data_block_encoding)
            .set_bloom_type(cf_opts.bloom_type)
            .set_sort_mode(cf_opts.sort_mode)
            .set_fsync_policy(cf_opts.fsync_policy)
            .set_error_policy(cf_opts.error_policy)
            .set_max_error_count(cf_opts.max_error_count)
            .set_error_callback(error_callback)
            .set_max_row_key_bytes(cf_opts.max_row_key_bytes)
            .set_max_value_bytes(cf_opts.max_value_bytes)
            .set_max_memory(cf_opts.max_memory_bytes)
            .set_min_free_disk(cf_opts.min_free_disk_bytes)
            .set_disk_check_interval(cf_opts.disk_check_interval_bytes)
            .build();
        return {std::move(writer), rel_path, status};
    }

    ProgressInfo make_progress_info() const {
        ProgressInfo p;
        p.total_kv_written    = kv_written_.load(std::memory_order_relaxed);
        p.total_bytes_written = static_cast<int64_t>(
            bytes_written_.load(std::memory_order_relaxed));
        size_t writer_count = 0;
        {
            std::lock_guard<std::mutex> lk(writers_mu_);
            writer_count = writers_.size();
        }
        p.files_completed     = completed_files_count_;
        p.files_in_progress   = static_cast<int>(writer_count);
        p.skipped_rows        = kv_skipped_.load(std::memory_order_relaxed);
        p.elapsed             = elapsed_ms();
        // estimated_progress based on batches: rough heuristic
        int64_t batches = batches_processed_.load(std::memory_order_relaxed);
        p.estimated_progress  = batches > 0 ? std::min(1.0, batches / 1000.0) : 0.0;
        return p;
    }

    std::chrono::milliseconds elapsed_ms() const {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start_time_);
    }

    std::string                        table_name_;
    std::vector<std::string>           column_families_;
    std::string                        output_dir_;
    std::unique_ptr<RegionPartitioner> partitioner_;
    WriterOptions                      opts_;
    int                                parallelism_{1};
    partition::CFGrouper               cf_grouper_;

    std::function<void(const ProgressInfo&)> progress_cb_;
    std::chrono::seconds               progress_interval_{10};
    std::atomic<bool>                  progress_running_{false};
    std::thread                        progress_thread_;

    mutable std::mutex                 writers_mu_;
    std::unordered_map<WriterKey, ActiveWriter, WriterKeyHash> writers_;
    std::unordered_map<WriterKey, uint32_t, WriterKeyHash> next_file_seq_;
    uint64_t                           use_clock_{0};
    int                                completed_files_count_{0};
    std::vector<std::string>           completed_files_;
    std::vector<std::string>           failed_files_;
    uint64_t                           completed_entries_{0};
    uint64_t                           completed_bytes_{0};
    Status                             first_finish_error_;

    // Counters (updated from potentially multiple threads, so atomic)
    std::atomic<int64_t> kv_written_{0};
    std::atomic<int64_t> kv_skipped_{0};
    std::atomic<int64_t> bytes_written_{0};
    std::atomic<int64_t> total_rows_{0};
    std::atomic<int64_t> batches_processed_{0};

    std::chrono::steady_clock::time_point start_time_;
};

// ─── Public API ───────────────────────────────────────────────────────────────

BulkLoadWriter::BulkLoadWriter(std::unique_ptr<BulkLoadWriterImpl> impl)
    : impl_{std::move(impl)} {}
BulkLoadWriter::~BulkLoadWriter() = default;
BulkLoadWriter::BulkLoadWriter(BulkLoadWriter&&) noexcept = default;
BulkLoadWriter& BulkLoadWriter::operator=(BulkLoadWriter&&) noexcept = default;

Status BulkLoadWriter::write_batch(const arrow::RecordBatch& batch, MappingMode mode) {
    return impl_->write_batch(batch, mode);
}
std::pair<BulkLoadResult, Status> BulkLoadWriter::finish() {
    return impl_->finish();
}

// ─── Builder ─────────────────────────────────────────────────────────────────

BulkLoadWriter::Builder& BulkLoadWriter::Builder::set_table_name(std::string n) {
    table_name_ = std::move(n); return *this;
}
BulkLoadWriter::Builder& BulkLoadWriter::Builder::set_column_families(
        std::vector<std::string> cfs) {
    column_families_ = std::move(cfs); return *this;
}
BulkLoadWriter::Builder& BulkLoadWriter::Builder::set_output_dir(std::string dir) {
    output_dir_ = std::move(dir); return *this;
}
BulkLoadWriter::Builder& BulkLoadWriter::Builder::set_partitioner(
        std::unique_ptr<RegionPartitioner> p) {
    partitioner_ = std::move(p); return *this;
}
BulkLoadWriter::Builder& BulkLoadWriter::Builder::set_compression(Compression c) {
    opts_.compression = c; return *this;
}
BulkLoadWriter::Builder& BulkLoadWriter::Builder::set_block_size(size_t sz) {
    opts_.block_size = sz; return *this;
}
BulkLoadWriter::Builder& BulkLoadWriter::Builder::set_data_block_encoding(Encoding enc) {
    opts_.data_block_encoding = enc; return *this;
}
BulkLoadWriter::Builder& BulkLoadWriter::Builder::set_bloom_type(BloomType bt) {
    opts_.bloom_type = bt; return *this;
}
BulkLoadWriter::Builder& BulkLoadWriter::Builder::set_parallelism(int n) {
    parallelism_ = n; return *this;
}
BulkLoadWriter::Builder& BulkLoadWriter::Builder::set_fsync_policy(FsyncPolicy p) {
    opts_.fsync_policy = p; return *this;
}
BulkLoadWriter::Builder& BulkLoadWriter::Builder::set_error_policy(ErrorPolicy p) {
    opts_.error_policy = p; return *this;
}
BulkLoadWriter::Builder& BulkLoadWriter::Builder::set_max_error_count(uint64_t n) {
    opts_.max_error_count = n; return *this;
}
BulkLoadWriter::Builder& BulkLoadWriter::Builder::set_error_callback(
        std::function<void(const RowError&)> cb) {
    opts_.error_callback = std::move(cb); return *this;
}
BulkLoadWriter::Builder& BulkLoadWriter::Builder::set_max_row_key_bytes(uint32_t n) {
    opts_.max_row_key_bytes = n; return *this;
}
BulkLoadWriter::Builder& BulkLoadWriter::Builder::set_max_value_bytes(size_t n) {
    opts_.max_value_bytes = n; return *this;
}
BulkLoadWriter::Builder& BulkLoadWriter::Builder::set_max_memory(size_t bytes) {
    opts_.max_memory_bytes = bytes; return *this;
}
BulkLoadWriter::Builder& BulkLoadWriter::Builder::set_min_free_disk(size_t bytes) {
    opts_.min_free_disk_bytes = bytes; return *this;
}
BulkLoadWriter::Builder& BulkLoadWriter::Builder::set_max_open_files(int n) {
    opts_.max_open_files = n; return *this;
}
BulkLoadWriter::Builder& BulkLoadWriter::Builder::set_progress_callback(
        std::function<void(const ProgressInfo&)> cb,
        std::chrono::seconds interval) {
    progress_cb_       = std::move(cb);
    progress_interval_ = interval;
    return *this;
}

std::pair<std::unique_ptr<BulkLoadWriter>, Status>
BulkLoadWriter::Builder::build() {
    if (output_dir_.empty())
        return {nullptr, Status::InvalidArg("output_dir must be set")};
    if (column_families_.empty())
        return {nullptr, Status::InvalidArg("column_families must not be empty")};
    if (!partitioner_)
        partitioner_ = RegionPartitioner::none();

    try {
        auto impl = std::make_unique<BulkLoadWriterImpl>(
            table_name_, column_families_, output_dir_,
            std::move(partitioner_), opts_, parallelism_,
            std::move(progress_cb_), progress_interval_);

        return {std::unique_ptr<BulkLoadWriter>(new BulkLoadWriter(std::move(impl))),
                Status::OK()};
    } catch (const std::exception& e) {
        return {nullptr, Status::Internal(std::string("bulk writer build failed: ") + e.what())};
    } catch (...) {
        return {nullptr, Status::Internal("bulk writer build failed: unknown exception")};
    }
}

} // namespace hfile
