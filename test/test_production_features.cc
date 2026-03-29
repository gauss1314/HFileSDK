#include "hfile/types.h"
#include "hfile/writer.h"
#include "hfile/writer_options.h"
#include "hfile/bulk_load_writer.h"
#include "io/atomic_file_writer.h"
#include "memory/memory_budget.h"
#include "metrics/metrics_registry.h"

#include <cassert>
#include <cstdio>
#include <cstring>
#include <vector>
#include <string>
#include <filesystem>
#include <thread>
#include <chrono>
#include <atomic>

using namespace hfile;
namespace fs = std::filesystem;

static int TESTS = 0, PASSED = 0;
#define EXPECT(c) do{++TESTS;if(c){++PASSED;}else{\
    fprintf(stderr,"  FAIL %s:%d  %s\n",__FILE__,__LINE__,#c);}}while(0)
#define EXPECT_EQ(a,b)   EXPECT((a)==(b))
#define EXPECT_TRUE(c)   EXPECT(c)
#define EXPECT_FALSE(c)  EXPECT(!(c))
#define EXPECT_OK(s)     EXPECT((s).ok())
#define EXPECT_ERR(s)    EXPECT(!(s).ok())

// ─── Helper: make a simple KV ─────────────────────────────────────────────────

static KeyValue make_kv(const std::string& row_str,
                         int64_t ts = 1000,
                         const std::string& val = "value") {
    static std::vector<uint8_t> row_buf, fam_buf, q_buf, v_buf;
    KeyValue kv;
    // Use heap storage to keep spans valid
    auto* rk = new std::vector<uint8_t>(row_str.begin(), row_str.end());
    auto* fam = new std::vector<uint8_t>({'c','f'});
    auto* q   = new std::vector<uint8_t>({'q'});
    auto* v   = new std::vector<uint8_t>(val.begin(), val.end());
    kv.row       = *rk;
    kv.family    = *fam;
    kv.qualifier = *q;
    kv.timestamp = ts;
    kv.key_type  = KeyType::Put;
    kv.value     = *v;
    return kv;
}


static KeyValue make_bad_kv() {
    static std::vector<uint8_t> s_fam = {'c','f'};
    static std::vector<uint8_t> s_q   = {'q'};
    static std::vector<uint8_t> s_v   = {'v'};
    KeyValue kv;
    // empty row key — invalid, used to trigger validation errors
    kv.family    = s_fam;
    kv.qualifier = s_q;
    kv.timestamp = 1;
    kv.key_type  = KeyType::Put;
    kv.value     = s_v;
    return kv;
}

static fs::path tmpfile(const std::string& name) {
    return fs::temp_directory_path() / name;
}

// ─── 1. AtomicFileWriter ──────────────────────────────────────────────────────

void test_atomic_commit_creates_final_file() {
    auto final = tmpfile("test_atomic_commit.hfile");
    fs::remove(final);
    {
        io::AtomicFileWriter aw(final.string());
        std::vector<uint8_t> data = {'h','e','l','l','o'};
        EXPECT_OK(aw.write({data.data(), data.size()}));
        EXPECT_OK(aw.commit());
        EXPECT_TRUE(fs::exists(final));
        EXPECT_FALSE(fs::exists(aw.temp_path()));  // temp must be gone
    }
    EXPECT_TRUE(fs::exists(final));   // file survives after destructor
    fs::remove(final);
}

void test_atomic_abort_deletes_temp() {
    auto final = tmpfile("test_atomic_abort.hfile");
    fs::remove(final);
    std::string temp_path;
    {
        io::AtomicFileWriter aw(final.string());
        temp_path = aw.temp_path();
        std::vector<uint8_t> data = {'a','b','c'};
        EXPECT_OK(aw.write({data.data(), data.size()}));
        aw.abort();
        EXPECT_FALSE(fs::exists(temp_path));   // temp deleted
        EXPECT_FALSE(fs::exists(final));        // final never created
    }
    EXPECT_FALSE(fs::exists(final));
}

void test_atomic_destructor_cleans_up_if_no_commit() {
    auto final = tmpfile("test_atomic_dtor.hfile");
    fs::remove(final);
    std::string temp_path;
    {
        io::AtomicFileWriter aw(final.string());
        temp_path = aw.temp_path();
        std::vector<uint8_t> data = {'x'};
        EXPECT_OK(aw.write({data.data(), data.size()}));
        // No commit() — destructor should abort
    }
    EXPECT_FALSE(fs::exists(final));     // final never written
    EXPECT_FALSE(fs::exists(temp_path)); // temp cleaned up
}

void test_atomic_temp_in_dottemp_subdir() {
    auto final = tmpfile("test_dottemp.hfile");
    io::AtomicFileWriter aw(final.string());
    std::string tp = aw.temp_path();
    // Temp path must be in .tmp/ subdirectory
    auto tp_parent = fs::path(tp).parent_path().filename().string();
    EXPECT_EQ(tp_parent, std::string(".tmp"));
    aw.abort();
}

void test_atomic_position_tracks_bytes_written() {
    auto final = tmpfile("test_atomic_pos.hfile");
    io::AtomicFileWriter aw(final.string());
    EXPECT_EQ(aw.position(), 0);
    std::vector<uint8_t> data(100, 'x');
    EXPECT_OK(aw.write({data.data(), data.size()}));
    EXPECT_EQ(aw.position(), 100);
    aw.abort();
}

// ─── 2. MemoryBudget ──────────────────────────────────────────────────────────

void test_memory_budget_basic_reserve_release() {
    memory::MemoryBudget b(1024);
    EXPECT_EQ(b.used(), 0u);
    EXPECT_OK(b.reserve(512));
    EXPECT_EQ(b.used(), 512u);
    b.release(512);
    EXPECT_EQ(b.used(), 0u);
}

void test_memory_budget_over_limit_returns_error() {
    memory::MemoryBudget b(100);
    EXPECT_OK(b.reserve(100));
    EXPECT_ERR(b.reserve(1));   // would exceed limit
    EXPECT_EQ(b.used(), 100u);  // unchanged
    b.release(100);
}

void test_memory_budget_guard_raii() {
    memory::MemoryBudget b(200);
    {
        memory::MemoryBudget::Guard g(b, 150);
        EXPECT_TRUE(g.ok());
        EXPECT_EQ(b.used(), 150u);
    }
    EXPECT_EQ(b.used(), 0u);   // guard released on destruction
}

void test_memory_budget_guard_over_limit_not_ok() {
    memory::MemoryBudget b(100);
    memory::MemoryBudget::Guard g(b, 200);   // exceeds limit
    EXPECT_FALSE(g.ok());
    EXPECT_EQ(b.used(), 0u);   // nothing reserved
}

void test_memory_budget_peak_tracking() {
    memory::MemoryBudget b(1024);
    EXPECT_OK(b.reserve(300));
    EXPECT_OK(b.reserve(200));
    EXPECT_EQ(b.peak(), 500u);
    b.release(300);
    EXPECT_EQ(b.peak(), 500u);  // peak stays at high watermark
    b.release(200);
}

void test_memory_budget_unlimited() {
    memory::MemoryBudget b;   // default = unlimited
    EXPECT_TRUE(b.unlimited());
    EXPECT_OK(b.reserve(1ULL * 1024 * 1024 * 1024));  // 1 GB — should succeed
    b.release(1ULL * 1024 * 1024 * 1024);
}

// ─── 3. MetricsRegistry ───────────────────────────────────────────────────────

void test_metrics_counter() {
    MetricsRegistry reg;
    EXPECT_EQ(reg.counter("c"), 0);
    reg.increment("c");
    reg.increment("c", 9);
    EXPECT_EQ(reg.counter("c"), 10);
}

void test_metrics_gauge() {
    MetricsRegistry reg;
    reg.set_gauge("g", 3.14);
    EXPECT_TRUE(std::abs(reg.gauge("g") - 3.14) < 0.001);
    reg.add_gauge("g", 1.0);
    EXPECT_TRUE(std::abs(reg.gauge("g") - 4.14) < 0.001);
}

void test_metrics_histogram() {
    MetricsRegistry reg;
    for (double v : {1.0, 2.0, 3.0, 4.0, 5.0})
        reg.observe("h", v);
    auto s = reg.histogram("h");
    EXPECT_EQ(s.count, 5);
    EXPECT_TRUE(std::abs(s.min - 1.0) < 0.001);
    EXPECT_TRUE(std::abs(s.max - 5.0) < 0.001);
    EXPECT_TRUE(std::abs(s.p50 - 3.0) < 0.001);
}

void test_metrics_snapshot() {
    MetricsRegistry reg;
    reg.increment("kv.count", 100);
    reg.set_gauge("mem.used", 4096.0);
    reg.observe("latency", 50.0);

    auto snap = reg.snapshot();
    EXPECT_EQ(snap.counter("kv.count"), 100);
    EXPECT_TRUE(std::abs(snap.gauge("mem.used") - 4096.0) < 0.001);
    EXPECT_EQ(snap.histograms.at("latency").count, 1);
}

void test_metrics_thread_safe() {
    MetricsRegistry reg;
    std::atomic<int> done{0};
    auto worker = [&]{
        for (int i = 0; i < 10000; ++i)
            reg.increment("cnt");
        ++done;
    };
    std::thread t1(worker), t2(worker);
    t1.join(); t2.join();
    EXPECT_EQ(reg.counter("cnt"), 20000);
}

void test_scoped_timer() {
    MetricsRegistry reg;
    {
        ScopedTimer t(reg, "op");
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    auto h = reg.histogram("op");
    EXPECT_EQ(h.count, 1);
    EXPECT_TRUE(h.p50 >= 50.0);  // at least 50 µs (liberal bound)
}

void test_metrics_report_callback_can_be_replaced() {
    MetricsRegistry reg;
    std::atomic<int> calls1{0};
    std::atomic<int> calls2{0};
    reg.set_report_callback([&](const MetricsSnapshot&) { ++calls1; }, std::chrono::seconds(0));
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    reg.set_report_callback([&](const MetricsSnapshot&) { ++calls2; }, std::chrono::seconds(0));
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    reg.stop_report_thread();
    EXPECT_TRUE(calls1.load() > 0);
    EXPECT_TRUE(calls2.load() > 0);
}

// ─── 4. FsyncPolicy / HFileWriter crash-safety ────────────────────────────────

void test_safe_policy_commit_creates_file() {
    auto path = tmpfile("test_safe_policy.hfile");
    fs::remove(path);
    {
        auto [w, s] = HFileWriter::builder()
            .set_path(path.string()).set_column_family("cf")
            .set_compression(Compression::None)
            .set_data_block_encoding(Encoding::None)
            .set_bloom_type(BloomType::None)
            .set_fsync_policy(FsyncPolicy::Safe)
            .build();
        EXPECT_OK(s);
        auto kv = make_kv("row1");
        EXPECT_OK(w->append(kv));
        EXPECT_OK(w->finish());
    }
    EXPECT_TRUE(fs::exists(path));
    fs::remove(path);
}

void test_safe_policy_no_partial_file_in_output_dir() {
    auto path = tmpfile("test_safe_partial.hfile");
    fs::remove(path);
    {
        auto [w, s] = HFileWriter::builder()
            .set_path(path.string()).set_column_family("cf")
            .set_compression(Compression::None)
            .set_data_block_encoding(Encoding::None)
            .set_bloom_type(BloomType::None)
            .set_fsync_policy(FsyncPolicy::Safe)
            .build();
        EXPECT_OK(s);
        auto kv = make_kv("row1");
        EXPECT_OK(w->append(kv));
        // Do NOT call finish() — destructor aborts atomic writer
    }
    // Final file must NOT exist (AtomicFileWriter aborted)
    EXPECT_FALSE(fs::exists(path));
}

void test_fast_policy_file_exists_after_finish() {
    auto path = tmpfile("test_fast_policy.hfile");
    fs::remove(path);
    {
        auto [w, s] = HFileWriter::builder()
            .set_path(path.string()).set_column_family("cf")
            .set_compression(Compression::None)
            .set_data_block_encoding(Encoding::None)
            .set_bloom_type(BloomType::None)
            .set_fsync_policy(FsyncPolicy::Fast)
            .build();
        EXPECT_OK(s);
        auto kv = make_kv("row1");
        EXPECT_OK(w->append(kv));
        EXPECT_OK(w->finish());
    }
    EXPECT_TRUE(fs::exists(path));
    fs::remove(path);
}

// ─── 5. ErrorPolicy / input validation ────────────────────────────────────────

void test_error_policy_strict_rejects_empty_row_key() {
    auto path = tmpfile("test_strict.hfile");
    fs::remove(path);
    auto [w, s] = HFileWriter::builder()
        .set_path(path.string()).set_column_family("cf")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .set_bloom_type(BloomType::None)
        .set_error_policy(ErrorPolicy::Strict)
        .build();
    EXPECT_OK(s);

    auto rs = w->append(make_bad_kv());
    EXPECT_ERR(rs);
    EXPECT_TRUE(rs.message().find("ROW_KEY_EMPTY") != std::string::npos);

    if (fs::exists(path)) fs::remove(path);
}

void test_error_policy_skip_row_continues_after_bad_row() {
    auto path = tmpfile("test_skip_row.hfile");
    fs::remove(path);
    auto [w, s] = HFileWriter::builder()
        .set_path(path.string()).set_column_family("cf")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .set_bloom_type(BloomType::None)
        .set_error_policy(ErrorPolicy::SkipRow)
        .build();
    EXPECT_OK(s);

    // Good row
    EXPECT_OK(w->append(make_kv("row1", 100)));

    // Bad row (empty key) — should be skipped, not abort
    auto bad = make_bad_kv();
    EXPECT_OK(w->append(bad));   // SkipRow → returns OK

    // Good row with higher timestamp for ordering
    EXPECT_OK(w->append(make_kv("row2", 200)));
    EXPECT_OK(w->finish());

    EXPECT_TRUE(fs::exists(path));
    fs::remove(path);
}

void test_error_policy_max_error_count() {
    auto path = tmpfile("test_max_err.hfile");
    fs::remove(path);
    auto [w, s] = HFileWriter::builder()
        .set_path(path.string()).set_column_family("cf")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .set_bloom_type(BloomType::None)
        .set_error_policy(ErrorPolicy::SkipRow)
        .set_max_error_count(3)
        .build();
    EXPECT_OK(s);

    // Trigger 3 errors (empty row key)
    auto bad = make_bad_kv();
    EXPECT_OK(w->append(bad));   // error 1
    EXPECT_OK(w->append(bad));   // error 2
    EXPECT_OK(w->append(bad));   // error 3 → triggers max_error_count

    // 4th should fail with MAX_ERRORS_EXCEEDED
    auto rs = w->append(bad);
    EXPECT_ERR(rs);
    EXPECT_TRUE(rs.message().find("MAX_ERRORS_EXCEEDED") != std::string::npos);

    if (fs::exists(path)) fs::remove(path);
}

void test_error_callback_fired() {
    auto path = tmpfile("test_error_cb.hfile");
    fs::remove(path);
    int cb_count = 0;
    auto [w, s] = HFileWriter::builder()
        .set_path(path.string()).set_column_family("cf")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .set_bloom_type(BloomType::None)
        .set_error_policy(ErrorPolicy::SkipRow)
        .set_error_callback([&](const RowError&) { ++cb_count; })
        .build();
    EXPECT_OK(s);

    auto bad = make_bad_kv();
    EXPECT_OK(w->append(bad));
    EXPECT_EQ(cb_count, 1);

    if (fs::exists(path)) fs::remove(path);
}

void test_value_too_large_rejected() {
    auto path = tmpfile("test_val_too_large.hfile");
    fs::remove(path);
    auto [w, s] = HFileWriter::builder()
        .set_path(path.string()).set_column_family("cf")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .set_bloom_type(BloomType::None)
        .set_error_policy(ErrorPolicy::Strict)
        .set_max_value_bytes(100)
        .build();
    EXPECT_OK(s);

    std::vector<uint8_t> big_val(101, 'x');
    KeyValue kv = make_kv("row1");
    kv.value = big_val;
    auto rs = w->append(kv);
    EXPECT_ERR(rs);
    EXPECT_TRUE(rs.message().find("VALUE_TOO_LARGE") != std::string::npos);

    if (fs::exists(path)) fs::remove(path);
}

void test_negative_timestamp_rejected() {
    auto path = tmpfile("test_neg_ts.hfile");
    fs::remove(path);
    auto [w, s] = HFileWriter::builder()
        .set_path(path.string()).set_column_family("cf")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .set_bloom_type(BloomType::None)
        .set_error_policy(ErrorPolicy::Strict)
        .build();
    EXPECT_OK(s);

    KeyValue kv = make_kv("row1", -1);  // negative timestamp
    auto rs = w->append(kv);
    EXPECT_ERR(rs);
    EXPECT_TRUE(rs.message().find("NEGATIVE_TIMESTAMP") != std::string::npos);

    if (fs::exists(path)) fs::remove(path);
}

// ─── 6. MemoryBudget integration in writer ────────────────────────────────────

void test_writer_with_max_memory_set_does_not_crash() {
    auto path = tmpfile("test_mem_budget.hfile");
    fs::remove(path);
    // Setting a generous budget — just verify the writer works normally
    auto [w, s] = HFileWriter::builder()
        .set_path(path.string()).set_column_family("cf")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .set_bloom_type(BloomType::None)
        .set_max_memory(512 * 1024 * 1024)
        .build();
    EXPECT_OK(s);
    EXPECT_OK(w->append(make_kv("row1")));
    EXPECT_OK(w->finish());
    EXPECT_TRUE(fs::exists(path));
    fs::remove(path);
}

// ─── 7. BulkLoadResult enhancements ─────────────────────────────────────────

void test_bulk_load_result_has_skipped_rows_and_elapsed() {
    auto dir = tmpfile("test_bulk_result");
    fs::remove_all(dir);
    auto [bulk, s] = BulkLoadWriter::builder()
        .set_output_dir(dir.string())
        .set_column_families({"cf"})
        .set_partitioner(RegionPartitioner::none())
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .set_bloom_type(BloomType::None)
        .set_error_policy(ErrorPolicy::SkipRow)
        .build();
    EXPECT_OK(s);

    // Write via write_kv directly is private; just call finish on empty writer
    auto [result, fs2] = bulk->finish();
    EXPECT_OK(fs2);
    EXPECT_EQ(result.staging_dir, dir.string());
    EXPECT_TRUE(result.elapsed.count() >= 0);

    fs::remove_all(dir);
}

void test_bulk_load_partial_success_flag() {
    BulkLoadResult r;
    r.files = {"cf/file1.hfile"};
    r.failed_files = {"cf/file2.hfile"};
    EXPECT_TRUE(r.partial_success());

    BulkLoadResult r2;
    r2.files = {"cf/file1.hfile"};
    EXPECT_FALSE(r2.partial_success());
}

// ─── 8. ProgressCallback ─────────────────────────────────────────────────────

void test_progress_info_fields() {
    ProgressInfo p;
    p.total_kv_written    = 1000;
    p.total_bytes_written = 50000;
    p.files_completed     = 2;
    p.skipped_rows        = 5;
    p.elapsed             = std::chrono::milliseconds(123);
    p.estimated_progress  = 0.5;
    EXPECT_EQ(p.total_kv_written, 1000);
    EXPECT_EQ(p.skipped_rows, 5);
    EXPECT_EQ(p.elapsed.count(), 123);
}

void test_bulk_load_progress_callback_exception_is_contained() {
    auto dir = tmpfile("test_bulk_progress_exception");
    fs::remove_all(dir);
    std::atomic<int> calls{0};
    auto [bulk, s] = BulkLoadWriter::builder()
        .set_output_dir(dir.string())
        .set_column_families({"cf"})
        .set_partitioner(RegionPartitioner::none())
        .set_progress_callback([&](const ProgressInfo&) {
            ++calls;
            throw std::runtime_error("boom");
        }, std::chrono::seconds(0))
        .build();
    EXPECT_OK(s);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    auto [result, fs2] = bulk->finish();
    EXPECT_OK(fs2);
    EXPECT_EQ(result.staging_dir, dir.string());
    EXPECT_TRUE(calls.load() > 0);
    fs::remove_all(dir);
}

// ─── Main ─────────────────────────────────────────────────────────────────────

int main() {
    printf("\n=== Production feature tests ===\n\n");

    // AtomicFileWriter
    test_atomic_commit_creates_final_file();
    test_atomic_abort_deletes_temp();
    test_atomic_destructor_cleans_up_if_no_commit();
    test_atomic_temp_in_dottemp_subdir();
    test_atomic_position_tracks_bytes_written();

    // MemoryBudget
    test_memory_budget_basic_reserve_release();
    test_memory_budget_over_limit_returns_error();
    test_memory_budget_guard_raii();
    test_memory_budget_guard_over_limit_not_ok();
    test_memory_budget_peak_tracking();
    test_memory_budget_unlimited();

    // MetricsRegistry
    test_metrics_counter();
    test_metrics_gauge();
    test_metrics_histogram();
    test_metrics_snapshot();
    test_metrics_thread_safe();
    test_scoped_timer();
    test_metrics_report_callback_can_be_replaced();

    // FsyncPolicy / crash-safety
    test_safe_policy_commit_creates_file();
    test_safe_policy_no_partial_file_in_output_dir();
    test_fast_policy_file_exists_after_finish();

    // ErrorPolicy / input validation
    test_error_policy_strict_rejects_empty_row_key();
    test_error_policy_skip_row_continues_after_bad_row();
    test_error_policy_max_error_count();
    test_error_callback_fired();
    test_value_too_large_rejected();
    test_negative_timestamp_rejected();

    // MemoryBudget integration
    test_writer_with_max_memory_set_does_not_crash();

    // BulkLoadResult
    test_bulk_load_result_has_skipped_rows_and_elapsed();
    test_bulk_load_partial_success_flag();

    // ProgressInfo
    test_progress_info_fields();
    test_bulk_load_progress_callback_exception_is_contained();

    printf("Tests run: %d  Passed: %d  Failed: %d\n\n",
           TESTS, PASSED, TESTS - PASSED);
    return (PASSED == TESTS) ? 0 : 1;
}
