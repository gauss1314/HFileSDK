#include "jni_utils.h"
#include "json_utils.h"
#include "../convert/converter.h"
#include "../convert/convert_options.h"

#include <hfile/writer_options.h>

#include <jni.h>
#include <cctype>
#include <string>
#include <stdexcept>
#include <cstdio>
#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <limits>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// Auto-generated JNI header (produced by javac -h or javah):
// Expected class: com.hfile.HFileSDK
// Method signatures mirror HFileSDK.java

namespace hfile {
namespace jni {

static std::string ascii_lower(std::string value) {
    for (char& ch : value) {
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    }
    return value;
}

struct InstanceState {
    mutable std::mutex mutex;
    WriterOptions writer_opts;
    NumericSortFastPathMode numeric_sort_fast_path{NumericSortFastPathMode::Auto};
    ConvertResult last_result;
    std::string   last_result_json{"{}"};
    int64_t       default_timestamp_ms{0};
    // Column exclusion settings (set via configure())
    std::vector<std::string> excluded_columns;
    std::vector<std::string> excluded_column_prefixes;
};

static std::string result_to_json(const ConvertResult& r) {
    std::ostringstream oss;
    oss << "{"
        << "\"error_code\":"          << r.error_code << ','
        << "\"error_message\":\""     << json_escape(r.error_message) << "\","
        << "\"arrow_batches_read\":"  << static_cast<long long>(r.arrow_batches_read) << ','
        << "\"arrow_rows_read\":"     << static_cast<long long>(r.arrow_rows_read) << ','
        << "\"kv_written_count\":"    << static_cast<long long>(r.kv_written_count) << ','
        << "\"kv_skipped_count\":"    << static_cast<long long>(r.kv_skipped_count) << ','
        << "\"duplicate_key_count\":" << static_cast<long long>(r.duplicate_key_count) << ','
        << "\"memory_budget_bytes\":" << static_cast<long long>(r.memory_budget_bytes) << ','
        << "\"tracked_memory_peak_bytes\":" << static_cast<long long>(r.tracked_memory_peak_bytes) << ','
        << "\"numeric_sort_fast_path_mode\":\""
        << numeric_sort_fast_path_mode_name(r.numeric_sort_fast_path_mode) << "\","
        << "\"numeric_sort_fast_path_used\":" << (r.numeric_sort_fast_path_used ? "true" : "false") << ','
        << "\"hfile_size_bytes\":"    << static_cast<long long>(r.hfile_size_bytes) << ','
        << "\"elapsed_ms\":"          << static_cast<long long>(r.elapsed_ms.count()) << ','
        << "\"sort_ms\":"             << static_cast<long long>(r.sort_ms.count()) << ','
        << "\"write_ms\":"            << static_cast<long long>(r.write_ms.count()) << ','
        << "\"data_block_encode_ms\":" << static_cast<long long>(r.data_block_encode_ms.count()) << ','
        << "\"data_block_compress_ms\":" << static_cast<long long>(r.data_block_compress_ms.count()) << ','
        << "\"data_block_write_ms\":" << static_cast<long long>(r.data_block_write_ms.count()) << ','
        << "\"leaf_index_write_ms\":" << static_cast<long long>(r.leaf_index_write_ms.count()) << ','
        << "\"bloom_chunk_write_ms\":" << static_cast<long long>(r.bloom_chunk_write_ms.count()) << ','
        << "\"load_on_open_write_ms\":" << static_cast<long long>(r.load_on_open_write_ms.count()) << ','
        << "\"data_block_count\":" << static_cast<unsigned long long>(r.data_block_count) << ','
        << "\"leaf_index_block_count\":" << static_cast<unsigned long long>(r.leaf_index_block_count) << ','
        << "\"bloom_chunk_flush_count\":" << static_cast<unsigned long long>(r.bloom_chunk_flush_count) << ','
        << "\"load_on_open_block_count\":" << static_cast<unsigned long long>(r.load_on_open_block_count)
        << "}";
    return oss.str();
}

static ConvertResult make_error_result(int code, std::string message) {
    ConvertResult r;
    r.error_code = code;
    r.error_message = std::move(message);
    return r;
}

} // namespace jni
} // namespace hfile

// ─── Per-instance config / state (set via configure()) ──────────────────────
//
// Java owns an opaque monotonically increasing handle.  The registry is used
// instead of storing a raw pointer in Java so a stale/corrupted handle can be
// rejected without dereferencing arbitrary memory.  A lookup returns a
// shared_ptr: nativeDestroySession() removes future access immediately, while
// a convert already in progress can safely finish without a use-after-free.
using InstanceStatePtr = std::shared_ptr<hfile::jni::InstanceState>;

static std::shared_mutex g_sessions_mutex;
static std::unordered_map<jlong, InstanceStatePtr> g_sessions;
static std::atomic<uint64_t> g_next_session_handle{1};

// Compatibility-only state for HFileSDK.class versions compiled before the
// nativeHandle API existed. New Java code never enters this registry.
struct LegacyInstanceState {
    jweak weak_ref{nullptr};
    InstanceStatePtr state;
};
static std::mutex g_legacy_sessions_mutex;
static std::vector<LegacyInstanceState> g_legacy_sessions;

static InstanceStatePtr find_instance_state(jlong handle) {
    if (handle == 0) return {};
    std::shared_lock<std::shared_mutex> lock(g_sessions_mutex);
    auto it = g_sessions.find(handle);
    return it == g_sessions.end() ? InstanceStatePtr{} : it->second;
}

static jlong create_instance_state() {
    auto state = std::make_shared<hfile::jni::InstanceState>();
    state->writer_opts.column_family = "cf";

    // Handle 0 is permanently reserved as the Java-side "closed" sentinel.
    for (;;) {
        const auto raw = g_next_session_handle.fetch_add(1, std::memory_order_relaxed);
        const auto handle = static_cast<jlong>(raw);
        if (handle == 0) continue;
        std::unique_lock<std::shared_mutex> lock(g_sessions_mutex);
        if (g_sessions.emplace(handle, state).second) return handle;
    }
}

static void destroy_instance_state(jlong handle) noexcept {
    if (handle == 0) return;
    std::unique_lock<std::shared_mutex> lock(g_sessions_mutex);
    g_sessions.erase(handle);
}

static InstanceStatePtr get_or_create_legacy_instance_state(JNIEnv* env,
                                                            jobject obj) {
    std::lock_guard<std::mutex> lock(g_legacy_sessions_mutex);
    auto it = g_legacy_sessions.begin();
    while (it != g_legacy_sessions.end()) {
        if (env->IsSameObject(it->weak_ref, nullptr)) {
            env->DeleteWeakGlobalRef(it->weak_ref);
            it = g_legacy_sessions.erase(it);
            continue;
        }
        if (env->IsSameObject(it->weak_ref, obj)) return it->state;
        ++it;
    }

    auto state = std::make_shared<hfile::jni::InstanceState>();
    state->writer_opts.column_family = "cf";
    auto weak_ref = env->NewWeakGlobalRef(obj);
    if (weak_ref == nullptr) {
        if (env->ExceptionCheck()) env->ExceptionClear();
        return {};
    }
    g_legacy_sessions.push_back({weak_ref, state});
    return state;
}

static void set_instance_result(const InstanceStatePtr& state,
                                const hfile::ConvertResult& result) {
    if (!state) return;
    // JSON construction may allocate; keep it outside the instance lock so a
    // concurrent getLastResult() is not blocked by formatting work.
    std::string result_json = hfile::jni::result_to_json(result);
    std::lock_guard<std::mutex> lock(state->mutex);
    state->last_result = result;
    state->last_result_json = std::move(result_json);
}

static hfile::ConvertResult invalid_session_result() {
    return hfile::jni::make_error_result(
        hfile::ErrorCode::INTERNAL_ERROR,
        "HFileSDK native session is closed or invalid");
}

/// Retrieve a snapshot of the full InstanceState (writer_opts + exclusions).
/// Called once per convert() invocation under the lock, then used without lock.
struct InstanceSnapshot {
    hfile::WriterOptions     writer_opts;
    hfile::NumericSortFastPathMode numeric_sort_fast_path;
    int64_t                  default_timestamp_ms;
    std::vector<std::string> excluded_columns;
    std::vector<std::string> excluded_column_prefixes;
};
static InstanceSnapshot get_instance_snapshot(const InstanceStatePtr& state) {
    std::lock_guard<std::mutex> lock(state->mutex);
    const auto& s = *state;
    return {
        s.writer_opts,
        s.numeric_sort_fast_path,
        s.default_timestamp_ms,
        s.excluded_columns,
        s.excluded_column_prefixes
    };
}

// ─── JNI exports ─────────────────────────────────────────────────────────────
extern "C" {

/** Allocate an opaque native session for one HFileSDK Java object. */
JNIEXPORT jlong JNICALL
Java_com_hfile_HFileSDK_nativeCreateSession(JNIEnv*, jclass)
{
    try {
        return create_instance_state();
    } catch (const std::exception& e) {
        fprintf(stderr, "[ERROR] JNI nativeCreateSession: %s\n", e.what());
        return 0;
    } catch (...) {
        fprintf(stderr, "[ERROR] JNI nativeCreateSession: unknown exception\n");
        return 0;
    }
}

/** Remove a session from the registry. Safe and idempotent. */
JNIEXPORT void JNICALL
Java_com_hfile_HFileSDK_nativeDestroySession(JNIEnv*, jclass, jlong handle)
{
    try {
        destroy_instance_state(handle);
    } catch (...) {
        // Cleaner callbacks must never surface a C++ exception into the JVM.
        fprintf(stderr, "[ERROR] JNI nativeDestroySession: unknown exception\n");
    }
}

/**
 * Java: public native int convert(String arrowPath, String hfilePath,
 *                                  String tableName, String rowKeyRule)
 *
 * Note: rowValue parameter was REMOVED per v4.0 design (it is derived
 * internally from Arrow columns, not passed by the caller).
 *
 * Returns: 0 = success, non-zero = ErrorCode constant
 */
static jint convert_with_state(JNIEnv* env,
                               const InstanceStatePtr& state,
                               jstring j_arrow_path,
                               jstring j_hfile_path,
                               jstring j_table_name,
                               jstring j_row_key_rule)
{
    if (!state) return hfile::ErrorCode::INTERNAL_ERROR;

    try {
        using namespace hfile::jni;

        // ── Extract strings ───────────────────────────────────────────────
        std::string arrow_path;
        std::string hfile_path;
        std::string table_name;
        std::string row_key_rule;
        hfile::Status s = jstring_to_string(env, j_arrow_path, &arrow_path);
        if (!s.ok()) {
            auto r = make_error_result(
                s.code() == hfile::Status::Code::InvalidArg
                ? hfile::ErrorCode::INVALID_ARGUMENT
                : hfile::ErrorCode::INTERNAL_ERROR,
                "arrowPath: " + s.message());
            set_instance_result(state, r);
            return r.error_code;
        }
        s = jstring_to_string(env, j_hfile_path, &hfile_path);
        if (!s.ok()) {
            auto r = make_error_result(
                s.code() == hfile::Status::Code::InvalidArg
                ? hfile::ErrorCode::INVALID_ARGUMENT
                : hfile::ErrorCode::INTERNAL_ERROR,
                "hfilePath: " + s.message());
            set_instance_result(state, r);
            return r.error_code;
        }
        s = optional_jstring_to_string(env, j_table_name, &table_name);
        if (!s.ok()) {
            auto r = make_error_result(hfile::ErrorCode::INTERNAL_ERROR,
                                       "tableName: " + s.message());
            set_instance_result(state, r);
            return r.error_code;
        }
        s = optional_jstring_to_string(env, j_row_key_rule, &row_key_rule);
        if (!s.ok()) {
            auto r = make_error_result(hfile::ErrorCode::INTERNAL_ERROR,
                                       "rowKeyRule: " + s.message());
            set_instance_result(state, r);
            return r.error_code;
        }

        // ── Validate required parameters ──────────────────────────────────
        if (arrow_path.empty() || hfile_path.empty()) {
            auto r = make_error_result(hfile::ErrorCode::INVALID_ARGUMENT,
                                       "arrowPath and hfilePath must not be null/empty");
            set_instance_result(state, r);
            return r.error_code;
        }

        // ── Build ConvertOptions ──────────────────────────────────────────
        hfile::ConvertOptions opts;
        opts.arrow_path   = arrow_path;
        opts.hfile_path   = hfile_path;
        opts.table_name   = table_name;
        opts.row_key_rule = row_key_rule;

        auto snap = get_instance_snapshot(state);
        opts.writer_opts  = snap.writer_opts;
        opts.numeric_sort_fast_path = snap.numeric_sort_fast_path;
        opts.default_timestamp = snap.default_timestamp_ms;
        opts.excluded_columns         = snap.excluded_columns;
        opts.excluded_column_prefixes = snap.excluded_column_prefixes;

        // If configure() set a column_family, propagate it into ConvertOptions.
        // Otherwise leave ConvertOptions::column_family at its default ("cf").
        if (!opts.writer_opts.column_family.empty())
            opts.column_family = opts.writer_opts.column_family;
        opts.writer_opts.column_family = opts.column_family;

        // ── Execute conversion ────────────────────────────────────────────
        hfile::ConvertResult result = hfile::convert(opts);

        set_instance_result(state, result);

        return result.error_code;

    } catch (const std::exception& e) {
        fprintf(stderr, "[ERROR] JNI convert: %s\n", e.what());
        auto r = hfile::jni::make_error_result(
            hfile::ErrorCode::INTERNAL_ERROR,
            std::string("Internal C++ exception: ") + e.what());
        set_instance_result(state, r);
        return r.error_code;
    } catch (...) {
        fprintf(stderr, "[ERROR] JNI convert: unknown exception\n");
        auto r = hfile::jni::make_error_result(
            hfile::ErrorCode::INTERNAL_ERROR,
            "Internal C++ exception: unknown");
        set_instance_result(state, r);
        return r.error_code;
    }
}

JNIEXPORT jint JNICALL
Java_com_hfile_HFileSDK_nativeConvert(JNIEnv* env, jclass,
                                      jlong handle,
                                      jstring j_arrow_path,
                                      jstring j_hfile_path,
                                      jstring j_table_name,
                                      jstring j_row_key_rule)
{
    try {
        return convert_with_state(env, find_instance_state(handle),
                                  j_arrow_path, j_hfile_path,
                                  j_table_name, j_row_key_rule);
    } catch (...) {
        return hfile::ErrorCode::INTERNAL_ERROR;
    }
}

/**
 * Java: public native String getLastResult()
 * Returns JSON string with the details of the last convert() call.
 */
static jstring get_last_result_with_state(JNIEnv* env,
                                          const InstanceStatePtr& state)
{
    try {
        if (!state) {
            const std::string invalid_json =
                hfile::jni::result_to_json(invalid_session_result());
            return env->NewStringUTF(invalid_json.c_str());
        }
        std::string result_json;
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            result_json = state->last_result_json;
        }
        return env->NewStringUTF(result_json.c_str());
    } catch (...) {
        return env->NewStringUTF("{}");
    }
}

JNIEXPORT jstring JNICALL
Java_com_hfile_HFileSDK_nativeGetLastResult(JNIEnv* env, jclass, jlong handle)
{
    try {
        return get_last_result_with_state(env, find_instance_state(handle));
    } catch (...) {
        return env->NewStringUTF("{}");
    }
}

/**
 * Java: public native int configure(String configJson)
 * Apply global configuration before the first convert() call.
 * configJson: {"compression":"GZ","block_size":65536,"column_family":"cf",...}
 */
static jint configure_with_state(JNIEnv* env,
                                 const InstanceStatePtr& state,
                                 jstring j_config)
{
    if (!state) return hfile::ErrorCode::INTERNAL_ERROR;

    try {
        std::string config;
        auto str_status = hfile::jni::optional_jstring_to_string(env, j_config, &config);
        if (!str_status.ok()) {
            auto r = hfile::jni::make_error_result(hfile::ErrorCode::INTERNAL_ERROR,
                                                   "configJson: " + str_status.message());
            set_instance_result(state, r);
            return r.error_code;
        }
        if (config.empty()) {
            set_instance_result(state, hfile::ConvertResult{});
            return 0;
        }

        std::lock_guard<std::mutex> lock(state->mutex);
        auto next_opts = state->writer_opts;
        auto next_numeric_sort_fast_path = state->numeric_sort_fast_path;
        auto next_default_timestamp_ms = state->default_timestamp_ms;
        auto next_excluded_columns = state->excluded_columns;
        auto next_excluded_column_prefixes = state->excluded_column_prefixes;
        auto fail_config = [&](std::string message) {
            state->last_result = hfile::jni::make_error_result(
                hfile::ErrorCode::INVALID_ARGUMENT, std::move(message));
            state->last_result_json = hfile::jni::result_to_json(state->last_result);
            return state->last_result.error_code;
        };

        hfile::jni::JsonConfigObject cfg;
        auto parse_status = hfile::jni::parse_json_config(config, &cfg);
        if (!parse_status.ok()) {
            state->last_result = hfile::jni::make_error_result(
                hfile::ErrorCode::INVALID_ARGUMENT, parse_status.message());
            state->last_result_json = hfile::jni::result_to_json(state->last_result);
            return state->last_result.error_code;
        }

        static const std::unordered_set<std::string> kAllowedKeys = {
            "compression",
            "compression_level",
            "block_size",
            "column_family",
            "data_block_encoding",
            "fsync_policy",
            "error_policy",
            "bloom_type",
            "include_mvcc",
            "max_memory_bytes",
            "compression_threads",
            "compression_queue_depth",
            "numeric_sort_fast_path",
            "default_timestamp_ms",
            // Column exclusion — used for Hudi / CDC metadata columns
            "excluded_columns",          // ["col1","col2",...]  exact names
            "excluded_column_prefixes"   // ["_hoodie","_cdc_"]  prefix match
        };
        for (const auto& [key, _] : cfg) {
            if (!kAllowedKeys.count(key))
                return fail_config("Unsupported config key: " + key);
        }

        if (auto comp = hfile::jni::config_string(cfg, "compression")) {
            std::string normalized = hfile::jni::ascii_lower(*comp);
            if      (normalized == "none") next_opts.compression = hfile::Compression::None;
            else if (normalized == "gzip" || normalized == "gz")
                next_opts.compression = hfile::Compression::GZip;
            else return fail_config("Only compression=NONE, GZ, or gzip is supported");
        }

        if (auto cl = hfile::jni::config_int(cfg, "compression_level")) {
            if (*cl < 0 || *cl > 9)
                return fail_config("compression_level must be 0-9");
            next_opts.compression_level = static_cast<int>(*cl);
        }

        if (auto bs = hfile::jni::config_int(cfg, "block_size")) {
            if (*bs <= 0) return fail_config("block_size must be > 0");
            next_opts.block_size = static_cast<size_t>(*bs);
        }

        if (auto cf = hfile::jni::config_string(cfg, "column_family")) {
            if (cf->empty()) return fail_config("column_family must not be empty");
            next_opts.column_family = *cf;
        }

        if (auto enc = hfile::jni::config_string(cfg, "data_block_encoding")) {
            if (*enc == "NONE") next_opts.data_block_encoding = hfile::Encoding::None;
            else return fail_config("Only data_block_encoding=NONE is supported");
        }

        if (auto fp = hfile::jni::config_string(cfg, "fsync_policy")) {
            if      (*fp == "safe")      next_opts.fsync_policy = hfile::FsyncPolicy::Safe;
            else if (*fp == "fast")      next_opts.fsync_policy = hfile::FsyncPolicy::Fast;
            else if (*fp == "paranoid")  next_opts.fsync_policy = hfile::FsyncPolicy::Paranoid;
            else return fail_config("Invalid fsync_policy: " + *fp);
        }

        if (auto ep = hfile::jni::config_string(cfg, "error_policy")) {
            if      (*ep == "strict")    next_opts.error_policy = hfile::ErrorPolicy::Strict;
            else if (*ep == "skip_row")  next_opts.error_policy = hfile::ErrorPolicy::SkipRow;
            else if (*ep == "skip_batch")next_opts.error_policy = hfile::ErrorPolicy::SkipBatch;
            else return fail_config("Invalid error_policy: " + *ep);
        }

        if (auto bt = hfile::jni::config_string(cfg, "bloom_type")) {
            if      (*bt == "none")     next_opts.bloom_type = hfile::BloomType::None;
            else if (*bt == "row")      next_opts.bloom_type = hfile::BloomType::Row;
            else if (*bt == "rowcol")   next_opts.bloom_type = hfile::BloomType::RowCol;
            else return fail_config("Invalid bloom_type: " + *bt);
        }

        if (auto mvcc = hfile::jni::config_int(cfg, "include_mvcc")) {
            if (*mvcc != 0 && *mvcc != 1) return fail_config("include_mvcc must be 0 or 1");
            next_opts.include_mvcc = (*mvcc != 0);
        }

        if (auto mm = hfile::jni::config_int(cfg, "max_memory_bytes")) {
            if (*mm < 0) return fail_config("max_memory_bytes must be >= 0");
            next_opts.max_memory_bytes = static_cast<size_t>(*mm);
        }

        if (auto ct = hfile::jni::config_int(cfg, "compression_threads")) {
            if (*ct < 0 ||
                static_cast<uint64_t>(*ct) >
                    std::numeric_limits<uint32_t>::max()) {
                return fail_config("compression_threads must be 0-4294967295");
            }
            next_opts.compression_threads = static_cast<uint32_t>(*ct);
        }

        if (auto cq = hfile::jni::config_int(cfg, "compression_queue_depth")) {
            if (*cq < 0 || *cq > 4096)
                return fail_config("compression_queue_depth must be 0-4096");
            next_opts.compression_queue_depth = static_cast<uint32_t>(*cq);
        }

        if (auto nsfp = hfile::jni::config_string(cfg, "numeric_sort_fast_path")) {
            std::string normalized = hfile::jni::ascii_lower(*nsfp);
            if (normalized == "auto") {
                next_numeric_sort_fast_path = hfile::NumericSortFastPathMode::Auto;
            } else if (normalized == "on") {
                next_numeric_sort_fast_path = hfile::NumericSortFastPathMode::On;
            } else if (normalized == "off") {
                next_numeric_sort_fast_path = hfile::NumericSortFastPathMode::Off;
            } else {
                return fail_config("numeric_sort_fast_path must be one of: auto|on|off");
            }
        }

        if (auto ts = hfile::jni::config_int(cfg, "default_timestamp_ms")) {
            if (*ts < 0) return fail_config("default_timestamp_ms must be >= 0");
            next_default_timestamp_ms = *ts;
        }

        // ── Column exclusion ──────────────────────────────────────────────────
        // These are stored on InstanceState, not on WriterOptions, because they
        // are a converter-level concept (applied at Arrow → HFile mapping time).
        if (auto cols = hfile::jni::config_string_array(cfg, "excluded_columns")) {
            next_excluded_columns = std::move(*cols);
        }
        if (auto pfxs = hfile::jni::config_string_array(cfg, "excluded_column_prefixes")) {
            next_excluded_column_prefixes = std::move(*pfxs);
        }

        state->writer_opts = std::move(next_opts);
        state->numeric_sort_fast_path = next_numeric_sort_fast_path;
        state->default_timestamp_ms = next_default_timestamp_ms;
        state->excluded_columns = std::move(next_excluded_columns);
        state->excluded_column_prefixes = std::move(next_excluded_column_prefixes);
        state->last_result = {};
        state->last_result_json = hfile::jni::result_to_json(state->last_result);
        return 0;
    } catch (...) {
        auto r = hfile::jni::make_error_result(hfile::ErrorCode::INTERNAL_ERROR,
                                               "Internal C++ exception during configure()");
        set_instance_result(state, r);
        return hfile::ErrorCode::INTERNAL_ERROR;
    }
}

JNIEXPORT jint JNICALL
Java_com_hfile_HFileSDK_nativeConfigure(JNIEnv* env, jclass,
                                        jlong handle, jstring j_config)
{
    try {
        return configure_with_state(env, find_instance_state(handle), j_config);
    } catch (...) {
        return hfile::ErrorCode::INTERNAL_ERROR;
    }
}

/** Test-only visibility probe for deterministic lifecycle assertions. */
JNIEXPORT jboolean JNICALL
Java_com_hfile_HFileSDK_nativeSessionExistsForTesting(JNIEnv*, jclass, jlong handle)
{
    try {
        return find_instance_state(handle) ? JNI_TRUE : JNI_FALSE;
    } catch (...) {
        return JNI_FALSE;
    }
}

// ─── Legacy HFileSDK.class JNI ABI shims ────────────────────────────────────
// These exact symbols keep older already-compiled Java artifacts loadable. The
// new class calls only native*Session/native* methods above, so its hot path
// never scans the compatibility weak-reference table.
JNIEXPORT jint JNICALL
Java_com_hfile_HFileSDK_convert(JNIEnv* env, jobject obj,
                                jstring j_arrow_path,
                                jstring j_hfile_path,
                                jstring j_table_name,
                                jstring j_row_key_rule)
{
    try {
        return convert_with_state(env,
                                  get_or_create_legacy_instance_state(env, obj),
                                  j_arrow_path, j_hfile_path,
                                  j_table_name, j_row_key_rule);
    } catch (...) {
        return hfile::ErrorCode::INTERNAL_ERROR;
    }
}

JNIEXPORT jstring JNICALL
Java_com_hfile_HFileSDK_getLastResult(JNIEnv* env, jobject obj)
{
    try {
        return get_last_result_with_state(
            env, get_or_create_legacy_instance_state(env, obj));
    } catch (...) {
        return env->NewStringUTF("{}");
    }
}

JNIEXPORT jint JNICALL
Java_com_hfile_HFileSDK_configure(JNIEnv* env, jobject obj, jstring j_config)
{
    try {
        return configure_with_state(
            env, get_or_create_legacy_instance_state(env, obj), j_config);
    } catch (...) {
        return hfile::ErrorCode::INTERNAL_ERROR;
    }
}

} // extern "C"
