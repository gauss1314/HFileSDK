#include "jni_utils.h"
#include "json_utils.h"
#include "../convert/converter.h"
#include "../convert/convert_options.h"

#include <hfile/writer_options.h>

#include <jni.h>
#include <string>
#include <stdexcept>
#include <cstdio>
#include <mutex>
#include <sstream>
#include <unordered_set>
#include <vector>

// Auto-generated JNI header (produced by javac -h or javah):
// Expected class: com.hfile.HFileSDK
// Method signatures mirror HFileSDK.java

namespace hfile {
namespace jni {

struct InstanceState {
    jobject       weak_ref{nullptr};
    WriterOptions writer_opts;
    ConvertResult last_result;
    std::string   last_result_json{"{}"};
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
        << "\"hfile_size_bytes\":"    << static_cast<long long>(r.hfile_size_bytes) << ','
        << "\"elapsed_ms\":"          << static_cast<long long>(r.elapsed_ms.count()) << ','
        << "\"sort_ms\":"             << static_cast<long long>(r.sort_ms.count()) << ','
        << "\"write_ms\":"            << static_cast<long long>(r.write_ms.count())
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
static std::mutex                           g_config_mutex;
static std::vector<hfile::jni::InstanceState> g_instance_states;

static void cleanup_instance_states_locked(JNIEnv* env) {
    auto it = g_instance_states.begin();
    while (it != g_instance_states.end()) {
        if (env->IsSameObject(it->weak_ref, nullptr)) {
            env->DeleteWeakGlobalRef(it->weak_ref);
            it = g_instance_states.erase(it);
        } else {
            ++it;
        }
    }
}

static hfile::jni::InstanceState* find_instance_state_locked(JNIEnv* env, jobject obj) {
    cleanup_instance_states_locked(env);
    for (auto& state : g_instance_states) {
        if (env->IsSameObject(state.weak_ref, obj))
            return &state;
    }
    return nullptr;
}

static hfile::jni::InstanceState& get_or_create_instance_state_locked(JNIEnv* env, jobject obj) {
    if (auto* state = find_instance_state_locked(env, obj))
        return *state;
    hfile::jni::InstanceState state;
    state.weak_ref = env->NewWeakGlobalRef(obj);
    state.writer_opts.column_family = "cf";
    g_instance_states.push_back(std::move(state));
    return g_instance_states.back();
}

static void set_instance_result(JNIEnv* env, jobject obj, const hfile::ConvertResult& result) {
    std::lock_guard<std::mutex> lk(g_config_mutex);
    auto& state = get_or_create_instance_state_locked(env, obj);
    state.last_result = result;
    state.last_result_json = hfile::jni::result_to_json(result);
}

static hfile::WriterOptions get_instance_writer_opts(JNIEnv* env, jobject obj) {
    std::lock_guard<std::mutex> lk(g_config_mutex);
    return get_or_create_instance_state_locked(env, obj).writer_opts;
}

/// Retrieve a snapshot of the full InstanceState (writer_opts + exclusions).
/// Called once per convert() invocation under the lock, then used without lock.
struct InstanceSnapshot {
    hfile::WriterOptions     writer_opts;
    std::vector<std::string> excluded_columns;
    std::vector<std::string> excluded_column_prefixes;
};
static InstanceSnapshot get_instance_snapshot(JNIEnv* env, jobject obj) {
    std::lock_guard<std::mutex> lk(g_config_mutex);
    const auto& s = get_or_create_instance_state_locked(env, obj);
    return {s.writer_opts, s.excluded_columns, s.excluded_column_prefixes};
}

// ─── JNI exports ─────────────────────────────────────────────────────────────
extern "C" {

/**
 * Java: public native int convert(String arrowPath, String hfilePath,
 *                                  String tableName, String rowKeyRule)
 *
 * Note: rowValue parameter was REMOVED per v4.0 design (it is derived
 * internally from Arrow columns, not passed by the caller).
 *
 * Returns: 0 = success, non-zero = ErrorCode constant
 */
JNIEXPORT jint JNICALL
Java_com_hfile_HFileSDK_convert(JNIEnv* env, jobject obj,
                                 jstring j_arrow_path,
                                 jstring j_hfile_path,
                                 jstring j_table_name,
                                 jstring j_row_key_rule)
{
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
            set_instance_result(env, obj, r);
            return r.error_code;
        }
        s = jstring_to_string(env, j_hfile_path, &hfile_path);
        if (!s.ok()) {
            auto r = make_error_result(
                s.code() == hfile::Status::Code::InvalidArg
                ? hfile::ErrorCode::INVALID_ARGUMENT
                : hfile::ErrorCode::INTERNAL_ERROR,
                "hfilePath: " + s.message());
            set_instance_result(env, obj, r);
            return r.error_code;
        }
        s = optional_jstring_to_string(env, j_table_name, &table_name);
        if (!s.ok()) {
            auto r = make_error_result(hfile::ErrorCode::INTERNAL_ERROR,
                                       "tableName: " + s.message());
            set_instance_result(env, obj, r);
            return r.error_code;
        }
        s = optional_jstring_to_string(env, j_row_key_rule, &row_key_rule);
        if (!s.ok()) {
            auto r = make_error_result(hfile::ErrorCode::INTERNAL_ERROR,
                                       "rowKeyRule: " + s.message());
            set_instance_result(env, obj, r);
            return r.error_code;
        }

        // ── Validate required parameters ──────────────────────────────────
        if (arrow_path.empty() || hfile_path.empty()) {
            auto r = make_error_result(hfile::ErrorCode::INVALID_ARGUMENT,
                                       "arrowPath and hfilePath must not be null/empty");
            set_instance_result(env, obj, r);
            return r.error_code;
        }

        // ── Build ConvertOptions ──────────────────────────────────────────
        hfile::ConvertOptions opts;
        opts.arrow_path   = arrow_path;
        opts.hfile_path   = hfile_path;
        opts.table_name   = table_name;
        opts.row_key_rule = row_key_rule;

        auto snap = get_instance_snapshot(env, obj);
        opts.writer_opts  = snap.writer_opts;
        opts.excluded_columns         = snap.excluded_columns;
        opts.excluded_column_prefixes = snap.excluded_column_prefixes;

        // If configure() set a column_family, propagate it into ConvertOptions.
        // Otherwise leave ConvertOptions::column_family at its default ("cf").
        if (!opts.writer_opts.column_family.empty())
            opts.column_family = opts.writer_opts.column_family;
        opts.writer_opts.column_family = opts.column_family;

        // ── Execute conversion ────────────────────────────────────────────
        hfile::ConvertResult result = hfile::convert(opts);

        set_instance_result(env, obj, result);

        return result.error_code;

    } catch (const std::exception& e) {
        fprintf(stderr, "[ERROR] JNI convert: %s\n", e.what());
        auto r = hfile::jni::make_error_result(
            hfile::ErrorCode::INTERNAL_ERROR,
            std::string("Internal C++ exception: ") + e.what());
        set_instance_result(env, obj, r);
        return r.error_code;
    } catch (...) {
        fprintf(stderr, "[ERROR] JNI convert: unknown exception\n");
        auto r = hfile::jni::make_error_result(
            hfile::ErrorCode::INTERNAL_ERROR,
            "Internal C++ exception: unknown");
        set_instance_result(env, obj, r);
        return r.error_code;
    }
}

/**
 * Java: public native String getLastResult()
 * Returns JSON string with the details of the last convert() call.
 */
JNIEXPORT jstring JNICALL
Java_com_hfile_HFileSDK_getLastResult(JNIEnv* env, jobject obj)
{
    try {
        std::string result_json = "{}";
        {
            std::lock_guard<std::mutex> lk(g_config_mutex);
            if (auto* state = find_instance_state_locked(env, obj))
                result_json = state->last_result_json;
        }
        return env->NewStringUTF(result_json.c_str());
    } catch (...) {
        return env->NewStringUTF("{}");
    }
}

/**
 * Java: public native int configure(String configJson)
 * Apply global configuration before the first convert() call.
 * configJson: {"compression":"lz4","block_size":65536,"column_family":"cf",...}
 */
JNIEXPORT jint JNICALL
Java_com_hfile_HFileSDK_configure(JNIEnv* env, jobject obj, jstring j_config)
{
    try {
        std::string config;
        auto str_status = hfile::jni::optional_jstring_to_string(env, j_config, &config);
        if (!str_status.ok()) {
            auto r = hfile::jni::make_error_result(hfile::ErrorCode::INTERNAL_ERROR,
                                                   "configJson: " + str_status.message());
            set_instance_result(env, obj, r);
            return r.error_code;
        }
        if (config.empty()) {
            set_instance_result(env, obj, hfile::ConvertResult{});
            return 0;
        }

        std::lock_guard<std::mutex> lk(g_config_mutex);
        auto& state = get_or_create_instance_state_locked(env, obj);
        auto next_opts = state.writer_opts;
        auto fail_config = [&](std::string message) {
            state.last_result = hfile::jni::make_error_result(
                hfile::ErrorCode::INVALID_ARGUMENT, std::move(message));
            state.last_result_json = hfile::jni::result_to_json(state.last_result);
            return state.last_result.error_code;
        };

        hfile::jni::JsonConfigObject cfg;
        auto parse_status = hfile::jni::parse_json_config(config, &cfg);
        if (!parse_status.ok()) {
            state.last_result = hfile::jni::make_error_result(
                hfile::ErrorCode::INVALID_ARGUMENT, parse_status.message());
            state.last_result_json = hfile::jni::result_to_json(state.last_result);
            return state.last_result.error_code;
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
            // Column exclusion — used for Hudi / CDC metadata columns
            "excluded_columns",          // ["col1","col2",...]  exact names
            "excluded_column_prefixes"   // ["_hoodie","_cdc_"]  prefix match
        };
        for (const auto& [key, _] : cfg) {
            if (!kAllowedKeys.count(key))
                return fail_config("Unsupported config key: " + key);
        }

        if (auto comp = hfile::jni::config_string(cfg, "compression")) {
            if      (*comp == "none")   next_opts.compression = hfile::Compression::None;
            else if (*comp == "lz4")    next_opts.compression = hfile::Compression::LZ4;
            else if (*comp == "zstd")   next_opts.compression = hfile::Compression::Zstd;
            else if (*comp == "snappy") next_opts.compression = hfile::Compression::Snappy;
            else if (*comp == "gzip")   next_opts.compression = hfile::Compression::GZip;
            else return fail_config("Invalid compression: " + *comp);
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
            if      (*enc == "NONE")      next_opts.data_block_encoding = hfile::Encoding::None;
            else if (*enc == "PREFIX")    next_opts.data_block_encoding = hfile::Encoding::Prefix;
            else if (*enc == "DIFF")      next_opts.data_block_encoding = hfile::Encoding::Diff;
            else if (*enc == "FAST_DIFF") next_opts.data_block_encoding = hfile::Encoding::FastDiff;
            else return fail_config("Invalid data_block_encoding: " + *enc);
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

        // ── Column exclusion ──────────────────────────────────────────────────
        // These are stored on InstanceState, not on WriterOptions, because they
        // are a converter-level concept (applied at Arrow → HFile mapping time).
        if (auto cols = hfile::jni::config_string_array(cfg, "excluded_columns")) {
            state.excluded_columns = std::move(*cols);
        }
        if (auto pfxs = hfile::jni::config_string_array(cfg, "excluded_column_prefixes")) {
            state.excluded_column_prefixes = std::move(*pfxs);
        }

        state.writer_opts = std::move(next_opts);
        state.last_result = {};
        state.last_result_json = hfile::jni::result_to_json(state.last_result);
        return 0;
    } catch (...) {
        auto r = hfile::jni::make_error_result(hfile::ErrorCode::INTERNAL_ERROR,
                                               "Internal C++ exception during configure()");
        set_instance_result(env, obj, r);
        return hfile::ErrorCode::INTERNAL_ERROR;
    }
}

} // extern "C"
