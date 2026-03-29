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

// Auto-generated JNI header (produced by javac -h or javah):
// Expected class: com.hfile.HFileSDK
// Method signatures mirror HFileSDK.java

namespace hfile {
namespace jni {

// Thread-local last result storage (one per JNI calling thread)
static thread_local ConvertResult tl_last_result;
static thread_local std::string   tl_last_result_json;

static std::string result_to_json(const ConvertResult& r) {
    std::ostringstream oss;
    oss << "{"
        << "\"error_code\":" << r.error_code << ','
        << "\"error_message\":\"" << json_escape(r.error_message) << "\","
        << "\"arrow_batches_read\":" << static_cast<long long>(r.arrow_batches_read) << ','
        << "\"arrow_rows_read\":" << static_cast<long long>(r.arrow_rows_read) << ','
        << "\"kv_written_count\":" << static_cast<long long>(r.kv_written_count) << ','
        << "\"kv_skipped_count\":" << static_cast<long long>(r.kv_skipped_count) << ','
        << "\"hfile_size_bytes\":" << static_cast<long long>(r.hfile_size_bytes) << ','
        << "\"elapsed_ms\":" << static_cast<long long>(r.elapsed_ms.count()) << ','
        << "\"sort_ms\":" << static_cast<long long>(r.sort_ms.count()) << ','
        << "\"write_ms\":" << static_cast<long long>(r.write_ms.count())
        << "}";
    return oss.str();
}

} // namespace jni
} // namespace hfile

// ─── Global config (set via configure()) ─────────────────────────────────────
static hfile::WriterOptions g_writer_opts;
static std::mutex           g_config_mutex;

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
Java_com_hfile_HFileSDK_convert(JNIEnv* env, jobject /*obj*/,
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
            tl_last_result = {};
            tl_last_result.error_code = s.code() == hfile::Status::Code::InvalidArg
                ? hfile::ErrorCode::INVALID_ARGUMENT
                : hfile::ErrorCode::INTERNAL_ERROR;
            tl_last_result.error_message = "arrowPath: " + s.message();
            tl_last_result_json = result_to_json(tl_last_result);
            return tl_last_result.error_code;
        }
        s = jstring_to_string(env, j_hfile_path, &hfile_path);
        if (!s.ok()) {
            tl_last_result = {};
            tl_last_result.error_code = s.code() == hfile::Status::Code::InvalidArg
                ? hfile::ErrorCode::INVALID_ARGUMENT
                : hfile::ErrorCode::INTERNAL_ERROR;
            tl_last_result.error_message = "hfilePath: " + s.message();
            tl_last_result_json = result_to_json(tl_last_result);
            return tl_last_result.error_code;
        }
        s = optional_jstring_to_string(env, j_table_name, &table_name);
        if (!s.ok()) {
            tl_last_result = {};
            tl_last_result.error_code = hfile::ErrorCode::INTERNAL_ERROR;
            tl_last_result.error_message = "tableName: " + s.message();
            tl_last_result_json = result_to_json(tl_last_result);
            return tl_last_result.error_code;
        }
        s = optional_jstring_to_string(env, j_row_key_rule, &row_key_rule);
        if (!s.ok()) {
            tl_last_result = {};
            tl_last_result.error_code = hfile::ErrorCode::INTERNAL_ERROR;
            tl_last_result.error_message = "rowKeyRule: " + s.message();
            tl_last_result_json = result_to_json(tl_last_result);
            return tl_last_result.error_code;
        }

        // ── Validate required parameters ──────────────────────────────────
        if (arrow_path.empty() || hfile_path.empty()) {
            tl_last_result = {};
            tl_last_result.error_code    = hfile::ErrorCode::INVALID_ARGUMENT;
            tl_last_result.error_message = "arrowPath and hfilePath must not be null/empty";
            tl_last_result_json = result_to_json(tl_last_result);
            return hfile::ErrorCode::INVALID_ARGUMENT;
        }

        // ── Build ConvertOptions ──────────────────────────────────────────
        hfile::ConvertOptions opts;
        opts.arrow_path   = arrow_path;
        opts.hfile_path   = hfile_path;
        opts.table_name   = table_name;
        opts.row_key_rule = row_key_rule;
        {
            std::lock_guard<std::mutex> lk(g_config_mutex);
            opts.writer_opts = g_writer_opts;
        }

        // ── Execute conversion ────────────────────────────────────────────
        hfile::ConvertResult result = hfile::convert(opts);

        // Store for getLastResult()
        tl_last_result      = result;
        tl_last_result_json = result_to_json(result);

        return result.error_code;

    } catch (const std::exception& e) {
        fprintf(stderr, "[ERROR] JNI convert: %s\n", e.what());
        hfile::ConvertResult r;
        r.error_code    = hfile::ErrorCode::INTERNAL_ERROR;
        r.error_message = std::string("Internal C++ exception: ") + e.what();
        hfile::jni::tl_last_result      = r;
        hfile::jni::tl_last_result_json = hfile::jni::result_to_json(r);
        return hfile::ErrorCode::INTERNAL_ERROR;
    } catch (...) {
        fprintf(stderr, "[ERROR] JNI convert: unknown exception\n");
        hfile::ConvertResult r;
        r.error_code    = hfile::ErrorCode::INTERNAL_ERROR;
        r.error_message = "Internal C++ exception: unknown";
        hfile::jni::tl_last_result      = r;
        hfile::jni::tl_last_result_json = hfile::jni::result_to_json(r);
        return hfile::ErrorCode::INTERNAL_ERROR;
    }
}

/**
 * Java: public native String getLastResult()
 * Returns JSON string with the details of the last convert() call.
 */
JNIEXPORT jstring JNICALL
Java_com_hfile_HFileSDK_getLastResult(JNIEnv* env, jobject /*obj*/)
{
    try {
        return env->NewStringUTF(hfile::jni::tl_last_result_json.c_str());
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
Java_com_hfile_HFileSDK_configure(JNIEnv* env, jobject /*obj*/, jstring j_config)
{
    try {
        std::string config;
        auto str_status = hfile::jni::optional_jstring_to_string(env, j_config, &config);
        if (!str_status.ok()) return hfile::ErrorCode::INTERNAL_ERROR;
        if (config.empty()) return 0;

        std::lock_guard<std::mutex> lk(g_config_mutex);

        hfile::jni::JsonConfigObject cfg;
        auto parse_status = hfile::jni::parse_json_config(config, &cfg);
        if (!parse_status.ok()) return hfile::ErrorCode::INVALID_ARGUMENT;

        static const std::unordered_set<std::string> kAllowedKeys = {
            "compression",
            "block_size",
            "column_family",
            "data_block_encoding",
            "fsync_policy",
            "error_policy",
            "bloom_type"
        };
        for (const auto& [key, _] : cfg) {
            if (!kAllowedKeys.count(key))
                return hfile::ErrorCode::INVALID_ARGUMENT;
        }

        if (auto comp = hfile::jni::config_string(cfg, "compression")) {
            if      (*comp == "none")   g_writer_opts.compression = hfile::Compression::None;
            else if (*comp == "lz4")    g_writer_opts.compression = hfile::Compression::LZ4;
            else if (*comp == "zstd")   g_writer_opts.compression = hfile::Compression::Zstd;
            else if (*comp == "snappy") g_writer_opts.compression = hfile::Compression::Snappy;
            else if (*comp == "gzip")   g_writer_opts.compression = hfile::Compression::GZip;
            else return hfile::ErrorCode::INVALID_ARGUMENT;
        }

        if (auto bs = hfile::jni::config_int(cfg, "block_size")) {
            if (*bs <= 0) return hfile::ErrorCode::INVALID_ARGUMENT;
            g_writer_opts.block_size = static_cast<size_t>(*bs);
        }

        if (auto cf = hfile::jni::config_string(cfg, "column_family")) {
            if (cf->empty()) return hfile::ErrorCode::INVALID_ARGUMENT;
            g_writer_opts.column_family = *cf;
        }

        if (auto enc = hfile::jni::config_string(cfg, "data_block_encoding")) {
            if      (*enc == "NONE")      g_writer_opts.data_block_encoding = hfile::Encoding::None;
            else if (*enc == "PREFIX")    g_writer_opts.data_block_encoding = hfile::Encoding::Prefix;
            else if (*enc == "DIFF")      g_writer_opts.data_block_encoding = hfile::Encoding::Diff;
            else if (*enc == "FAST_DIFF") g_writer_opts.data_block_encoding = hfile::Encoding::FastDiff;
            else return hfile::ErrorCode::INVALID_ARGUMENT;
        }

        if (auto fp = hfile::jni::config_string(cfg, "fsync_policy")) {
            if      (*fp == "safe")      g_writer_opts.fsync_policy = hfile::FsyncPolicy::Safe;
            else if (*fp == "fast")      g_writer_opts.fsync_policy = hfile::FsyncPolicy::Fast;
            else if (*fp == "paranoid")  g_writer_opts.fsync_policy = hfile::FsyncPolicy::Paranoid;
            else return hfile::ErrorCode::INVALID_ARGUMENT;
        }

        if (auto ep = hfile::jni::config_string(cfg, "error_policy")) {
            if      (*ep == "strict")    g_writer_opts.error_policy = hfile::ErrorPolicy::Strict;
            else if (*ep == "skip_row")  g_writer_opts.error_policy = hfile::ErrorPolicy::SkipRow;
            else if (*ep == "skip_batch")g_writer_opts.error_policy = hfile::ErrorPolicy::SkipBatch;
            else return hfile::ErrorCode::INVALID_ARGUMENT;
        }

        if (auto bt = hfile::jni::config_string(cfg, "bloom_type")) {
            if      (*bt == "none")     g_writer_opts.bloom_type = hfile::BloomType::None;
            else if (*bt == "row")      g_writer_opts.bloom_type = hfile::BloomType::Row;
            else if (*bt == "rowcol")   g_writer_opts.bloom_type = hfile::BloomType::RowCol;
            else return hfile::ErrorCode::INVALID_ARGUMENT;
        }

        return 0;
    } catch (...) {
        return hfile::ErrorCode::INTERNAL_ERROR;
    }
}

} // extern "C"
