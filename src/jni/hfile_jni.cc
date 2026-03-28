#include "jni_utils.h"
#include "../convert/converter.h"
#include "../convert/convert_options.h"

#include <hfile/writer_options.h>

#include <jni.h>
#include <string>
#include <stdexcept>
#include <cstdio>
#include <mutex>

// Auto-generated JNI header (produced by javac -h or javah):
// Expected class: com.hfile.HFileSDK
// Method signatures mirror HFileSDK.java

namespace hfile {
namespace jni {

// Thread-local last result storage (one per JNI calling thread)
static thread_local ConvertResult tl_last_result;
static thread_local std::string   tl_last_result_json;

/// Serialise a ConvertResult to JSON (no external dep).
static std::string result_to_json(const ConvertResult& r) {
    char buf[1024];
    std::snprintf(buf, sizeof(buf),
        "{"
        "\"error_code\":%d,"
        "\"error_message\":\"%s\","
        "\"arrow_batches_read\":%lld,"
        "\"arrow_rows_read\":%lld,"
        "\"kv_written_count\":%lld,"
        "\"kv_skipped_count\":%lld,"
        "\"hfile_size_bytes\":%lld,"
        "\"elapsed_ms\":%lld,"
        "\"sort_ms\":%lld,"
        "\"write_ms\":%lld"
        "}",
        r.error_code,
        r.error_message.c_str(),
        (long long)r.arrow_batches_read,
        (long long)r.arrow_rows_read,
        (long long)r.kv_written_count,
        (long long)r.kv_skipped_count,
        (long long)r.hfile_size_bytes,
        (long long)r.elapsed_ms.count(),
        (long long)r.sort_ms.count(),
        (long long)r.write_ms.count());
    return std::string(buf);
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
        std::string arrow_path   = jstring_to_string(env, j_arrow_path);
        std::string hfile_path   = jstring_to_string(env, j_hfile_path);
        std::string table_name   = jstring_to_string(env, j_table_name);
        std::string row_key_rule = jstring_to_string(env, j_row_key_rule);

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
        std::string config = hfile::jni::jstring_to_string(env, j_config);
        if (config.empty()) return 0;

        std::lock_guard<std::mutex> lk(g_config_mutex);

        // Simple key-value JSON parsing (no external dep)
        // Supported keys: compression, block_size, column_family, bloom_filter,
        //                  data_block_encoding, fsync_policy, error_policy
        auto get_str = [&](const std::string& key) -> std::string {
            // Find "key":"value"
            std::string search = "\"" + key + "\"";
            auto pos = config.find(search);
            if (pos == std::string::npos) return "";
            pos = config.find(':', pos + search.size());
            if (pos == std::string::npos) return "";
            pos = config.find('"', pos);
            if (pos == std::string::npos) return "";
            auto end = config.find('"', pos + 1);
            if (end == std::string::npos) return "";
            return config.substr(pos + 1, end - pos - 1);
        };
        auto get_int = [&](const std::string& key, int def) -> int {
            auto pos = config.find("\"" + key + "\"");
            if (pos == std::string::npos) return def;
            pos = config.find(':', pos);
            if (pos == std::string::npos) return def;
            ++pos;
            while (pos < config.size() && config[pos] == ' ') ++pos;
            try { return std::stoi(config.substr(pos)); }
            catch (...) { return def; }
        };

        std::string comp = get_str("compression");
        if (!comp.empty()) {
            if      (comp == "none")   g_writer_opts.compression = hfile::Compression::None;
            else if (comp == "lz4")    g_writer_opts.compression = hfile::Compression::LZ4;
            else if (comp == "zstd")   g_writer_opts.compression = hfile::Compression::Zstd;
            else if (comp == "snappy") g_writer_opts.compression = hfile::Compression::Snappy;
            else if (comp == "gzip")   g_writer_opts.compression = hfile::Compression::GZip;
        }

        int bs = get_int("block_size", 0);
        if (bs > 0) g_writer_opts.block_size = static_cast<size_t>(bs);

        std::string cf = get_str("column_family");
        if (!cf.empty()) g_writer_opts.column_family = cf;

        std::string enc = get_str("data_block_encoding");
        if (!enc.empty()) {
            if      (enc == "NONE")      g_writer_opts.data_block_encoding = hfile::Encoding::None;
            else if (enc == "PREFIX")    g_writer_opts.data_block_encoding = hfile::Encoding::Prefix;
            else if (enc == "DIFF")      g_writer_opts.data_block_encoding = hfile::Encoding::Diff;
            else if (enc == "FAST_DIFF") g_writer_opts.data_block_encoding = hfile::Encoding::FastDiff;
        }

        return 0;
    } catch (...) {
        return hfile::ErrorCode::INTERNAL_ERROR;
    }
}

} // extern "C"
