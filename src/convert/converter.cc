#include "converter.h"
#include "convert_options.h"
#include "../arrow/row_key_builder.h"

#include <hfile/writer.h>
#include <hfile/types.h>

// Arrow C++ headers (Arrow 15+)
#include <arrow/api.h>
#include <arrow/io/file.h>
#include <arrow/ipc/reader.h>
#include <arrow/array.h>
#include <arrow/scalar.h>
#include <arrow/type.h>

#include <algorithm>
#include <vector>
#include <string>
#include <cstring>
#include <chrono>
#include <filesystem>
#include <cstdio>

namespace hfile {

// ─── Logging (no external dep) ───────────────────────────────────────────────
namespace clog {
static void info (const std::string& m){ fprintf(stderr,"[INFO]  convert: %s\n",m.c_str()); }
static void warn (const std::string& m){ fprintf(stderr,"[WARN]  convert: %s\n",m.c_str()); }
static void err  (const std::string& m){ fprintf(stderr,"[ERROR] convert: %s\n",m.c_str()); }
}

// ─── SortEntry ────────────────────────────────────────────────────────────────

struct SortEntry {
    std::string row_key;     // fully built row key string
    int32_t     batch_idx;   // which RecordBatch (0-based)
    int32_t     row_idx;     // row within that batch
};

// ─── Arrow scalar → UTF-8 string (for rowValue building) ──────────────────────

static std::string scalar_to_string(const arrow::Array& arr, int64_t row) {
    if (arr.IsNull(row)) return "";

    using T = arrow::Type;
    auto t = arr.type_id();

    switch (t) {
    case T::STRING: case T::LARGE_STRING: {
        auto& sa = static_cast<const arrow::StringArray&>(arr);
        auto sv = sa.GetView(row);
        return std::string(sv.data(), sv.size());
    }
    case T::INT8:   return std::to_string(static_cast<const arrow::Int8Array&>(arr).Value(row));
    case T::INT16:  return std::to_string(static_cast<const arrow::Int16Array&>(arr).Value(row));
    case T::INT32:  return std::to_string(static_cast<const arrow::Int32Array&>(arr).Value(row));
    case T::INT64:  return std::to_string(static_cast<const arrow::Int64Array&>(arr).Value(row));
    case T::UINT8:  return std::to_string(static_cast<const arrow::UInt8Array&>(arr).Value(row));
    case T::UINT16: return std::to_string(static_cast<const arrow::UInt16Array&>(arr).Value(row));
    case T::UINT32: return std::to_string(static_cast<const arrow::UInt32Array&>(arr).Value(row));
    case T::UINT64: return std::to_string(static_cast<const arrow::UInt64Array&>(arr).Value(row));
    case T::FLOAT:  return std::to_string(static_cast<const arrow::FloatArray&>(arr).Value(row));
    case T::DOUBLE: return std::to_string(static_cast<const arrow::DoubleArray&>(arr).Value(row));
    case T::BOOL:
        return static_cast<const arrow::BooleanArray&>(arr).Value(row) ? "1" : "0";
    case T::TIMESTAMP: {
        return std::to_string(static_cast<const arrow::Int64Array&>(arr).Value(row));
    }
    default:
        // Fallback: ToString
        auto s = arr.GetScalar(row);
        if (s.ok()) return s.ValueOrDie()->ToString();
        return "";
    }
}

// ─── Arrow scalar → Big-Endian bytes (for HBase value) ────────────────────────

static std::vector<uint8_t> scalar_to_bytes(const arrow::Array& arr, int64_t row) {
    if (arr.IsNull(row)) return {};

    using T = arrow::Type;
    auto t = arr.type_id();

    auto put8  = [](uint8_t v)  { return std::vector<uint8_t>{v}; };
    auto put16 = [](uint16_t v) {
        return std::vector<uint8_t>{
            static_cast<uint8_t>(v >> 8), static_cast<uint8_t>(v)};
    };
    auto put32 = [](uint32_t v) {
        return std::vector<uint8_t>{
            static_cast<uint8_t>(v>>24), static_cast<uint8_t>(v>>16),
            static_cast<uint8_t>(v>>8),  static_cast<uint8_t>(v)};
    };
    auto put64 = [](uint64_t v) {
        std::vector<uint8_t> b(8);
        for (int i = 7; i >= 0; --i) { b[i] = v & 0xFF; v >>= 8; }
        return b;
    };

    switch (t) {
    case T::STRING: case T::LARGE_STRING: {
        auto& sa = static_cast<const arrow::StringArray&>(arr);
        auto sv = sa.GetView(row);
        return std::vector<uint8_t>(sv.begin(), sv.end());
    }
    case T::BINARY: case T::LARGE_BINARY: {
        auto& ba = static_cast<const arrow::BinaryArray&>(arr);
        auto sv = ba.GetView(row);
        return std::vector<uint8_t>(sv.begin(), sv.end());
    }
    case T::BOOL:   return put8(static_cast<const arrow::BooleanArray&>(arr).Value(row)?1:0);
    case T::INT8:   return put8(static_cast<uint8_t>(
                        static_cast<const arrow::Int8Array&>(arr).Value(row)));
    case T::INT16:  return put16(static_cast<uint16_t>(
                        static_cast<const arrow::Int16Array&>(arr).Value(row)));
    case T::INT32:  return put32(static_cast<uint32_t>(
                        static_cast<const arrow::Int32Array&>(arr).Value(row)));
    case T::INT64:  return put64(static_cast<uint64_t>(
                        static_cast<const arrow::Int64Array&>(arr).Value(row)));
    case T::UINT8:  return put8(static_cast<const arrow::UInt8Array&>(arr).Value(row));
    case T::UINT16: return put16(static_cast<const arrow::UInt16Array&>(arr).Value(row));
    case T::UINT32: return put32(static_cast<const arrow::UInt32Array&>(arr).Value(row));
    case T::UINT64: return put64(static_cast<const arrow::UInt64Array&>(arr).Value(row));
    case T::FLOAT: {
        float v = static_cast<const arrow::FloatArray&>(arr).Value(row);
        uint32_t bits; std::memcpy(&bits, &v, 4);
        return put32(bits);
    }
    case T::DOUBLE: {
        double v = static_cast<const arrow::DoubleArray&>(arr).Value(row);
        uint64_t bits; std::memcpy(&bits, &v, 8);
        return put64(bits);
    }
    case T::TIMESTAMP: {
        int64_t v = static_cast<const arrow::Int64Array&>(arr).Value(row);
        return put64(static_cast<uint64_t>(v));
    }
    default: {
        // Fallback via string
        auto s = arr.GetScalar(row);
        if (s.ok()) {
            auto str = s.ValueOrDie()->ToString();
            return std::vector<uint8_t>(str.begin(), str.end());
        }
        return {};
    }
    }
}

// ─── Open Arrow IPC Stream file ───────────────────────────────────────────────

static arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchStreamReader>>
open_arrow_stream(const std::string& path) {
    ARROW_ASSIGN_OR_RAISE(auto file, arrow::io::ReadableFile::Open(path));
    return arrow::ipc::RecordBatchStreamReader::Open(file);
}

// ─── First pass: build sort index ────────────────────────────────────────────

static Status build_sort_index(
        const std::string&                  arrow_path,
        arrow_convert::RowKeyBuilder&       rkb,
        int                                 max_col_idx,
        std::vector<SortEntry>&             index_out,
        std::vector<std::shared_ptr<arrow::RecordBatch>>& batches_out,
        ConvertResult&                      result) {

    auto reader_res = open_arrow_stream(arrow_path);
    if (!reader_res.ok())
        return Status::IoError("Cannot open Arrow file: " + reader_res.status().ToString());

    auto reader = reader_res.ValueOrDie();
    auto schema = reader->schema();
    int num_cols = schema->num_fields();

    std::shared_ptr<arrow::RecordBatch> batch;
    int32_t batch_idx = 0;

    while (true) {
        auto s = reader->ReadNext(&batch);
        if (!s.ok()) return Status::IoError("Arrow read error: " + s.ToString());
        if (!batch) break;  // EOS

        int64_t n_rows = batch->num_rows();
        batches_out.push_back(batch);

        for (int64_t r = 0; r < n_rows; ++r) {
            // Build rowValue: pipe-join all column string values
            // Max index needed = max_col_idx, so we only need columns 0..max_col_idx
            std::string row_val;
            int max_i = std::min(max_col_idx + 1, num_cols);
            row_val.reserve(static_cast<size_t>(max_i) * 12);
            for (int c = 0; c < max_i; ++c) {
                if (c > 0) row_val += '|';
                row_val += scalar_to_string(*batch->column(c), r);
            }

            // Build fields view
            auto fields = arrow_convert::split_row_value(row_val,
                                                          max_col_idx + 2);
            std::string rk = rkb.build(fields);

            if (rk.empty()) {
                result.kv_skipped_count++;
                // Use empty placeholder so row_idx is preserved
                rk = "";
            }

            index_out.push_back({std::move(rk),
                                  batch_idx,
                                  static_cast<int32_t>(r)});
        }

        result.arrow_batches_read++;
        result.arrow_rows_read += n_rows;
        batch_idx++;
    }
    return Status::OK();
}

// ─── Write KVs for one row in sorted qualifier order ─────────────────────────

static Status write_row_kvs(
        HFileWriter&                        writer,
        const arrow::RecordBatch&           batch,
        int64_t                             row_idx,
        const std::string&                  row_key,
        const std::string&                  cf,
        int64_t                             default_ts) {

    auto schema = batch.schema();
    int num_cols = batch.num_columns();

    // Collect (qualifier, value) pairs, then sort by qualifier ASC
    // (HBase ordering: Row↑ → Family↑ → Qualifier↑ → Timestamp↓)
    struct ColKV {
        std::string qualifier;
        std::vector<uint8_t> value;
    };
    std::vector<ColKV> col_kvs;
    col_kvs.reserve(static_cast<size_t>(num_cols));

    for (int c = 0; c < num_cols; ++c) {
        if (batch.column(c)->IsNull(row_idx)) continue;
        auto val = scalar_to_bytes(*batch.column(c), row_idx);
        if (val.empty()) continue;
        col_kvs.push_back({schema->field(c)->name(), std::move(val)});
    }

    // Sort qualifiers alphabetically
    std::sort(col_kvs.begin(), col_kvs.end(),
              [](const ColKV& a, const ColKV& b){
                  return a.qualifier < b.qualifier;
              });

    // Determine timestamp
    int64_t ts = default_ts > 0 ? default_ts
                : std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::system_clock::now().time_since_epoch()).count();

    // Emit KVs
    std::vector<uint8_t> row_bytes(row_key.begin(), row_key.end());
    std::vector<uint8_t> cf_bytes(cf.begin(), cf.end());

    for (const auto& ckv : col_kvs) {
        std::vector<uint8_t> q_bytes(ckv.qualifier.begin(), ckv.qualifier.end());
        auto s = writer.append(row_bytes, cf_bytes, q_bytes,
                               ts, ckv.value);
        if (!s.ok()) return s;
    }
    return Status::OK();
}

// ─── convert() ───────────────────────────────────────────────────────────────

ConvertResult convert(const ConvertOptions& opts) {
    ConvertResult result;
    auto t_start = std::chrono::steady_clock::now();

    clog::info("Convert started: arrow=" + opts.arrow_path +
               " hfile=" + opts.hfile_path +
               " rule=" + opts.row_key_rule);

    // ── 1. Validate inputs ────────────────────────────────────────────────
    if (opts.arrow_path.empty()) {
        result.error_code = ErrorCode::INVALID_ARGUMENT;
        result.error_message = "arrow_path is empty";
        return result;
    }
    if (opts.hfile_path.empty()) {
        result.error_code = ErrorCode::INVALID_ARGUMENT;
        result.error_message = "hfile_path is empty";
        return result;
    }
    if (!std::filesystem::exists(opts.arrow_path)) {
        result.error_code = ErrorCode::ARROW_FILE_ERROR;
        result.error_message = "Arrow file not found: " + opts.arrow_path;
        return result;
    }

    // ── 2. Compile rowKeyRule ─────────────────────────────────────────────
    if (opts.row_key_rule.empty()) {
        result.error_code = ErrorCode::INVALID_ROW_KEY_RULE;
        result.error_message = "row_key_rule is empty";
        return result;
    }
    auto [rkb, rk_status] = arrow_convert::RowKeyBuilder::compile(opts.row_key_rule);
    if (!rk_status.ok()) {
        result.error_code = ErrorCode::INVALID_ROW_KEY_RULE;
        result.error_message = rk_status.message();
        clog::err("rowKeyRule compile failed: " + rk_status.message());
        return result;
    }
    int max_col_idx = rkb.max_col_index();

    // ── 3. First pass: build sort index (load all batches into memory) ────
    clog::info("Pass 1: building sort index...");
    std::vector<SortEntry> sort_index;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

    auto t_sort_start = std::chrono::steady_clock::now();
    {
        Status s = build_sort_index(opts.arrow_path, rkb, max_col_idx,
                                    sort_index, batches, result);
        if (!s.ok()) {
            result.error_code = ErrorCode::ARROW_FILE_ERROR;
            result.error_message = s.message();
            clog::err("Pass 1 failed: " + s.message());
            return result;
        }
    }

    // Filter out empty row keys (rows that failed key generation)
    sort_index.erase(
        std::remove_if(sort_index.begin(), sort_index.end(),
                       [](const SortEntry& e){ return e.row_key.empty(); }),
        sort_index.end());

    // Sort by row key (lexicographic = HBase Row ordering)
    std::stable_sort(sort_index.begin(), sort_index.end(),
                     [](const SortEntry& a, const SortEntry& b){
                         return a.row_key < b.row_key;
                     });

    result.sort_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t_sort_start);

    clog::info("Pass 1 done: rows=" + std::to_string(sort_index.size()) +
               " sort=" + std::to_string(result.sort_ms.count()) + "ms");

    if (sort_index.empty()) {
        clog::warn("No valid rows after sort index build — writing empty HFile");
    }

    // ── 4. Open HFile writer ─────────────────────────────────────────────
    WriterOptions wo          = opts.writer_opts;
    wo.column_family          = opts.column_family;
    wo.sort_mode              = WriterOptions::SortMode::PreSortedTrusted;  // we sorted

    auto [writer, ws] = HFileWriter::builder()
        .set_path(opts.hfile_path)
        .set_column_family(wo.column_family)
        .set_compression(wo.compression)
        .set_block_size(wo.block_size)
        .set_data_block_encoding(wo.data_block_encoding)
        .set_bloom_type(wo.bloom_type)
        .set_fsync_policy(wo.fsync_policy)
        .set_error_policy(wo.error_policy)
        .build();

    if (!ws.ok()) {
        result.error_code = ErrorCode::IO_ERROR;
        result.error_message = ws.message();
        clog::err("HFile open failed: " + ws.message());
        return result;
    }

    // ── 5. Second pass: write in sorted order ────────────────────────────
    clog::info("Pass 2: writing HFile in sorted order...");
    auto t_write_start = std::chrono::steady_clock::now();

    int64_t progress_step = std::max(int64_t(1),
        static_cast<int64_t>(sort_index.size()) / 20);
    int64_t rows_done = 0;

    for (const auto& entry : sort_index) {
        const auto& batch = batches[static_cast<size_t>(entry.batch_idx)];
        auto s = write_row_kvs(*writer, *batch, entry.row_idx,
                               entry.row_key,
                               opts.column_family,
                               opts.default_timestamp);
        if (!s.ok()) {
            // SkipRow: log and continue
            result.kv_skipped_count++;
            clog::warn("Row write failed, skipping: " + s.message());
            continue;
        }
        result.kv_written_count++;

        ++rows_done;
        if (opts.progress_cb && rows_done % progress_step == 0)
            opts.progress_cb(rows_done,
                             static_cast<int64_t>(sort_index.size()));
    }

    // ── 6. Finish HFile ───────────────────────────────────────────────────
    auto fs = writer->finish();
    if (!fs.ok()) {
        result.error_code = ErrorCode::IO_ERROR;
        result.error_message = fs.message();
        clog::err("HFile finish failed: " + fs.message());
        return result;
    }

    result.write_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t_write_start);

    // HFile size
    std::error_code ec;
    auto sz = std::filesystem::file_size(opts.hfile_path, ec);
    if (!ec) result.hfile_size_bytes = static_cast<int64_t>(sz);

    result.elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t_start);

    clog::info("Convert done: kvs=" + std::to_string(result.kv_written_count) +
               " skipped=" + std::to_string(result.kv_skipped_count) +
               " hfile=" + std::to_string(result.hfile_size_bytes / 1024) + "KB" +
               " elapsed=" + std::to_string(result.elapsed_ms.count()) + "ms");

    return result;
}

} // namespace hfile
