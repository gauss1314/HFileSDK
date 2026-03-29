#include "converter.h"
#include "convert_options.h"
#include "../arrow/row_key_builder.h"
#include "../memory/memory_budget.h"

#include <hfile/writer.h>
#include <hfile/types.h>

// Arrow C++ headers (Arrow 15+)
#include <arrow/api.h>
#include <arrow/io/file.h>
#include <arrow/ipc/reader.h>
#include <arrow/array.h>
#include <arrow/scalar.h>
#include <arrow/type.h>
#include <arrow/util/byte_size.h>

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

struct GroupedCell {
    std::string          qualifier;
    std::vector<uint8_t> value;
};

// ─── Arrow scalar → UTF-8 string (for rowValue building) ──────────────────────

static int64_t normalize_timestamp_to_millis(int64_t value, arrow::TimeUnit::type unit) {
    switch (unit) {
    case arrow::TimeUnit::SECOND: return value * 1000;
    case arrow::TimeUnit::MILLI:  return value;
    case arrow::TimeUnit::MICRO:  return value / 1000;
    case arrow::TimeUnit::NANO:   return value / 1000000;
    }
    return value;
}

static bool is_supported_rowkey_type(const std::shared_ptr<arrow::DataType>& type) {
    using T = arrow::Type;
    switch (type->id()) {
    case T::STRING:
    case T::LARGE_STRING:
    case T::INT8:
    case T::INT16:
    case T::INT32:
    case T::INT64:
    case T::UINT8:
    case T::UINT16:
    case T::UINT32:
    case T::UINT64:
    case T::FLOAT:
    case T::DOUBLE:
    case T::BOOL:
    case T::TIMESTAMP:
        return true;
    default:
        return false;
    }
}

static Status scalar_to_string(const arrow::Array& arr, int64_t row, std::string* out) {
    out->clear();
    if (arr.IsNull(row)) return Status::OK();

    using T = arrow::Type;
    auto t = arr.type_id();

    switch (t) {
    case T::STRING: {
        auto& sa = static_cast<const arrow::StringArray&>(arr);
        auto sv = sa.GetView(row);
        *out = std::string(sv.data(), sv.size());
        return Status::OK();
    }
    case T::LARGE_STRING: {
        auto& sa = static_cast<const arrow::LargeStringArray&>(arr);
        auto sv = sa.GetView(row);
        *out = std::string(sv.data(), sv.size());
        return Status::OK();
    }
    case T::INT8:   *out = std::to_string(static_cast<const arrow::Int8Array&>(arr).Value(row)); return Status::OK();
    case T::INT16:  *out = std::to_string(static_cast<const arrow::Int16Array&>(arr).Value(row)); return Status::OK();
    case T::INT32:  *out = std::to_string(static_cast<const arrow::Int32Array&>(arr).Value(row)); return Status::OK();
    case T::INT64:  *out = std::to_string(static_cast<const arrow::Int64Array&>(arr).Value(row)); return Status::OK();
    case T::UINT8:  *out = std::to_string(static_cast<const arrow::UInt8Array&>(arr).Value(row)); return Status::OK();
    case T::UINT16: *out = std::to_string(static_cast<const arrow::UInt16Array&>(arr).Value(row)); return Status::OK();
    case T::UINT32: *out = std::to_string(static_cast<const arrow::UInt32Array&>(arr).Value(row)); return Status::OK();
    case T::UINT64: *out = std::to_string(static_cast<const arrow::UInt64Array&>(arr).Value(row)); return Status::OK();
    case T::FLOAT:  *out = std::to_string(static_cast<const arrow::FloatArray&>(arr).Value(row)); return Status::OK();
    case T::DOUBLE: *out = std::to_string(static_cast<const arrow::DoubleArray&>(arr).Value(row)); return Status::OK();
    case T::BOOL:   *out = static_cast<const arrow::BooleanArray&>(arr).Value(row) ? "1" : "0"; return Status::OK();
    case T::TIMESTAMP: {
        auto& ta = static_cast<const arrow::TimestampArray&>(arr);
        auto unit = static_cast<const arrow::TimestampType&>(*arr.type()).unit();
        *out = std::to_string(normalize_timestamp_to_millis(ta.Value(row), unit));
        return Status::OK();
    }
    default:
        return Status::InvalidArg("SCHEMA_MISMATCH: unsupported Arrow type for row key field: " +
                                  arr.type()->ToString());
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
    case T::STRING: {
        auto& sa = static_cast<const arrow::StringArray&>(arr);
        auto sv = sa.GetView(row);
        return std::vector<uint8_t>(sv.begin(), sv.end());
    }
    case T::LARGE_STRING: {
        auto& sa = static_cast<const arrow::LargeStringArray&>(arr);
        auto sv = sa.GetView(row);
        return std::vector<uint8_t>(sv.begin(), sv.end());
    }
    case T::BINARY: {
        auto& ba = static_cast<const arrow::BinaryArray&>(arr);
        auto sv = ba.GetView(row);
        return std::vector<uint8_t>(sv.begin(), sv.end());
    }
    case T::LARGE_BINARY: {
        auto& ba = static_cast<const arrow::LargeBinaryArray&>(arr);
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
        auto& ta = static_cast<const arrow::TimestampArray&>(arr);
        auto unit = static_cast<const arrow::TimestampType&>(*arr.type()).unit();
        int64_t v = normalize_timestamp_to_millis(ta.Value(row), unit);
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
        memory::MemoryBudget*               budget,
        ConvertResult&                      result) {

    auto reader_res = open_arrow_stream(arrow_path);
    if (!reader_res.ok())
        return Status::IoError("Cannot open Arrow file: " + reader_res.status().ToString());

    auto reader = reader_res.ValueOrDie();
    auto schema = reader->schema();
    int num_cols = schema->num_fields();

    if (max_col_idx >= num_cols) {
        return Status::InvalidArg(
            "SCHEMA_MISMATCH: rowKeyRule references column index " +
            std::to_string(max_col_idx) + " but Arrow schema has only " +
            std::to_string(num_cols) + " columns");
    }

    std::vector<bool> referenced_cols(static_cast<size_t>(std::max(0, num_cols)), false);
    for (const auto& seg : rkb.segments()) {
        if (seg.type != arrow_convert::RowKeySegment::Type::ColumnRef &&
            seg.type != arrow_convert::RowKeySegment::Type::EncodedColumn)
            continue;
        if (seg.col_index >= num_cols) {
            return Status::InvalidArg(
                "SCHEMA_MISMATCH: rowKeyRule references column index " +
                std::to_string(seg.col_index) + " but Arrow schema has only " +
                std::to_string(num_cols) + " columns");
        }
        referenced_cols[static_cast<size_t>(seg.col_index)] = true;
    }
    for (int c = 0; c < num_cols; ++c) {
        if (!referenced_cols[static_cast<size_t>(c)])
            continue;
        if (!is_supported_rowkey_type(schema->field(c)->type())) {
            return Status::InvalidArg(
                "SCHEMA_MISMATCH: unsupported Arrow type for row key field '" +
                schema->field(c)->name() + "': " + schema->field(c)->type()->ToString());
        }
    }

    std::shared_ptr<arrow::RecordBatch> batch;
    int32_t batch_idx = 0;

    while (true) {
        auto s = reader->ReadNext(&batch);
        if (!s.ok()) return Status::IoError("Arrow read error: " + s.ToString());
        if (!batch) break;  // EOS

        int64_t n_rows = batch->num_rows();
        if (budget) {
            auto reserve_status = budget->reserve(
                static_cast<size_t>(std::max<int64_t>(0, arrow::util::TotalBufferSize(*batch))));
            if (!reserve_status.ok()) return reserve_status;
        }
        batches_out.push_back(batch);

        for (int64_t r = 0; r < n_rows; ++r) {
            std::vector<std::string> owned_fields(
                max_col_idx >= 0 ? static_cast<size_t>(max_col_idx + 1) : 0);
            std::vector<std::string_view> fields(owned_fields.size());
            for (int c = 0; c <= max_col_idx; ++c) {
                if (!referenced_cols[static_cast<size_t>(c)])
                    continue;
                auto field_status = scalar_to_string(*batch->column(c), r, &owned_fields[static_cast<size_t>(c)]);
                if (!field_status.ok())
                    return field_status;
                fields[static_cast<size_t>(c)] = owned_fields[static_cast<size_t>(c)];
            }
            std::string rk = rkb.build(fields);

            if (rk.empty()) {
                result.kv_skipped_count++;
                // Use empty placeholder so row_idx is preserved
                rk = "";
            }

            if (budget) {
                auto reserve_status = budget->reserve(sizeof(SortEntry) + rk.size());
                if (!reserve_status.ok()) return reserve_status;
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

static Status append_grouped_row_cells(
        HFileWriter&                        writer,
        std::vector<GroupedCell>&           cells,
        const std::string&                  row_key,
        const std::string&                  cf,
        int64_t                             default_ts,
        int64_t*                            kv_written,
        int64_t*                            kv_skipped) {
    std::sort(cells.begin(), cells.end(),
              [](const GroupedCell& a, const GroupedCell& b) {
                  return a.qualifier < b.qualifier;
              });

    for (size_t i = 1; i < cells.size(); ++i) {
        if (cells[i - 1].qualifier == cells[i].qualifier)
            return Status::InvalidArg(
                "DUPLICATE_CELL: duplicate qualifier under row key '" + row_key + "'");
    }

    int64_t ts = default_ts > 0 ? default_ts
                : std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::system_clock::now().time_since_epoch()).count();

    std::vector<uint8_t> row_bytes(row_key.begin(), row_key.end());
    std::vector<uint8_t> cf_bytes(cf.begin(), cf.end());

    for (const auto& cell : cells) {
        std::vector<uint8_t> q_bytes(cell.qualifier.begin(), cell.qualifier.end());
        auto before_entry_count = writer.entry_count();
        auto s = writer.append(row_bytes, cf_bytes, q_bytes,
                               ts, cell.value);
        if (!s.ok()) return s;
        auto after_entry_count = writer.entry_count();
        if (after_entry_count > before_entry_count) {
            if (kv_written) ++(*kv_written);
        } else if (kv_skipped) {
            ++(*kv_skipped);
        }
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
    std::unique_ptr<memory::MemoryBudget> budget;
    if (opts.writer_opts.max_memory_bytes > 0)
        budget = std::make_unique<memory::MemoryBudget>(opts.writer_opts.max_memory_bytes);

    auto t_sort_start = std::chrono::steady_clock::now();
    {
        Status s = build_sort_index(opts.arrow_path, rkb, max_col_idx,
                                    sort_index, batches, budget.get(), result);
        if (!s.ok()) {
            result.error_code = s.message().find("MemoryBudget:") == 0
                ? ErrorCode::MEMORY_EXHAUSTED
                : s.message().find("SCHEMA_MISMATCH:") == 0
                    ? ErrorCode::SCHEMA_MISMATCH
                : ErrorCode::ARROW_FILE_ERROR;
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
    wo.sort_mode              = WriterOptions::SortMode::PreSortedVerified;

    auto [writer, ws] = HFileWriter::builder()
        .set_path(opts.hfile_path)
        .set_column_family(wo.column_family)
        .set_compression(wo.compression)
        .set_block_size(wo.block_size)
        .set_data_block_encoding(wo.data_block_encoding)
        .set_bloom_type(wo.bloom_type)
        .set_sort_mode(wo.sort_mode)
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

    for (size_t i = 0; i < sort_index.size();) {
        const std::string& row_key = sort_index[i].row_key;
        std::vector<GroupedCell> cells;

        size_t j = i;
        for (; j < sort_index.size() && sort_index[j].row_key == row_key; ++j) {
            const auto& entry = sort_index[j];
            const auto& batch = batches[static_cast<size_t>(entry.batch_idx)];
            auto schema = batch->schema();
            int num_cols = batch->num_columns();
            for (int c = 0; c < num_cols; ++c) {
                if (batch->column(c)->IsNull(entry.row_idx)) continue;
                auto val = scalar_to_bytes(*batch->column(c), entry.row_idx);
                if (val.empty()) continue;
                cells.push_back({schema->field(c)->name(), std::move(val)});
            }
        }

        int64_t kv_written = 0;
        int64_t kv_skipped = 0;
        auto s = append_grouped_row_cells(*writer, cells, row_key,
                                          opts.column_family,
                                          opts.default_timestamp,
                                          &kv_written,
                                          &kv_skipped);
        if (!s.ok()) {
            result.error_code = s.message().find("SORT_ORDER_VIOLATION") != std::string::npos ||
                                s.message().find("DUPLICATE_CELL") != std::string::npos
                ? ErrorCode::SORT_VIOLATION
                : ErrorCode::IO_ERROR;
            result.error_message = s.message();
            clog::err("Pass 2 failed: " + s.message());
            return result;
        }
        result.kv_written_count += kv_written;
        result.kv_skipped_count += kv_skipped;
        rows_done += static_cast<int64_t>(j - i);

        if (opts.progress_cb && rows_done % progress_step == 0)
            opts.progress_cb(rows_done,
                             static_cast<int64_t>(sort_index.size()));
        i = j;
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
