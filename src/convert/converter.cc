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
#include <unordered_set>
#include <vector>
#include <string>
#include <string_view>
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

static int map_status_to_error_code(const Status& s) {
    if (s.message().find("MemoryBudget:") == 0)
        return ErrorCode::MEMORY_EXHAUSTED;
    if (s.message().find("SCHEMA_MISMATCH:") == 0)
        return ErrorCode::SCHEMA_MISMATCH;
    if (s.message().find("INVALID_ROW_KEY_RULE:") == 0)
        return ErrorCode::INVALID_ROW_KEY_RULE;
    if (s.message().find("DISK_SPACE_EXHAUSTED") != std::string::npos)
        return ErrorCode::DISK_EXHAUSTED;
    return s.code() == Status::Code::IoError ? ErrorCode::IO_ERROR
                                             : ErrorCode::ARROW_FILE_ERROR;
}

static int map_pass1_status_to_error_code(const Status& s) {
    if (s.message().find("MemoryBudget:") == 0)
        return ErrorCode::MEMORY_EXHAUSTED;
    if (s.message().find("SCHEMA_MISMATCH:") == 0)
        return ErrorCode::SCHEMA_MISMATCH;
    if (s.message().find("INVALID_ROW_KEY_RULE:") == 0)
        return ErrorCode::INVALID_ROW_KEY_RULE;
    return ErrorCode::ARROW_FILE_ERROR;
}

// ─── Column exclusion helper ──────────────────────────────────────────────────

/// Build the set of column indices that should be excluded from HBase KV output.
/// Row key segments always reference columns by index from the *original* Arrow
/// schema, so exclusions never affect row key construction — only the second pass
/// that writes qualifiers skips these columns.
///
/// Logs one INFO line listing the excluded column names.
// ─── Column exclusion helpers ─────────────────────────────────────────────────

/// Compute the indices (in the ORIGINAL schema) that must be removed,
/// sorted in DESCENDING order so RemoveColumn() calls preserve earlier indices.
static std::vector<int> build_removal_indices(
        const std::shared_ptr<arrow::Schema>&   schema,
        const std::vector<std::string>&         excluded_names,
        const std::vector<std::string>&         excluded_prefixes) {

    if (excluded_names.empty() && excluded_prefixes.empty())
        return {};

    std::unordered_set<std::string> name_set(excluded_names.begin(), excluded_names.end());

    std::vector<int>         indices;
    std::vector<std::string> matched_names;

    for (int c = 0; c < schema->num_fields(); ++c) {
        const std::string& col = schema->field(c)->name();
        bool matched = name_set.count(col) > 0;
        if (!matched) {
            for (const auto& pfx : excluded_prefixes) {
                if (!pfx.empty() && col.size() >= pfx.size() &&
                    col.compare(0, pfx.size(), pfx) == 0) {
                    matched = true;
                    break;
                }
            }
        }
        if (matched) {
            indices.push_back(c);
            matched_names.push_back(col);
        }
    }

    // Must be descending so that each RemoveColumn(idx) doesn't shift remaining indices
    std::sort(indices.begin(), indices.end(), std::greater<int>());

    if (!indices.empty()) {
        std::string names_str;
        for (size_t i = 0; i < matched_names.size(); ++i) {
            if (i) names_str += ", ";
            names_str += matched_names[i];
        }
        clog::info("Column exclusion: dropping " + std::to_string(indices.size()) +
                   " column(s) before rowKeyRule index mapping: [" + names_str + "]");
    }
    return indices;
}

/// Apply RemoveColumn for each index in `removal_indices` (must be sorted descending).
/// Returns a new RecordBatch with those columns physically removed.
/// rowKeyRule indices in the caller then reference the FILTERED schema directly.
static arrow::Result<std::shared_ptr<arrow::RecordBatch>> apply_column_removal(
        std::shared_ptr<arrow::RecordBatch>  batch,
        const std::vector<int>&              removal_indices) {
    if (removal_indices.empty()) return batch;
    auto result = std::move(batch);
    for (int idx : removal_indices) {             // descending — safe to remove one by one
        if (idx < result->num_columns()) {
            ARROW_ASSIGN_OR_RAISE(result, result->RemoveColumn(idx));
        }
    }
    return result;
}

static arrow::Result<std::shared_ptr<arrow::Schema>> apply_schema_removal(
        std::shared_ptr<arrow::Schema> schema,
        const std::vector<int>&        removal_indices) {
    if (removal_indices.empty()) return schema;
    auto result = std::move(schema);
    for (int idx : removal_indices) {
        if (idx < result->num_fields()) {
            ARROW_ASSIGN_OR_RAISE(result, result->RemoveField(idx));
        }
    }
    return result;
}

// ─── SortEntry ────────────────────────────────────────────────────────────────

struct SortEntry {
    std::string row_key;     // fully built row key string
    int32_t     batch_idx;   // which RecordBatch (0-based)
    int32_t     row_idx;     // row within that batch
};

struct GroupedCell {
    std::string_view     qualifier;
    std::vector<uint8_t> value;
};

static std::span<const uint8_t> as_bytes(const std::string& s) {
    return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
}

static std::span<const uint8_t> as_bytes(std::string_view s) {
    return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
}

static Status scalar_to_bytes(const arrow::Array& arr,
                              int64_t row,
                              std::vector<uint8_t>* out);

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
        out->assign(sv.data(), sv.size());
        return Status::OK();
    }
    case T::LARGE_STRING: {
        auto& sa = static_cast<const arrow::LargeStringArray&>(arr);
        auto sv = sa.GetView(row);
        out->assign(sv.data(), sv.size());
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

static Status scalar_to_bytes(const arrow::Array& arr, int64_t row, std::vector<uint8_t>* out) {
    out->clear();
    if (arr.IsNull(row)) return Status::OK();

    using T = arrow::Type;
    auto t = arr.type_id();

    switch (t) {
    case T::STRING: {
        auto& sa = static_cast<const arrow::StringArray&>(arr);
        auto sv = sa.GetView(row);
        out->assign(sv.begin(), sv.end());
        return Status::OK();
    }
    case T::LARGE_STRING: {
        auto& sa = static_cast<const arrow::LargeStringArray&>(arr);
        auto sv = sa.GetView(row);
        out->assign(sv.begin(), sv.end());
        return Status::OK();
    }
    case T::BINARY: {
        auto& ba = static_cast<const arrow::BinaryArray&>(arr);
        auto sv = ba.GetView(row);
        out->assign(sv.begin(), sv.end());
        return Status::OK();
    }
    case T::LARGE_BINARY: {
        auto& ba = static_cast<const arrow::LargeBinaryArray&>(arr);
        auto sv = ba.GetView(row);
        out->assign(sv.begin(), sv.end());
        return Status::OK();
    }
    case T::BOOL:
        out->assign({static_cast<uint8_t>(static_cast<const arrow::BooleanArray&>(arr).Value(row) ? 1 : 0)});
        return Status::OK();
    case T::INT8:
        out->assign({static_cast<uint8_t>(static_cast<const arrow::Int8Array&>(arr).Value(row))});
        return Status::OK();
    case T::INT16: {
        uint16_t v = static_cast<uint16_t>(static_cast<const arrow::Int16Array&>(arr).Value(row));
        out->resize(2);
        (*out)[0] = static_cast<uint8_t>(v >> 8);
        (*out)[1] = static_cast<uint8_t>(v);
        return Status::OK();
    }
    case T::INT32: {
        uint32_t v = static_cast<uint32_t>(static_cast<const arrow::Int32Array&>(arr).Value(row));
        out->resize(4);
        (*out)[0] = static_cast<uint8_t>(v >> 24);
        (*out)[1] = static_cast<uint8_t>(v >> 16);
        (*out)[2] = static_cast<uint8_t>(v >> 8);
        (*out)[3] = static_cast<uint8_t>(v);
        return Status::OK();
    }
    case T::INT64: {
        uint64_t v = static_cast<uint64_t>(static_cast<const arrow::Int64Array&>(arr).Value(row));
        out->resize(8);
        for (int i = 7; i >= 0; --i) { (*out)[i] = v & 0xFF; v >>= 8; }
        return Status::OK();
    }
    case T::UINT8:
        out->assign({static_cast<const arrow::UInt8Array&>(arr).Value(row)});
        return Status::OK();
    case T::UINT16: {
        uint16_t v = static_cast<const arrow::UInt16Array&>(arr).Value(row);
        out->resize(2);
        (*out)[0] = static_cast<uint8_t>(v >> 8);
        (*out)[1] = static_cast<uint8_t>(v);
        return Status::OK();
    }
    case T::UINT32: {
        uint32_t v = static_cast<const arrow::UInt32Array&>(arr).Value(row);
        out->resize(4);
        (*out)[0] = static_cast<uint8_t>(v >> 24);
        (*out)[1] = static_cast<uint8_t>(v >> 16);
        (*out)[2] = static_cast<uint8_t>(v >> 8);
        (*out)[3] = static_cast<uint8_t>(v);
        return Status::OK();
    }
    case T::UINT64: {
        uint64_t v = static_cast<const arrow::UInt64Array&>(arr).Value(row);
        out->resize(8);
        for (int i = 7; i >= 0; --i) { (*out)[i] = v & 0xFF; v >>= 8; }
        return Status::OK();
    }
    case T::FLOAT: {
        float v = static_cast<const arrow::FloatArray&>(arr).Value(row);
        uint32_t bits; std::memcpy(&bits, &v, 4);
        out->resize(4);
        (*out)[0] = static_cast<uint8_t>(bits >> 24);
        (*out)[1] = static_cast<uint8_t>(bits >> 16);
        (*out)[2] = static_cast<uint8_t>(bits >> 8);
        (*out)[3] = static_cast<uint8_t>(bits);
        return Status::OK();
    }
    case T::DOUBLE: {
        double v = static_cast<const arrow::DoubleArray&>(arr).Value(row);
        uint64_t bits; std::memcpy(&bits, &v, 8);
        out->resize(8);
        for (int i = 7; i >= 0; --i) { (*out)[i] = bits & 0xFF; bits >>= 8; }
        return Status::OK();
    }
    case T::TIMESTAMP: {
        auto& ta = static_cast<const arrow::TimestampArray&>(arr);
        auto unit = static_cast<const arrow::TimestampType&>(*arr.type()).unit();
        int64_t v = normalize_timestamp_to_millis(ta.Value(row), unit);
        uint64_t bits = static_cast<uint64_t>(v);
        out->resize(8);
        for (int i = 7; i >= 0; --i) { (*out)[i] = bits & 0xFF; bits >>= 8; }
        return Status::OK();
    }
    default: {
        auto s = arr.GetScalar(row);
        if (s.ok()) {
            auto str = s.ValueOrDie()->ToString();
            out->assign(str.begin(), str.end());
            return Status::OK();
        }
        return Status::InvalidArg("cannot materialize Arrow scalar: " + s.status().ToString());
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
        const std::vector<int>&             removal_indices,   // sorted desc, applied per-batch
        std::vector<SortEntry>&             index_out,
        std::vector<std::shared_ptr<arrow::RecordBatch>>& batches_out,
        memory::MemoryBudget*               budget,
        ConvertResult&                      result) {

    auto reader_res = open_arrow_stream(arrow_path);
    if (!reader_res.ok())
        return Status::IoError("Cannot open Arrow file: " + reader_res.status().ToString());

    auto reader = reader_res.ValueOrDie();

    // Compute the FILTERED schema directly from the original schema.
    // rowKeyRule indices are validated against this filtered schema.
    std::shared_ptr<arrow::Schema> filtered_schema;
    {
        auto filtered_res = apply_schema_removal(reader->schema(), removal_indices);
        if (!filtered_res.ok())
            return Status::IoError("Failed to compute filtered schema: " +
                                   filtered_res.status().ToString());
        filtered_schema = std::move(*filtered_res);
    }
    int num_filtered_cols = filtered_schema->num_fields();

    // Validate that rowKeyRule indices refer to columns that exist after filtering
    if (max_col_idx >= num_filtered_cols) {
        return Status::InvalidArg(
            "SCHEMA_MISMATCH: rowKeyRule references column index " +
            std::to_string(max_col_idx) + " but filtered Arrow schema has only " +
            std::to_string(num_filtered_cols) + " columns. " +
            "Note: column indices are based on the schema AFTER excluded columns are removed.");
    }

    std::vector<uint8_t> referenced_cols(static_cast<size_t>(std::max(0, num_filtered_cols)), 0);
    for (const auto& seg : rkb.segments()) {
        if (seg.type != arrow_convert::RowKeySegment::Type::ColumnRef &&
            seg.type != arrow_convert::RowKeySegment::Type::EncodedColumn)
            continue;
        if (seg.col_index >= num_filtered_cols) {
            return Status::InvalidArg(
                "SCHEMA_MISMATCH: rowKeyRule references column index " +
                std::to_string(seg.col_index) + " but filtered Arrow schema has only " +
                std::to_string(num_filtered_cols) + " columns.");
        }
        referenced_cols[static_cast<size_t>(seg.col_index)] = true;
    }
    std::vector<int> referenced_col_indices;
    referenced_col_indices.reserve(rkb.segments().size());
    for (int c = 0; c < num_filtered_cols; ++c) {
        if (referenced_cols[static_cast<size_t>(c)] == 0) continue;
        referenced_col_indices.push_back(c);
        if (!is_supported_rowkey_type(filtered_schema->field(c)->type())) {
            return Status::InvalidArg(
                "SCHEMA_MISMATCH: unsupported Arrow type for row key field '" +
                filtered_schema->field(c)->name() + "': " +
                filtered_schema->field(c)->type()->ToString());
        }
    }

    std::shared_ptr<arrow::RecordBatch> raw_batch;
    int32_t batch_idx = 0;

    std::vector<std::string> owned_fields(
        max_col_idx >= 0 ? static_cast<size_t>(max_col_idx + 1) : 0);
    std::vector<std::string_view> fields(owned_fields.size());

    while (true) {
        auto s = reader->ReadNext(&raw_batch);
        if (!s.ok()) return Status::IoError("Arrow read error: " + s.ToString());
        if (!raw_batch) break;  // EOS

        // ── Strip excluded columns from the batch ──────────────────────────
        // After this call the batch's schema == filtered_schema.
        // All downstream code (row key building + second pass KV output) uses
        // the filtered schema, so rowKeyRule indices need no adjustment.
        std::shared_ptr<arrow::RecordBatch> batch;
        if (!removal_indices.empty()) {
            auto stripped = apply_column_removal(raw_batch, removal_indices);
            if (!stripped.ok())
                return Status::IoError("Column removal failed: " + stripped.status().ToString());
            batch = std::move(*stripped);
        } else {
            batch = std::move(raw_batch);
        }

        int64_t n_rows = batch->num_rows();
        if (budget) {
            auto reserve_status = budget->reserve(
                static_cast<size_t>(std::max<int64_t>(0, arrow::util::TotalBufferSize(*batch))));
            if (!reserve_status.ok()) return reserve_status;
        }
        batches_out.push_back(batch);    // store the FILTERED batch

        for (int64_t r = 0; r < n_rows; ++r) {
            std::fill(fields.begin(), fields.end(), std::string_view{});
            for (int c : referenced_col_indices) {
                auto field_status = scalar_to_string(*batch->column(c), r,
                                                     &owned_fields[static_cast<size_t>(c)]);
                if (!field_status.ok()) return field_status;
                fields[static_cast<size_t>(c)] = owned_fields[static_cast<size_t>(c)];
            }
            std::string rk;
            auto build_status = rkb.build_checked(fields, &rk);
            if (!build_status.ok())
                return Status::InvalidArg("INVALID_ROW_KEY_RULE: " + build_status.message());

            if (rk.empty()) {
                result.kv_skipped_count++;
                rk = "";
            }

            if (budget) {
                auto reserve_status = budget->reserve(sizeof(SortEntry) + rk.size());
                if (!reserve_status.ok()) return reserve_status;
            }

            index_out.push_back({std::move(rk), batch_idx, static_cast<int32_t>(r)});
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
        size_t                              source_row_count,   // how many Arrow rows mapped here
        int64_t*                            kv_written,
        int64_t*                            kv_skipped) {
    std::sort(cells.begin(), cells.end(),
              [](const GroupedCell& a, const GroupedCell& b) {
                  return a.qualifier < b.qualifier;
              });

    // Deduplicate: when the same qualifier appears multiple times under the same
    // row key (i.e. multiple Arrow source rows mapped to this HBase row key),
    // keep only the first occurrence (first-in-sort-order wins).
    //
    // NOTE: we do NOT log per-qualifier here.  The caller logs ONE group-level
    // warning via clog::warn before calling this function, so the user sees
    // exactly one line per row-key collision — not one line per column.
    {
        size_t write_pos = 0;
        for (size_t read_pos = 0; read_pos < cells.size(); ++read_pos) {
            if (write_pos > 0 &&
                cells[write_pos - 1].qualifier == cells[read_pos].qualifier) {
                // Silently count: the group-level warning was already emitted.
                if (kv_skipped) ++(*kv_skipped);
                continue;
            }
            if (read_pos != write_pos)
                cells[write_pos] = std::move(cells[read_pos]);
            ++write_pos;
        }
        cells.resize(write_pos);
    }
    (void)source_row_count;  // available for future per-qualifier logging if needed

    int64_t ts = default_ts > 0 ? default_ts
                : std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::system_clock::now().time_since_epoch()).count();

    auto row_bytes = as_bytes(row_key);
    auto cf_bytes = as_bytes(cf);

    for (const auto& cell : cells) {
        auto before_entry_count = writer.entry_count();
        auto s = writer.append(row_bytes, cf_bytes, as_bytes(cell.qualifier),
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

    // ── 3. Build column removal list ──────────────────────────────────────
    // Compute which column indices (in the ORIGINAL Arrow schema) to physically
    // remove from each RecordBatch before any further processing.
    //
    // After removal the stored batches use the FILTERED schema.  rowKeyRule
    // indices then refer to positions in this filtered schema, so the caller
    // never needs to account for excluded columns in their index numbers.
    //
    // The extra schema-peek open is cheap: Arrow IPC Stream readers read the
    // schema message first; we never read actual row data here.
    std::vector<int> removal_indices;  // sorted descending
    if (!opts.excluded_columns.empty() || !opts.excluded_column_prefixes.empty()) {
        auto schema_reader_res = open_arrow_stream(opts.arrow_path);
        if (!schema_reader_res.ok()) {
            result.error_code = ErrorCode::ARROW_FILE_ERROR;
            result.error_message = "Cannot open Arrow file for schema read: " +
                                   schema_reader_res.status().ToString();
            return result;
        }
        removal_indices = build_removal_indices(
            schema_reader_res.ValueOrDie()->schema(),
            opts.excluded_columns,
            opts.excluded_column_prefixes);
    }

    // ── 4. First pass: build sort index (load all batches into memory) ────
    clog::info("Pass 1: building sort index...");
    std::vector<SortEntry> sort_index;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    std::unique_ptr<memory::MemoryBudget> budget;
    if (opts.writer_opts.max_memory_bytes > 0)
        budget = std::make_unique<memory::MemoryBudget>(opts.writer_opts.max_memory_bytes);

    auto t_sort_start = std::chrono::steady_clock::now();
    {
        Status s = build_sort_index(opts.arrow_path, rkb, max_col_idx,
                                    removal_indices,
                                    sort_index, batches, budget.get(), result);
        if (!s.ok()) {
            result.error_code = map_pass1_status_to_error_code(s);
            result.error_message = s.message();
            clog::err("Pass 1 failed: " + s.message());
            return result;
        }
    }

    // Filter out empty row keys (rows that failed key generation).
    // If a memory budget is active, release the quota reserved for each
    // discarded SortEntry — they were reserved but will never be used.
    {
        size_t before = sort_index.size();
        sort_index.erase(
            std::remove_if(sort_index.begin(), sort_index.end(),
                           [](const SortEntry& e){ return e.row_key.empty(); }),
            sort_index.end());
        size_t erased = before - sort_index.size();
        if (budget && erased > 0) {
            // Each empty-key entry reserved exactly sizeof(SortEntry) + 0 bytes
            // (rk.size() == 0 at the time of reservation).
            budget->release(erased * sizeof(SortEntry));
        }
    }

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
    std::vector<GroupedCell> cells;

    for (size_t i = 0; i < sort_index.size();) {
        const std::string& row_key = sort_index[i].row_key;
        cells.clear();

        size_t j = i;
        for (; j < sort_index.size() && sort_index[j].row_key == row_key; ++j) {
            const auto& entry = sort_index[j];
            const auto& batch = batches[static_cast<size_t>(entry.batch_idx)];
            auto schema = batch->schema();
            int num_cols = batch->num_columns();
            if (cells.capacity() < static_cast<size_t>(num_cols))
                cells.reserve(static_cast<size_t>(num_cols));
            for (int c = 0; c < num_cols; ++c) {
                if (batch->column(c)->IsNull(entry.row_idx)) continue;
                std::vector<uint8_t> val;
                auto value_status = scalar_to_bytes(*batch->column(c), entry.row_idx, &val);
                if (!value_status.ok()) {
                    clog::warn("Cell skipped (" + row_key + "/" + schema->field(c)->name() +
                               "): " + value_status.message());
                    ++result.kv_skipped_count;
                    continue;
                }
                if (val.empty()) continue;
                cells.push_back({schema->field(c)->name(), std::move(val)});
            }
        }

        int64_t kv_written = 0;
        int64_t kv_skipped = 0;
        size_t  source_rows = j - i;   // Arrow rows that mapped to this HBase row key

        // Log ONE group-level warning when multiple source rows collapsed into one
        // HBase row.  This replaces the old per-qualifier warn that fired N times
        // (once per column) for every collision, flooding the log.
        if (source_rows > 1) {
            // Truncate key for readability — raw bytes may not be printable.
            std::string display_key = row_key.size() > 64
                ? row_key.substr(0, 64) + "...(+" + std::to_string(row_key.size() - 64) + "B)"
                : row_key;
            clog::warn("DUPLICATE_KEY: row key '" + display_key +
                       "' was produced by " + std::to_string(source_rows) +
                       " source rows — keeping first, discarding " +
                       std::to_string(source_rows - 1) +
                       " duplicate(s). Check your rowKeyRule for uniqueness.");
            ++result.duplicate_key_count;
        }

        auto s = append_grouped_row_cells(*writer, cells, row_key,
                                          opts.column_family,
                                          opts.default_timestamp,
                                          source_rows,
                                          &kv_written,
                                          &kv_skipped);
        if (!s.ok()) {
            // Only SORT_ORDER_VIOLATION is fatal — it indicates a logic bug
            // (the sort index or HFileWriter is broken).  Other errors (I/O,
            // value-too-large from the HFileWriter's ErrorPolicy) are treated
            // as per the configured error_policy.
            if (s.message().find("SORT_ORDER_VIOLATION") != std::string::npos) {
                result.error_code = ErrorCode::SORT_VIOLATION;
                result.error_message = s.message();
                clog::err("Pass 2 aborted: " + s.message());
                return result;
            }
            // Non-fatal: log, count as skipped, continue with next row group
            clog::warn("Row group skipped (" + row_key + "): " + s.message());
            result.kv_skipped_count += static_cast<int64_t>(j - i);
            i = j;
            continue;
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
        result.error_code = map_status_to_error_code(fs);
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
