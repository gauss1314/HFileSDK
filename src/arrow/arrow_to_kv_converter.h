#pragma once

#include <hfile/types.h>
#include <hfile/status.h>
#include <hfile/bulk_load_writer.h>

#include <arrow/api.h>
#include <arrow/array.h>
#include <arrow/record_batch.h>

#include <vector>
#include <string>
#include <functional>
#include <span>

namespace hfile {
namespace arrow_convert {

/// Configuration for Wide Table mapping mode.
struct WideTableConfig {
    std::string  row_key_column  = "__row_key__";
    std::string  column_family   = "cf";     // single CF for wide tables
    int64_t      default_timestamp = 0;      // 0 = use current time
    bool         skip_null_columns = true;   // don't emit KVs for null cells
};

/// Configuration for Tall Table mapping mode.
/// Schema must have columns: row_key, cf, qualifier, timestamp, value
struct TallTableConfig {
    std::string col_row_key   = "row_key";
    std::string col_cf        = "cf";
    std::string col_qualifier = "qualifier";
    std::string col_timestamp = "timestamp";
    std::string col_value     = "value";
};

/// Callback invoked for each produced KeyValue.
using KVCallback = std::function<Status(const KeyValue&)>;

/// Converts an Arrow RecordBatch to HBase KeyValues.
class ArrowToKVConverter {
public:
    /// Wide table: each row → N KVs (one per non-null column).
    static Status convert_wide_table(
        const ::arrow::RecordBatch& batch,
        const WideTableConfig&      config,
        KVCallback                  callback);

    /// Tall table: each row → 1 KV.
    static Status convert_tall_table(
        const ::arrow::RecordBatch& batch,
        const TallTableConfig&      config,
        KVCallback                  callback);

    /// Raw KV: two columns (key bytes + value bytes), pre-encoded.
    static Status convert_raw_kv(
        const ::arrow::RecordBatch& batch,
        const std::string&          key_column,
        const std::string&          value_column,
        KVCallback                  callback);

private:
    /// Serialize an Arrow scalar value to bytes (Big-Endian for numerics).
    static std::vector<uint8_t> serialize_scalar(
        const ::arrow::Array& arr, int64_t row);
};

} // namespace arrow_convert
} // namespace hfile
