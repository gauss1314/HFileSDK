#include "arrow_to_kv_converter.h"

#include <arrow/type.h>
#include <arrow/array/array_base.h>
#include <arrow/array/array_binary.h>
#include <arrow/array/array_primitive.h>
#include <arrow/scalar.h>
#include <arrow/util/checked_cast.h>

#include <hfile/types.h>
#include <chrono>
#include <cstring>
#include <stdexcept>

namespace hfile {
namespace arrow_convert {

// ─── Scalar serializer ────────────────────────────────────────────────────────

std::vector<uint8_t> ArrowToKVConverter::serialize_scalar(
        const ::arrow::Array& arr, int64_t row) {
    using namespace ::arrow;

    if (arr.IsNull(row)) return {};

    switch (arr.type_id()) {
    case Type::BOOL: {
        auto& a = checked_cast<const BooleanArray&>(arr);
        return {static_cast<uint8_t>(a.Value(row) ? 1 : 0)};
    }
    case Type::INT8: {
        auto v = checked_cast<const Int8Array&>(arr).Value(row);
        return {static_cast<uint8_t>(v)};
    }
    case Type::INT16: {
        int16_t v = checked_cast<const Int16Array&>(arr).Value(row);
        uint8_t buf[2]; write_be16(buf, static_cast<uint16_t>(v));
        return {buf, buf + 2};
    }
    case Type::INT32: {
        int32_t v = checked_cast<const Int32Array&>(arr).Value(row);
        uint8_t buf[4]; write_be32(buf, static_cast<uint32_t>(v));
        return {buf, buf + 4};
    }
    case Type::INT64: {
        int64_t v = checked_cast<const Int64Array&>(arr).Value(row);
        uint8_t buf[8]; write_be64(buf, static_cast<uint64_t>(v));
        return {buf, buf + 8};
    }
    case Type::UINT8: {
        return {checked_cast<const UInt8Array&>(arr).Value(row)};
    }
    case Type::UINT16: {
        uint16_t v = checked_cast<const UInt16Array&>(arr).Value(row);
        uint8_t buf[2]; write_be16(buf, v);
        return {buf, buf + 2};
    }
    case Type::UINT32: {
        uint32_t v = checked_cast<const UInt32Array&>(arr).Value(row);
        uint8_t buf[4]; write_be32(buf, v);
        return {buf, buf + 4};
    }
    case Type::UINT64: {
        uint64_t v = checked_cast<const UInt64Array&>(arr).Value(row);
        uint8_t buf[8]; write_be64(buf, v);
        return {buf, buf + 8};
    }
    case Type::FLOAT: {
        float f = checked_cast<const FloatArray&>(arr).Value(row);
        uint32_t bits; std::memcpy(&bits, &f, 4);
        uint8_t buf[4]; write_be32(buf, bits);
        return {buf, buf + 4};
    }
    case Type::DOUBLE: {
        double d = checked_cast<const DoubleArray&>(arr).Value(row);
        uint64_t bits; std::memcpy(&bits, &d, 8);
        uint8_t buf[8]; write_be64(buf, bits);
        return {buf, buf + 8};
    }
    case Type::STRING:
    case Type::LARGE_STRING: {
        // Zero-copy: return span of Arrow buffer
        ::arrow::util::string_view sv;
        if (arr.type_id() == Type::STRING)
            sv = checked_cast<const StringArray&>(arr).GetView(row);
        else
            sv = checked_cast<const LargeStringArray&>(arr).GetView(row);
        const auto* d = reinterpret_cast<const uint8_t*>(sv.data());
        return std::vector<uint8_t>(d, d + sv.size());
    }
    case Type::BINARY:
    case Type::LARGE_BINARY: {
        ::arrow::util::string_view sv;
        if (arr.type_id() == Type::BINARY)
            sv = checked_cast<const BinaryArray&>(arr).GetView(row);
        else
            sv = checked_cast<const LargeBinaryArray&>(arr).GetView(row);
        const auto* d = reinterpret_cast<const uint8_t*>(sv.data());
        return std::vector<uint8_t>(d, d + sv.size());
    }
    case Type::TIMESTAMP: {
        // Convert to milliseconds Big-Endian
        int64_t ts = checked_cast<const Int64Array&>(arr).Value(row);
        const auto& ts_type = checked_cast<const TimestampType&>(*arr.type());
        // Normalise to milliseconds
        switch (ts_type.unit()) {
        case TimeUnit::SECOND: ts *= 1000; break;
        case TimeUnit::MILLI:  break;
        case TimeUnit::MICRO:  ts /= 1000; break;
        case TimeUnit::NANO:   ts /= 1000000; break;
        }
        uint8_t buf[8]; write_be64(buf, static_cast<uint64_t>(ts));
        return {buf, buf + 8};
    }
    default:
        return {};  // unsupported type → empty value
    }
}

// ─── Wide Table ───────────────────────────────────────────────────────────────

Status ArrowToKVConverter::convert_wide_table(
        const ::arrow::RecordBatch& batch,
        const WideTableConfig&      cfg,
        KVCallback                  callback) {

    // Find the row key column
    int rk_idx = batch.schema()->GetFieldIndex(cfg.row_key_column);
    if (rk_idx < 0)
        return Status::InvalidArg("Wide table: missing row key column '" +
                                   cfg.row_key_column + "'");

    int64_t ts_default = cfg.default_timestamp;
    if (ts_default == 0) {
        ts_default = static_cast<int64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count());
    }

    const auto& rk_arr = *batch.column(rk_idx);
    const std::string& family = cfg.column_family;

    for (int64_t row = 0; row < batch.num_rows(); ++row) {
        // Serialize row key
        std::vector<uint8_t> rk = serialize_scalar(rk_arr, row);
        if (rk.empty() && rk_arr.IsNull(row))
            return Status::InvalidArg("Row key column contains null at row " +
                                       std::to_string(row));

        // Emit one KV per non-row-key column
        for (int col = 0; col < batch.num_columns(); ++col) {
            if (col == rk_idx) continue;
            if (cfg.skip_null_columns && batch.column(col)->IsNull(row)) continue;

            std::vector<uint8_t> val = serialize_scalar(*batch.column(col), row);
            const std::string&   qualifier = batch.schema()->field(col)->name();

            KeyValue kv;
            kv.row       = {rk.data(), rk.size()};
            kv.family    = {reinterpret_cast<const uint8_t*>(family.data()), family.size()};
            kv.qualifier = {reinterpret_cast<const uint8_t*>(qualifier.data()), qualifier.size()};
            kv.timestamp = ts_default;
            kv.key_type  = KeyType::Put;
            kv.value     = {val.data(), val.size()};

            HFILE_RETURN_IF_ERROR(callback(kv));
        }
    }
    return Status::OK();
}

// ─── Tall Table ───────────────────────────────────────────────────────────────

Status ArrowToKVConverter::convert_tall_table(
        const ::arrow::RecordBatch& batch,
        const TallTableConfig&      cfg,
        KVCallback                  callback) {

    auto find_col = [&](const std::string& name) -> int {
        int idx = batch.schema()->GetFieldIndex(name);
        return idx;
    };

    int rk_idx  = find_col(cfg.col_row_key);
    int cf_idx  = find_col(cfg.col_cf);
    int q_idx   = find_col(cfg.col_qualifier);
    int ts_idx  = find_col(cfg.col_timestamp);
    int val_idx = find_col(cfg.col_value);

    if (rk_idx < 0)  return Status::InvalidArg("Tall table: missing column '" + cfg.col_row_key + "'");
    if (cf_idx < 0)  return Status::InvalidArg("Tall table: missing column '" + cfg.col_cf + "'");
    if (q_idx  < 0)  return Status::InvalidArg("Tall table: missing column '" + cfg.col_qualifier + "'");
    if (val_idx < 0) return Status::InvalidArg("Tall table: missing column '" + cfg.col_value + "'");

    for (int64_t row = 0; row < batch.num_rows(); ++row) {
        std::vector<uint8_t> rk  = serialize_scalar(*batch.column(rk_idx),  row);
        std::vector<uint8_t> cf  = serialize_scalar(*batch.column(cf_idx),   row);
        std::vector<uint8_t> q   = serialize_scalar(*batch.column(q_idx),    row);
        std::vector<uint8_t> val = serialize_scalar(*batch.column(val_idx),  row);

        int64_t ts = 0;
        if (ts_idx >= 0) {
            std::vector<uint8_t> ts_bytes = serialize_scalar(*batch.column(ts_idx), row);
            if (ts_bytes.size() == 8)
                ts = static_cast<int64_t>(read_be64(ts_bytes.data()));
        }
        if (ts == 0) {
            ts = static_cast<int64_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count());
        }

        KeyValue kv;
        kv.row       = {rk.data(),  rk.size()};
        kv.family    = {cf.data(),  cf.size()};
        kv.qualifier = {q.data(),   q.size()};
        kv.timestamp = ts;
        kv.key_type  = KeyType::Put;
        kv.value     = {val.data(), val.size()};

        HFILE_RETURN_IF_ERROR(callback(kv));
    }
    return Status::OK();
}

// ─── Raw KV ───────────────────────────────────────────────────────────────────

Status ArrowToKVConverter::convert_raw_kv(
        const ::arrow::RecordBatch& batch,
        const std::string&          key_column,
        const std::string&          value_column,
        KVCallback                  callback) {

    int key_idx = batch.schema()->GetFieldIndex(key_column);
    int val_idx = batch.schema()->GetFieldIndex(value_column);

    if (key_idx < 0) return Status::InvalidArg("RawKV: missing column '" + key_column + "'");
    if (val_idx < 0) return Status::InvalidArg("RawKV: missing column '" + value_column + "'");

    for (int64_t row = 0; row < batch.num_rows(); ++row) {
        std::vector<uint8_t> raw_key = serialize_scalar(*batch.column(key_idx), row);
        std::vector<uint8_t> val     = serialize_scalar(*batch.column(val_idx), row);

        // Parse the pre-encoded HBase key
        // Format: rowLen(2) + row + familyLen(1) + family + qualifier + ts(8) + type(1)
        const uint8_t* p = raw_key.data();
        if (raw_key.size() < 3) return Status::Corruption("RawKV: key too short");

        uint16_t row_len = read_be16(p); p += 2;
        if (raw_key.size() < 2 + row_len + 1) return Status::Corruption("RawKV: truncated row");

        std::span<const uint8_t> row_span{p, row_len}; p += row_len;
        uint8_t fam_len = *p++; p += 0;
        std::span<const uint8_t> fam_span{p, fam_len}; p += fam_len;

        size_t remaining = raw_key.size() - (p - raw_key.data());
        if (remaining < 9) return Status::Corruption("RawKV: truncated qualifier+ts+type");

        size_t q_len = remaining - 9;
        std::span<const uint8_t> q_span{p, q_len}; p += q_len;
        int64_t ts    = static_cast<int64_t>(read_be64(p)); p += 8;
        KeyType ktype = static_cast<KeyType>(*p);

        KeyValue kv;
        kv.row       = row_span;
        kv.family    = fam_span;
        kv.qualifier = q_span;
        kv.timestamp = ts;
        kv.key_type  = ktype;
        kv.value     = {val.data(), val.size()};

        HFILE_RETURN_IF_ERROR(callback(kv));
    }
    return Status::OK();
}

} // namespace arrow_convert
} // namespace hfile
