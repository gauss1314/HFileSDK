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
#include <array>
#include <deque>
#include <unordered_set>
#include <vector>
#include <string>
#include <string_view>
#include <cstring>
#include <charconv>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <cstdio>
#include <limits>
#include <new>

namespace hfile
{

// --- Logging (no external dep) ----
namespace clog
{
static void info(const std::string& m)
{
    fprintf(stderr, "[INFO]  convert: %s\n", m.c_str());
}
static void warn(const std::string& m)
{
    fprintf(stderr, "[WARN]  convert: %s\n", m.c_str());
}
static void err(const std::string& m)
{
    fprintf(stderr, "[ERROR] convert: %s\n", m.c_str());
}
} // namespace clog

static bool hotpath_profiling_enabled()
{
    static const bool enabled = []()
    {
        const char* env = std::getenv("HFILESDK_ENABLE_HOTPATH_PROFILING");
        if (env == nullptr || env[0] == '\0')
        {
            return false;
        }
        return std::strcmp(env, "0") != 0 && std::strcmp(env, "false") != 0 && std::strcmp(env, "FALSE") != 0;
    }();
    return enabled;
}

static int map_status_to_error_code(const Status& s)
{
    if (s.message().find("DISK_SPACE_EXHAUSTED") != std::string::npos)
    {
        return ErrorCode::DISK_EXHAUSTED;
    }
    if (s.message().find("MemoryBudget:") == 0)
    {
        return ErrorCode::MEMORY_EXHAUSTED;
    }
    switch (s.code())
    {
        case Status::Code::IoError:
            return ErrorCode::IO_ERROR;
        case Status::Code::InvalidArg:
        case Status::Code::OutOfRange:
            return ErrorCode::INVALID_ARGUMENT;
        case Status::Code::Ok:
            return ErrorCode::OK;
        default:
            return ErrorCode::INTERNAL_ERROR;
    }
}

static int map_pass1_status_to_error_code(const Status& s)
{
    if (s.message().find("INVALID_ARGUMENT:") == 0)
    {
        return ErrorCode::INVALID_ARGUMENT;
    }
    if (s.message().find("MemoryBudget:") == 0)
    {
        return ErrorCode::MEMORY_EXHAUSTED;
    }
    if (s.message().find("SCHEMA_MISMATCH:") == 0)
    {
        return ErrorCode::SCHEMA_MISMATCH;
    }
    if (s.message().find("INVALID_ROW_KEY_RULE:") == 0)
    {
        return ErrorCode::INVALID_ROW_KEY_RULE;
    }
    switch (s.code())
    {
        case Status::Code::IoError:
            return ErrorCode::ARROW_FILE_ERROR;
        case Status::Code::InvalidArg:
        case Status::Code::OutOfRange:
            return ErrorCode::INVALID_ARGUMENT;
        case Status::Code::Ok:
            return ErrorCode::OK;
        default:
            return ErrorCode::INTERNAL_ERROR;
    }
}

static int64_t saturating_metric(size_t value) noexcept
{
    constexpr auto kMax = static_cast<size_t>(std::numeric_limits<int64_t>::max());
    return static_cast<int64_t>(std::min(value, kMax));
}

// --- Column exclusion helper
// ----

/// Build the set of column indices that should be excluded from HBase KV
/// output. Row key segments always reference columns by index from the
/// *original* Arrow schema, so exclusions never affect row key construction —
/// only the second pass that builds the pipe-delimited cell value skips these
/// columns.
///
/// Logs one INFO line listing the excluded column names.
// --- Column exclusion helpers
// ----

/// Compute the indices (in the ORIGINAL schema) that must be removed,
/// sorted in DESCENDING order so RemoveColumn() calls preserve earlier indices.
static std::vector<int> build_removal_indices(const std::shared_ptr<arrow::Schema>& schema,
                                              const std::vector<std::string>& excluded_names,
                                              const std::vector<std::string>& excluded_prefixes)
{
    std::unordered_set<std::string> name_set(excluded_names.begin(), excluded_names.end());

    std::vector<int> indices;
    std::vector<std::string> matched_names;

    for (int c = 0; c < schema->num_fields(); ++c)
    {
        const std::string& col = schema->field(c)->name();
        bool matched = name_set.count(col) > 0;
        if (!matched)
        {
            for (const auto& pfx : excluded_prefixes)
            {
                if (!pfx.empty() && col.size() >= pfx.size() && col.compare(0, pfx.size(), pfx) == 0)
                {
                    matched = true;
                    break;
                }
            }
        }
        if (matched)
        {
            indices.push_back(c);
            matched_names.push_back(col);
        }
    }

    // Must be descending so that each RemoveColumn(idx) doesn't shift remaining
    // indices
    std::sort(indices.begin(), indices.end(), std::greater<int>());

    if (!indices.empty())
    {
        std::string names_str;
        for (size_t i = 0; i < matched_names.size(); ++i)
        {
            if (i)
            {
                names_str += ", ";
            }
            names_str += matched_names[i];
        }
        clog::info("Column exclusion: dropping " + std::to_string(indices.size())
                   + " column(s) before rowKeyRule index mapping: [" + names_str + "]");
    }
    return indices;
}

/// Apply RemoveColumn for each index in `removal_indices` (must be sorted
/// descending). Returns a new RecordBatch with those columns physically
/// removed. rowKeyRule indices in the caller then reference the FILTERED schema
/// directly.
static arrow::Result<std::shared_ptr<arrow::RecordBatch>>
apply_column_removal(std::shared_ptr<arrow::RecordBatch> batch, const std::vector<int>& removal_indices)
{
    if (removal_indices.empty())
    {
        return batch;
    }
    auto result = std::move(batch);
    for (int idx : removal_indices)
    { // descending — safe to remove one by one
        if (idx < result->num_columns())
        {
            ARROW_ASSIGN_OR_RAISE(result, result->RemoveColumn(idx));
        }
    }
    return result;
}

static arrow::Result<std::shared_ptr<arrow::Schema>> apply_schema_removal(std::shared_ptr<arrow::Schema> schema,
                                                                          const std::vector<int>& removal_indices)
{
    if (removal_indices.empty())
    {
        return schema;
    }
    auto result = std::move(schema);
    for (int idx : removal_indices)
    {
        if (idx < result->num_fields())
        {
            ARROW_ASSIGN_OR_RAISE(result, result->RemoveField(idx));
        }
    }
    return result;
}

// --- SortEntry
// ----

struct StringSortEntry
{
    std::string_view row_key; // view into Arrow string data or owned_row_keys
    int32_t batch_idx;
    int32_t row_idx;
};

struct NumericSortEntry
{
    uint64_t numeric_sort_key;
    int32_t batch_idx;
    int32_t row_idx;
};

static_assert(sizeof(StringSortEntry) == 24);
static_assert(sizeof(NumericSortEntry) == 16);

template <typename Entry>
static Status reserve_sort_entries(std::vector<Entry>* entries, size_t additional, memory::MemoryBudget* budget)
{
    if (additional == 0)
    {
        return Status::OK();
    }
    if (additional > entries->max_size() - entries->size())
    {
        return Status::InvalidArg("Sort entry count exceeds addressable memory");
    }

    const size_t required = entries->size() + additional;
    if (required <= entries->capacity())
    {
        return Status::OK();
    }
    const size_t doubled_capacity
        = entries->capacity() > entries->max_size() / 2 ? entries->max_size() : entries->capacity() * 2;
    size_t target = entries->capacity() == 0 ? std::max<size_t>(1024, required) : std::max(required, doubled_capacity);
    size_t reserved_bytes = 0;
    if (budget)
    {
        reserved_bytes = (target - entries->capacity()) * sizeof(Entry);
        auto reserve_status = budget->reserve(reserved_bytes);
        if (!reserve_status.ok() && target != required)
        {
            target = required;
            reserved_bytes = (target - entries->capacity()) * sizeof(Entry);
            reserve_status = budget->reserve(reserved_bytes);
        }
        if (!reserve_status.ok())
        {
            return reserve_status;
        }
    }

    try
    {
        entries->reserve(target);
    }
    catch (const std::bad_alloc&)
    {
        if (budget && reserved_bytes > 0)
        {
            budget->release(reserved_bytes);
        }
        return Status::Internal("Sort entry reserve failed");
    }
    return Status::OK();
}

static void radix_sort_numeric_entries(std::vector<NumericSortEntry>* entries)
{
    if (entries->size() < 2)
    {
        return;
    }

    static constexpr unsigned kRadixBits = 11;
    static constexpr size_t kRadixSize = 1u << kRadixBits;
    static constexpr uint64_t kRadixMask = kRadixSize - 1;
    static constexpr unsigned kPasses = 6;
    std::vector<NumericSortEntry> scratch(entries->size());
    for (unsigned pass = 0; pass < kPasses; ++pass)
    {
        auto& source = (pass & 1u) == 0 ? *entries : scratch;
        auto& destination = (pass & 1u) == 0 ? scratch : *entries;
        std::array<size_t, kRadixSize> offsets{};
        const unsigned shift = pass * kRadixBits;
        for (const auto& entry : source)
        {
            ++offsets[static_cast<size_t>((entry.numeric_sort_key >> shift) & kRadixMask)];
        }
        size_t next = 0;
        for (auto& offset : offsets)
        {
            const size_t count = offset;
            offset = next;
            next += count;
        }
        for (const auto& entry : source)
        {
            destination[offsets[static_cast<size_t>((entry.numeric_sort_key >> shift) & kRadixMask)]++] = entry;
        }
    }
}

struct BatchColumnPlan;
using ColumnAppendFn = Status (*)(const BatchColumnPlan&, int64_t, std::string*);

/// Per-batch serializer compiled once from the schema. The hot row loop calls
/// the cached typed appender directly instead of repeatedly loading Arrow's
/// shared DataType and switching on type_id().
struct BatchColumnPlan
{
    std::string_view name;
    const arrow::Array* column;
    ColumnAppendFn append;
};

struct Pass2Profile
{
    std::chrono::nanoseconds materialize_ns{0};
    std::chrono::nanoseconds append_ns{0};
    uint64_t materialized_cells{0};
    uint64_t appended_cells{0};
};

static std::span<const uint8_t> as_bytes(const std::string& s)
{
    return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
}

static std::span<const uint8_t> as_bytes(std::string_view s)
{
    return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
}

static constexpr std::array<uint8_t, 1> kEmptyQualifierBytes{0};

static bool try_extract_nonnegative_numeric_sort_key(const arrow::Array& arr, int64_t row, uint64_t* out);

// --- Arrow scalar → UTF-8 string (for rowValue building)
// ----

static int64_t normalize_timestamp_to_millis(int64_t value, arrow::TimeUnit::type unit)
{
    switch (unit)
    {
        case arrow::TimeUnit::SECOND:
            return value * 1000;
        case arrow::TimeUnit::MILLI:
            return value;
        case arrow::TimeUnit::MICRO:
            return value / 1000;
        case arrow::TimeUnit::NANO:
            return value / 1000000;
    }
    return value;
}

static bool is_supported_rowkey_type(const std::shared_ptr<arrow::DataType>& type)
{
    using T = arrow::Type;
    switch (type->id())
    {
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

static bool is_simple_numeric_rowkey_fast_path_type(arrow::Type::type type_id) noexcept
{
    using T = arrow::Type;
    switch (type_id)
    {
        case T::BOOL:
        case T::INT8:
        case T::INT16:
        case T::INT32:
        case T::INT64:
        case T::UINT8:
        case T::UINT16:
        case T::UINT32:
        case T::UINT64:
        case T::TIMESTAMP:
            return true;
        default:
            return false;
    }
}

static uint64_t max_zero_left_padded_value(int pad_len) noexcept
{
    if (pad_len <= 0)
    {
        return 0;
    }
    if (pad_len >= 20)
    {
        return UINT64_MAX;
    }
    uint64_t max_value = 9;
    for (int digits = 1; digits < pad_len; ++digits)
    {
        max_value = max_value * 10 + 9;
    }
    return max_value;
}

template <typename IntT> static Status append_zero_left_padded_decimal(IntT value, int pad_len, std::string* out)
{
    char buf[32];
    auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), value);
    if (ec != std::errc{})
    {
        return Status::InvalidArg("cannot format integer row key segment");
    }
    const size_t len = static_cast<size_t>(ptr - buf);
    out->clear();
    if (pad_len > static_cast<int>(len))
    {
        out->append(static_cast<size_t>(pad_len) - len, '0');
    }
    out->append(buf, len);
    return Status::OK();
}

static Status build_simple_rowkey_fast(const arrow::Array& arr, int64_t row, int pad_len, std::string* out)
{
    out->clear();
    if (arr.IsNull(row))
    {
        return Status::OK();
    }

    using T = arrow::Type;
    switch (arr.type_id())
    {
        case T::BOOL:
            {
                const char bit = static_cast<const arrow::BooleanArray&>(arr).Value(row) ? '1' : '0';
                if (pad_len > 1)
                {
                    out->append(static_cast<size_t>(pad_len - 1), '0');
                }
                out->push_back(bit);
                return Status::OK();
            }
        case T::INT8:
            return append_zero_left_padded_decimal(static_cast<const arrow::Int8Array&>(arr).Value(row), pad_len, out);
        case T::INT16:
            return append_zero_left_padded_decimal(static_cast<const arrow::Int16Array&>(arr).Value(row), pad_len, out);
        case T::INT32:
            return append_zero_left_padded_decimal(static_cast<const arrow::Int32Array&>(arr).Value(row), pad_len, out);
        case T::INT64:
            return append_zero_left_padded_decimal(static_cast<const arrow::Int64Array&>(arr).Value(row), pad_len, out);
        case T::UINT8:
            return append_zero_left_padded_decimal(static_cast<const arrow::UInt8Array&>(arr).Value(row), pad_len, out);
        case T::UINT16:
            return append_zero_left_padded_decimal(
                static_cast<const arrow::UInt16Array&>(arr).Value(row), pad_len, out);
        case T::UINT32:
            return append_zero_left_padded_decimal(
                static_cast<const arrow::UInt32Array&>(arr).Value(row), pad_len, out);
        case T::UINT64:
            return append_zero_left_padded_decimal(
                static_cast<const arrow::UInt64Array&>(arr).Value(row), pad_len, out);
        case T::TIMESTAMP:
            {
                auto& ta = static_cast<const arrow::TimestampArray&>(arr);
                auto unit = static_cast<const arrow::TimestampType&>(*arr.type()).unit();
                return append_zero_left_padded_decimal(
                    normalize_timestamp_to_millis(ta.Value(row), unit), pad_len, out);
            }
        default:
            return Status::InvalidArg("unsupported simple fast-path row key type");
    }
}

static bool try_extract_nonnegative_numeric_sort_key(const arrow::Array& arr, int64_t row, uint64_t* out)
{
    using T = arrow::Type;
    switch (arr.type_id())
    {
        case T::BOOL:
            *out = static_cast<const arrow::BooleanArray&>(arr).Value(row) ? 1u : 0u;
            return true;
        case T::INT8:
            {
                int8_t value = static_cast<const arrow::Int8Array&>(arr).Value(row);
                if (value < 0)
                {
                    return false;
                }
                *out = static_cast<uint64_t>(static_cast<uint8_t>(value));
                return true;
            }
        case T::INT16:
            {
                int16_t value = static_cast<const arrow::Int16Array&>(arr).Value(row);
                if (value < 0)
                {
                    return false;
                }
                *out = static_cast<uint64_t>(value);
                return true;
            }
        case T::INT32:
            {
                int32_t value = static_cast<const arrow::Int32Array&>(arr).Value(row);
                if (value < 0)
                {
                    return false;
                }
                *out = static_cast<uint64_t>(value);
                return true;
            }
        case T::INT64:
            {
                int64_t value = static_cast<const arrow::Int64Array&>(arr).Value(row);
                if (value < 0)
                {
                    return false;
                }
                *out = static_cast<uint64_t>(value);
                return true;
            }
        case T::UINT8:
            *out = static_cast<const arrow::UInt8Array&>(arr).Value(row);
            return true;
        case T::UINT16:
            *out = static_cast<const arrow::UInt16Array&>(arr).Value(row);
            return true;
        case T::UINT32:
            *out = static_cast<const arrow::UInt32Array&>(arr).Value(row);
            return true;
        case T::UINT64:
            *out = static_cast<const arrow::UInt64Array&>(arr).Value(row);
            return true;
        case T::TIMESTAMP:
            {
                auto& ta = static_cast<const arrow::TimestampArray&>(arr);
                auto unit = static_cast<const arrow::TimestampType&>(*arr.type()).unit();
                int64_t value = normalize_timestamp_to_millis(ta.Value(row), unit);
                if (value < 0)
                {
                    return false;
                }
                *out = static_cast<uint64_t>(value);
                return true;
            }
        default:
            return false;
    }
}

static Status validate_numeric_sort_fast_path_rule(NumericSortFastPathMode mode, bool eligible)
{
    if (mode != NumericSortFastPathMode::On || eligible)
    {
        return Status::OK();
    }
    return Status::InvalidArg("INVALID_ARGUMENT: numeric_sort_fast_path=on requires the first row "
                              "key "
                              "segment to be a direct non-reversed numeric/timestamp column with "
                              "zero-left-padding");
}

static Status scalar_to_string(const arrow::Array& arr, int64_t row, std::string* out)
{
    out->clear();
    if (arr.IsNull(row))
    {
        return Status::OK();
    }

    using T = arrow::Type;
    auto t = arr.type_id();

    switch (t)
    {
        case T::STRING:
            {
                auto& sa = static_cast<const arrow::StringArray&>(arr);
                auto sv = sa.GetView(row);
                out->assign(sv.data(), sv.size());
                return Status::OK();
            }
        case T::LARGE_STRING:
            {
                auto& sa = static_cast<const arrow::LargeStringArray&>(arr);
                auto sv = sa.GetView(row);
                out->assign(sv.data(), sv.size());
                return Status::OK();
            }
        case T::INT8:
            *out = std::to_string(static_cast<const arrow::Int8Array&>(arr).Value(row));
            return Status::OK();
        case T::INT16:
            *out = std::to_string(static_cast<const arrow::Int16Array&>(arr).Value(row));
            return Status::OK();
        case T::INT32:
            *out = std::to_string(static_cast<const arrow::Int32Array&>(arr).Value(row));
            return Status::OK();
        case T::INT64:
            *out = std::to_string(static_cast<const arrow::Int64Array&>(arr).Value(row));
            return Status::OK();
        case T::UINT8:
            *out = std::to_string(static_cast<const arrow::UInt8Array&>(arr).Value(row));
            return Status::OK();
        case T::UINT16:
            *out = std::to_string(static_cast<const arrow::UInt16Array&>(arr).Value(row));
            return Status::OK();
        case T::UINT32:
            *out = std::to_string(static_cast<const arrow::UInt32Array&>(arr).Value(row));
            return Status::OK();
        case T::UINT64:
            *out = std::to_string(static_cast<const arrow::UInt64Array&>(arr).Value(row));
            return Status::OK();
        case T::FLOAT:
            *out = std::to_string(static_cast<const arrow::FloatArray&>(arr).Value(row));
            return Status::OK();
        case T::DOUBLE:
            *out = std::to_string(static_cast<const arrow::DoubleArray&>(arr).Value(row));
            return Status::OK();
        case T::BOOL:
            *out = static_cast<const arrow::BooleanArray&>(arr).Value(row) ? "1" : "0";
            return Status::OK();
        case T::TIMESTAMP:
            {
                auto& ta = static_cast<const arrow::TimestampArray&>(arr);
                auto unit = static_cast<const arrow::TimestampType&>(*arr.type()).unit();
                *out = std::to_string(normalize_timestamp_to_millis(ta.Value(row), unit));
                return Status::OK();
            }
        default:
            return Status::InvalidArg("SCHEMA_MISMATCH: unsupported Arrow type for row key field: "
                                      + arr.type()->ToString());
    }
}

static void append_base64(std::string_view bytes, std::string* out)
{
    static constexpr char kAlphabet[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    size_t i = 0;
    while (i + 3 <= bytes.size())
    {
        const uint32_t block = (static_cast<uint32_t>(static_cast<unsigned char>(bytes[i])) << 16)
                               | (static_cast<uint32_t>(static_cast<unsigned char>(bytes[i + 1])) << 8)
                               | static_cast<uint32_t>(static_cast<unsigned char>(bytes[i + 2]));
        out->push_back(kAlphabet[(block >> 18) & 0x3f]);
        out->push_back(kAlphabet[(block >> 12) & 0x3f]);
        out->push_back(kAlphabet[(block >> 6) & 0x3f]);
        out->push_back(kAlphabet[block & 0x3f]);
        i += 3;
    }

    const size_t remaining = bytes.size() - i;
    if (remaining == 1)
    {
        const uint32_t block = static_cast<uint32_t>(static_cast<unsigned char>(bytes[i])) << 16;
        out->push_back(kAlphabet[(block >> 18) & 0x3f]);
        out->push_back(kAlphabet[(block >> 12) & 0x3f]);
        out->push_back('=');
        out->push_back('=');
    }
    else if (remaining == 2)
    {
        const uint32_t block = (static_cast<uint32_t>(static_cast<unsigned char>(bytes[i])) << 16)
                               | (static_cast<uint32_t>(static_cast<unsigned char>(bytes[i + 1])) << 8);
        out->push_back(kAlphabet[(block >> 18) & 0x3f]);
        out->push_back(kAlphabet[(block >> 12) & 0x3f]);
        out->push_back(kAlphabet[(block >> 6) & 0x3f]);
        out->push_back('=');
    }
}

template <typename T> static void append_decimal(T value, std::string* out)
{
    // 32 bytes covers the sign and every decimal digit of all integral Arrow
    // types. This keeps the conversion on the stack instead of creating one
    // temporary std::string per numeric cell.
    std::array<char, 32> buffer{};
    const auto result = std::to_chars(buffer.data(), buffer.data() + buffer.size(), value);
    out->append(buffer.data(), static_cast<size_t>(result.ptr - buffer.data()));
}

template <typename ArrayType>
static Status append_integral_column(const BatchColumnPlan& plan, int64_t row, std::string* out)
{
    const auto& array = static_cast<const ArrayType&>(*plan.column);
    if (array.IsNull(row))
    {
        return Status::OK();
    }
    append_decimal(array.Value(row), out);
    return Status::OK();
}

template <typename ArrayType>
static Status append_string_column(const BatchColumnPlan& plan, int64_t row, std::string* out)
{
    const auto& array = static_cast<const ArrayType&>(*plan.column);
    if (array.IsNull(row))
    {
        return Status::OK();
    }
    const auto value = array.GetView(row);
    out->append(value.data(), value.size());
    return Status::OK();
}

template <typename ArrayType>
static Status append_binary_column(const BatchColumnPlan& plan, int64_t row, std::string* out)
{
    const auto& array = static_cast<const ArrayType&>(*plan.column);
    if (array.IsNull(row))
    {
        return Status::OK();
    }
    const auto value = array.GetView(row);
    append_base64(std::string_view(value.data(), value.size()), out);
    return Status::OK();
}

template <typename ArrayType>
static Status append_floating_column(const BatchColumnPlan& plan, int64_t row, std::string* out)
{
    const auto& array = static_cast<const ArrayType&>(*plan.column);
    if (array.IsNull(row))
    {
        return Status::OK();
    }
    // Keep std::to_string here: C++20 specifies its fixed-six-decimal
    // representation and changing formatting would change HFile bytes.
    out->append(std::to_string(array.Value(row)));
    return Status::OK();
}

static Status append_boolean_column(const BatchColumnPlan& plan, int64_t row, std::string* out)
{
    const auto& array = static_cast<const arrow::BooleanArray&>(*plan.column);
    if (array.IsNull(row))
    {
        return Status::OK();
    }
    out->push_back(array.Value(row) ? '1' : '0');
    return Status::OK();
}

template <arrow::TimeUnit::type Unit>
static Status append_timestamp_column(const BatchColumnPlan& plan, int64_t row, std::string* out)
{
    const auto& array = static_cast<const arrow::TimestampArray&>(*plan.column);
    if (array.IsNull(row))
    {
        return Status::OK();
    }
    append_decimal(normalize_timestamp_to_millis(array.Value(row), Unit), out);
    return Status::OK();
}

static Status append_fallback_column(const BatchColumnPlan& plan, int64_t row, std::string* out)
{
    if (plan.column->IsNull(row))
    {
        return Status::OK();
    }
    auto scalar = plan.column->GetScalar(row);
    if (!scalar.ok())
    {
        return Status::InvalidArg("cannot materialize Arrow scalar as text: " + scalar.status().ToString());
    }
    out->append(scalar.ValueOrDie()->ToString());
    return Status::OK();
}

static ColumnAppendFn compile_column_appender(const arrow::Array& array)
{
    using T = arrow::Type;
    switch (array.type_id())
    {
        case T::STRING:
            return append_string_column<arrow::StringArray>;
        case T::LARGE_STRING:
            return append_string_column<arrow::LargeStringArray>;
        case T::BINARY:
            return append_binary_column<arrow::BinaryArray>;
        case T::LARGE_BINARY:
            return append_binary_column<arrow::LargeBinaryArray>;
        case T::INT8:
            return append_integral_column<arrow::Int8Array>;
        case T::INT16:
            return append_integral_column<arrow::Int16Array>;
        case T::INT32:
            return append_integral_column<arrow::Int32Array>;
        case T::INT64:
            return append_integral_column<arrow::Int64Array>;
        case T::UINT8:
            return append_integral_column<arrow::UInt8Array>;
        case T::UINT16:
            return append_integral_column<arrow::UInt16Array>;
        case T::UINT32:
            return append_integral_column<arrow::UInt32Array>;
        case T::UINT64:
            return append_integral_column<arrow::UInt64Array>;
        case T::FLOAT:
            return append_floating_column<arrow::FloatArray>;
        case T::DOUBLE:
            return append_floating_column<arrow::DoubleArray>;
        case T::BOOL:
            return append_boolean_column;
        case T::TIMESTAMP:
            {
                const auto unit = static_cast<const arrow::TimestampType&>(*array.type()).unit();
                switch (unit)
                {
                    case arrow::TimeUnit::SECOND:
                        return append_timestamp_column<arrow::TimeUnit::SECOND>;
                    case arrow::TimeUnit::MILLI:
                        return append_timestamp_column<arrow::TimeUnit::MILLI>;
                    case arrow::TimeUnit::MICRO:
                        return append_timestamp_column<arrow::TimeUnit::MICRO>;
                    case arrow::TimeUnit::NANO:
                        return append_timestamp_column<arrow::TimeUnit::NANO>;
                }
            }
        default:
            return append_fallback_column;
    }
    return append_fallback_column;
}

// --- Open Arrow IPC Stream file
// ----

static arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchStreamReader>> open_arrow_stream(const std::string& path)
{
    ARROW_ASSIGN_OR_RAISE(auto file, arrow::io::ReadableFile::Open(path));
    return arrow::ipc::RecordBatchStreamReader::Open(file);
}

// --- First pass: build sort index ----

static Status prepare_row_key_columns(const std::shared_ptr<arrow::Schema>& filtered_schema,
                                      const arrow_convert::RowKeyBuilder& row_key_builder,
                                      int max_column_index,
                                      std::vector<int>* referenced_column_indices,
                                      std::vector<int>* suffix_referenced_column_indices)
{
    const int column_count = filtered_schema->num_fields();
    if (max_column_index >= column_count)
    {
        return Status::InvalidArg("SCHEMA_MISMATCH: rowKeyRule references column index "
                                  + std::to_string(max_column_index) + " but filtered Arrow schema has only "
                                  + std::to_string(column_count) + " columns. "
                                  + "Note: column indices are based on the schema AFTER excluded "
                                    "columns are removed.");
    }

    std::vector<uint8_t> referenced_columns(static_cast<size_t>(std::max(0, column_count)), 0);
    for (const auto& segment : row_key_builder.segments())
    {
        if (segment.type != arrow_convert::RowKeySegment::Type::ColumnRef
            && segment.type != arrow_convert::RowKeySegment::Type::EncodedColumn)
        {
            continue;
        }
        if (segment.col_index >= column_count)
        {
            return Status::InvalidArg("SCHEMA_MISMATCH: rowKeyRule references column index "
                                      + std::to_string(segment.col_index) + " but filtered Arrow schema has only "
                                      + std::to_string(column_count) + " columns.");
        }
        referenced_columns[static_cast<size_t>(segment.col_index)] = true;
    }

    referenced_column_indices->reserve(row_key_builder.segments().size());
    for (int column_index = 0; column_index < column_count; ++column_index)
    {
        if (referenced_columns[static_cast<size_t>(column_index)] == 0)
        {
            continue;
        }
        referenced_column_indices->push_back(column_index);
        if (!is_supported_rowkey_type(filtered_schema->field(column_index)->type()))
        {
            return Status::InvalidArg("SCHEMA_MISMATCH: unsupported Arrow type for row key field '"
                                      + filtered_schema->field(column_index)->name()
                                      + "': " + filtered_schema->field(column_index)->type()->ToString());
        }
    }

    if (row_key_builder.segments().size() <= 1)
    {
        return Status::OK();
    }

    std::vector<uint8_t> suffix_referenced_columns(static_cast<size_t>(std::max(0, column_count)), 0);
    for (size_t index = 1; index < row_key_builder.segments().size(); ++index)
    {
        const auto& segment = row_key_builder.segments()[index];
        if (segment.type != arrow_convert::RowKeySegment::Type::ColumnRef
            && segment.type != arrow_convert::RowKeySegment::Type::EncodedColumn)
        {
            continue;
        }
        suffix_referenced_columns[static_cast<size_t>(segment.col_index)] = true;
    }
    suffix_referenced_column_indices->reserve(row_key_builder.segments().size() - 1);
    for (int column_index = 0; column_index < column_count; ++column_index)
    {
        if (suffix_referenced_columns[static_cast<size_t>(column_index)] != 0)
        {
            suffix_referenced_column_indices->push_back(column_index);
        }
    }
    return Status::OK();
}

static Status materialize_numeric_sort_entries(const arrow_convert::RowKeySegment* first_segment,
                                               bool single_numeric_fast_path,
                                               int32_t stop_batch_index,
                                               int64_t stop_row_index,
                                               std::vector<StringSortEntry>* string_index,
                                               std::vector<NumericSortEntry>* numeric_index,
                                               std::deque<std::string>* owned_row_keys,
                                               const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
                                               memory::MemoryBudget* budget)
{
    if (first_segment == nullptr)
    {
        return Status::InvalidArg("INVALID_ROW_KEY_RULE: numeric row key segment missing");
    }

    // Avoid requiring compact numeric and final string indices to coexist.
    // Rebuilding pure numeric keys from retained batches is deterministic.
    if (budget != nullptr && single_numeric_fast_path)
    {
        const size_t expected_entries = numeric_index->size();
        const size_t numeric_capacity_bytes = numeric_index->capacity() * sizeof(NumericSortEntry);
        std::vector<NumericSortEntry>().swap(*numeric_index);
        if (numeric_capacity_bytes > 0)
        {
            budget->release(numeric_capacity_bytes);
        }

        HFILE_RETURN_IF_ERROR(reserve_sort_entries(string_index, expected_entries, budget));
        const size_t original_entry_count = string_index->size();
        for (int32_t batch_index = 0; batch_index <= stop_batch_index; ++batch_index)
        {
            const auto& batch = batches[static_cast<size_t>(batch_index)];
            const auto& numeric_column = *batch->columns()[static_cast<size_t>(first_segment->col_index)];
            const int64_t row_limit = batch_index == stop_batch_index ? stop_row_index : batch->num_rows();
            for (int64_t row_index = 0; row_index < row_limit; ++row_index)
            {
                if (numeric_column.IsNull(row_index))
                {
                    continue;
                }
                std::string row_key;
                auto status = build_simple_rowkey_fast(numeric_column, row_index, first_segment->pad_len, &row_key);
                if (!status.ok())
                {
                    return Status::InvalidArg("INVALID_ROW_KEY_RULE: " + status.message());
                }
                if (row_key.empty())
                {
                    continue;
                }
                HFILE_RETURN_IF_ERROR(budget->reserve(row_key.size()));
                owned_row_keys->push_back(std::move(row_key));
                string_index->push_back({owned_row_keys->back(), batch_index, static_cast<int32_t>(row_index)});
            }
        }
        if (string_index->size() - original_entry_count != expected_entries)
        {
            return Status::Internal("numeric sort fallback rebuild count mismatch");
        }
        return Status::OK();
    }

    HFILE_RETURN_IF_ERROR(reserve_sort_entries(string_index, numeric_index->size(), budget));
    for (const auto& entry : *numeric_index)
    {
        std::string row_key;
        auto status = append_zero_left_padded_decimal(entry.numeric_sort_key, first_segment->pad_len, &row_key);
        if (!status.ok())
        {
            return Status::InvalidArg("INVALID_ROW_KEY_RULE: " + status.message());
        }
        if (budget != nullptr)
        {
            HFILE_RETURN_IF_ERROR(budget->reserve(row_key.size()));
        }
        owned_row_keys->push_back(std::move(row_key));
        string_index->push_back({owned_row_keys->back(), entry.batch_idx, entry.row_idx});
    }

    const size_t numeric_capacity_bytes = numeric_index->capacity() * sizeof(NumericSortEntry);
    std::vector<NumericSortEntry>().swap(*numeric_index);
    if (budget != nullptr && numeric_capacity_bytes > 0)
    {
        budget->release(numeric_capacity_bytes);
    }
    return Status::OK();
}

struct SortRowContext
{
    arrow_convert::RowKeyBuilder& row_key_builder;
    const std::vector<int>& referenced_column_indices;
    const std::vector<int>& suffix_referenced_column_indices;
    std::vector<std::string>& owned_fields;
    std::vector<std::string_view>& fields;
    const arrow_convert::RowKeySegment* first_fast_segment;
    bool single_numeric_fast_path;
    uint64_t numeric_sort_max_value;
    NumericSortFastPathMode numeric_sort_fast_path_mode;
    bool numeric_sort_path_active;
    std::vector<StringSortEntry>& string_index;
    std::vector<NumericSortEntry>& numeric_index;
    std::deque<std::string>& owned_row_keys;
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches;
    memory::MemoryBudget* budget;
    ConvertResult& result;
};

static void append_direct_string_sort_entry(
    const arrow::Array& column, bool column_is_string, int32_t batch_index, int64_t row_index, SortRowContext* context)
{
    if (column.IsNull(row_index))
    {
        ++context->result.kv_skipped_count;
        return;
    }

    std::string_view row_key;
    if (column_is_string)
    {
        const auto value = static_cast<const arrow::StringArray&>(column).GetView(row_index);
        row_key = std::string_view(value.data(), value.size());
    }
    else
    {
        const auto value = static_cast<const arrow::LargeStringArray&>(column).GetView(row_index);
        row_key = std::string_view(value.data(), value.size());
    }
    if (row_key.empty())
    {
        ++context->result.kv_skipped_count;
        return;
    }
    context->string_index.push_back({row_key, batch_index, static_cast<int32_t>(row_index)});
}

static Status append_numeric_sort_entry(const std::vector<std::shared_ptr<arrow::Array>>& columns,
                                        const arrow::Array& numeric_column,
                                        int32_t batch_index,
                                        int64_t row_index,
                                        int64_t batch_row_count,
                                        SortRowContext* context,
                                        bool* completed)
{
    *completed = false;
    if (!context->numeric_sort_path_active)
    {
        return Status::OK();
    }
    if (numeric_column.IsNull(row_index))
    {
        ++context->result.kv_skipped_count;
        *completed = true;
        return Status::OK();
    }

    uint64_t numeric_sort_key = 0;
    const bool valid_key = try_extract_nonnegative_numeric_sort_key(numeric_column, row_index, &numeric_sort_key);
    const bool key_fits = valid_key && numeric_sort_key <= context->numeric_sort_max_value;
    if (!key_fits)
    {
        if (context->numeric_sort_fast_path_mode == NumericSortFastPathMode::On)
        {
            const char* requirement = valid_key ? "fit the configured padLen" : "be non-negative";
            return Status::InvalidArg("INVALID_ARGUMENT: numeric_sort_fast_path=on requires all "
                                      "row key values to "
                                      + std::string(requirement));
        }
        HFILE_RETURN_IF_ERROR(materialize_numeric_sort_entries(context->first_fast_segment,
                                                               context->single_numeric_fast_path,
                                                               batch_index,
                                                               row_index,
                                                               &context->string_index,
                                                               &context->numeric_index,
                                                               &context->owned_row_keys,
                                                               context->batches,
                                                               context->budget));
        context->numeric_sort_path_active = false;
        if (context->single_numeric_fast_path)
        {
            HFILE_RETURN_IF_ERROR(reserve_sort_entries(
                &context->string_index, static_cast<size_t>(batch_row_count - row_index), context->budget));
        }
        return Status::OK();
    }

    if (context->single_numeric_fast_path)
    {
        context->numeric_index.push_back({numeric_sort_key, batch_index, static_cast<int32_t>(row_index)});
        *completed = true;
        return Status::OK();
    }

    std::string row_key_suffix;
    for (int column_index : context->suffix_referenced_column_indices)
    {
        auto status = scalar_to_string(*columns[static_cast<size_t>(column_index)],
                                       row_index,
                                       &context->owned_fields[static_cast<size_t>(column_index)]);
        HFILE_RETURN_IF_ERROR(status);
        context->fields[static_cast<size_t>(column_index)] = context->owned_fields[static_cast<size_t>(column_index)];
    }
    auto status = context->row_key_builder.build_checked_from_segment(1, context->fields, &row_key_suffix);
    if (!status.ok())
    {
        return Status::InvalidArg("INVALID_ROW_KEY_RULE: " + status.message());
    }

    std::string row_key;
    status = append_zero_left_padded_decimal(numeric_sort_key, context->first_fast_segment->pad_len, &row_key);
    if (!status.ok())
    {
        return Status::InvalidArg("INVALID_ROW_KEY_RULE: " + status.message());
    }
    row_key.append(row_key_suffix);
    if (context->budget != nullptr)
    {
        HFILE_RETURN_IF_ERROR(context->budget->reserve(row_key.size()));
    }
    context->owned_row_keys.push_back(std::move(row_key));
    context->string_index.push_back({context->owned_row_keys.back(), batch_index, static_cast<int32_t>(row_index)});
    *completed = true;
    return Status::OK();
}

static Status append_generic_sort_entry(const std::vector<std::shared_ptr<arrow::Array>>& columns,
                                        const arrow::Array* numeric_column,
                                        int32_t batch_index,
                                        int64_t row_index,
                                        SortRowContext* context)
{
    std::string row_key;
    if (context->single_numeric_fast_path)
    {
        auto status
            = build_simple_rowkey_fast(*numeric_column, row_index, context->first_fast_segment->pad_len, &row_key);
        if (!status.ok())
        {
            return Status::InvalidArg("INVALID_ROW_KEY_RULE: " + status.message());
        }
    }
    else
    {
        for (int column_index : context->referenced_column_indices)
        {
            auto status = scalar_to_string(*columns[static_cast<size_t>(column_index)],
                                           row_index,
                                           &context->owned_fields[static_cast<size_t>(column_index)]);
            HFILE_RETURN_IF_ERROR(status);
            context->fields[static_cast<size_t>(column_index)]
                = context->owned_fields[static_cast<size_t>(column_index)];
        }
        auto status = context->row_key_builder.build_checked(context->fields, &row_key);
        if (!status.ok())
        {
            return Status::InvalidArg("INVALID_ROW_KEY_RULE: " + status.message());
        }
    }

    if (row_key.empty())
    {
        ++context->result.kv_skipped_count;
        return Status::OK();
    }
    if (context->budget != nullptr)
    {
        HFILE_RETURN_IF_ERROR(context->budget->reserve(row_key.size()));
    }
    context->owned_row_keys.push_back(std::move(row_key));
    context->string_index.push_back({context->owned_row_keys.back(), batch_index, static_cast<int32_t>(row_index)});
    return Status::OK();
}

static Status build_sort_index(const std::string& arrow_path,
                               arrow_convert::RowKeyBuilder& rkb,
                               int max_col_idx,
                               const std::vector<int>& removal_indices, // sorted desc, applied per-batch
                               std::vector<StringSortEntry>& string_index_out,
                               std::vector<NumericSortEntry>& numeric_index_out,
                               std::deque<std::string>& owned_row_keys_out,
                               std::vector<std::shared_ptr<arrow::RecordBatch>>& batches_out,
                               memory::MemoryBudget* budget,
                               ConvertResult& result,
                               bool* used_numeric_sort_path_out,
                               int* numeric_sort_pad_len_out,
                               NumericSortFastPathMode numeric_sort_fast_path_mode)
{

    auto reader_res = open_arrow_stream(arrow_path);
    if (!reader_res.ok())
    {
        return Status::IoError("Cannot open Arrow file: " + reader_res.status().ToString());
    }

    auto reader = reader_res.ValueOrDie();

    // Compute the FILTERED schema directly from the original schema.
    // rowKeyRule indices are validated against this filtered schema.
    std::shared_ptr<arrow::Schema> filtered_schema;
    {
        auto filtered_res = apply_schema_removal(reader->schema(), removal_indices);
        if (!filtered_res.ok())
        {
            return Status::IoError("Failed to compute filtered schema: " + filtered_res.status().ToString());
        }
        filtered_schema = std::move(*filtered_res);
    }
    std::vector<int> referenced_col_indices;
    std::vector<int> suffix_referenced_col_indices;
    HFILE_RETURN_IF_ERROR(prepare_row_key_columns(
        filtered_schema, rkb, max_col_idx, &referenced_col_indices, &suffix_referenced_col_indices));

    std::shared_ptr<arrow::RecordBatch> raw_batch;
    int32_t batch_idx = 0;

    std::vector<std::string> owned_fields(max_col_idx >= 0 ? static_cast<size_t>(max_col_idx + 1) : 0);
    std::vector<std::string_view> fields(owned_fields.size());
    const int direct_col_idx = rkb.direct_passthrough_col_index();
    const bool direct_string_fast_path
        = direct_col_idx >= 0
          && (filtered_schema->field(direct_col_idx)->type()->id() == arrow::Type::STRING
              || filtered_schema->field(direct_col_idx)->type()->id() == arrow::Type::LARGE_STRING);
    const arrow_convert::RowKeySegment* first_fast_segment = rkb.segments().empty() ? nullptr : &rkb.segments().front();
    const bool numeric_prefix_fast_path = first_fast_segment != nullptr
                                          && first_fast_segment->type == arrow_convert::RowKeySegment::Type::ColumnRef
                                          && !first_fast_segment->reverse && !first_fast_segment->pad_right
                                          && first_fast_segment->pad_char == '0' && first_fast_segment->pad_len > 0
                                          && is_simple_numeric_rowkey_fast_path_type(
                                              filtered_schema->field(first_fast_segment->col_index)->type()->id());
    const bool single_numeric_fast_path = numeric_prefix_fast_path && rkb.segments().size() == 1;
    const uint64_t numeric_sort_max_value
        = first_fast_segment != nullptr ? max_zero_left_padded_value(first_fast_segment->pad_len) : 0;
    HFILE_RETURN_IF_ERROR(validate_numeric_sort_fast_path_rule(numeric_sort_fast_path_mode, numeric_prefix_fast_path));
    bool numeric_sort_path_active
        = numeric_sort_fast_path_mode != NumericSortFastPathMode::Off && numeric_prefix_fast_path;

    while (true)
    {
        auto s = reader->ReadNext(&raw_batch);
        if (!s.ok())
        {
            return Status::IoError("Arrow read error: " + s.ToString());
        }
        if (!raw_batch)
        {
            break; // EOS
        }

        // -- Strip excluded columns from the batch ----
        // After this call the batch's schema == filtered_schema.
        // All downstream code (row key building + second pass KV output) uses
        // the filtered schema, so rowKeyRule indices need no adjustment.
        std::shared_ptr<arrow::RecordBatch> batch;
        if (!removal_indices.empty())
        {
            auto stripped = apply_column_removal(raw_batch, removal_indices);
            if (!stripped.ok())
            {
                return Status::IoError("Column removal failed: " + stripped.status().ToString());
            }
            batch = std::move(*stripped);
        }
        else
        {
            batch = std::move(raw_batch);
        }

        int64_t n_rows = batch->num_rows();
        if (budget)
        {
            auto reserve_status
                = budget->reserve(static_cast<size_t>(std::max<int64_t>(0, arrow::util::TotalBufferSize(*batch))));
            if (!reserve_status.ok())
            {
                return reserve_status;
            }
        }
        batches_out.push_back(batch); // store the FILTERED batch
        const size_t batch_row_count = static_cast<size_t>(std::max<int64_t>(0, n_rows));
        if (single_numeric_fast_path && numeric_sort_path_active)
        {
            HFILE_RETURN_IF_ERROR(reserve_sort_entries(&numeric_index_out, batch_row_count, budget));
        }
        else
        {
            HFILE_RETURN_IF_ERROR(reserve_sort_entries(&string_index_out, batch_row_count, budget));
        }

        const auto& batch_columns = batch->columns();
        const arrow::Array* direct_column
            = direct_string_fast_path ? batch_columns[static_cast<size_t>(direct_col_idx)].get() : nullptr;
        const arrow::Array* numeric_column
            = numeric_prefix_fast_path ? batch_columns[static_cast<size_t>(first_fast_segment->col_index)].get()
                                       : nullptr;
        const bool direct_column_is_string
            = direct_column != nullptr && direct_column->type_id() == arrow::Type::STRING;

        SortRowContext row_context{rkb,
                                   referenced_col_indices,
                                   suffix_referenced_col_indices,
                                   owned_fields,
                                   fields,
                                   first_fast_segment,
                                   single_numeric_fast_path,
                                   numeric_sort_max_value,
                                   numeric_sort_fast_path_mode,
                                   numeric_sort_path_active,
                                   string_index_out,
                                   numeric_index_out,
                                   owned_row_keys_out,
                                   batches_out,
                                   budget,
                                   result};
        for (int64_t row_index = 0; row_index < n_rows; ++row_index)
        {
            if (direct_string_fast_path)
            {
                append_direct_string_sort_entry(
                    *direct_column, direct_column_is_string, batch_idx, row_index, &row_context);
                continue;
            }

            bool completed = false;
            if (numeric_prefix_fast_path)
            {
                HFILE_RETURN_IF_ERROR(append_numeric_sort_entry(
                    batch_columns, *numeric_column, batch_idx, row_index, n_rows, &row_context, &completed));
            }
            if (!completed)
            {
                HFILE_RETURN_IF_ERROR(
                    append_generic_sort_entry(batch_columns, numeric_column, batch_idx, row_index, &row_context));
            }
        }
        numeric_sort_path_active = row_context.numeric_sort_path_active;

        result.arrow_batches_read++;
        result.arrow_rows_read += n_rows;
        batch_idx++;
    }
    if (used_numeric_sort_path_out)
    {
        *used_numeric_sort_path_out = numeric_sort_path_active && numeric_prefix_fast_path;
    }
    if (numeric_sort_pad_len_out)
    {
        *numeric_sort_pad_len_out = first_fast_segment != nullptr ? first_fast_segment->pad_len : 0;
    }
    return Status::OK();
}

static std::vector<BatchColumnPlan> build_column_plans(const std::shared_ptr<arrow::RecordBatch>& batch)
{
    std::vector<BatchColumnPlan> plans;
    plans.reserve(static_cast<size_t>(batch->num_columns()));
    auto schema = batch->schema();
    const auto& columns = batch->columns();
    for (int col_idx = 0; col_idx < batch->num_columns(); ++col_idx)
    {
        const auto* column = columns[static_cast<size_t>(col_idx)].get();
        plans.push_back({schema->field(col_idx)->name(), column, compile_column_appender(*column)});
    }
    return plans;
}

static Status build_joined_row_value(const std::vector<BatchColumnPlan>& columns,
                                     int32_t row_idx,
                                     std::string_view row_key,
                                     std::string* row_value,
                                     Pass2Profile* profile = nullptr)
{
    row_value->clear();
    bool first = true;

    for (const auto& column_ref : columns)
    {
        if (!first)
        {
            row_value->push_back('|');
        }
        first = false;

        const auto materialize_start
            = profile != nullptr ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
        auto value_status = column_ref.append(column_ref, row_idx, row_value);
        if (profile)
        {
            profile->materialize_ns += std::chrono::steady_clock::now() - materialize_start;
            ++profile->materialized_cells;
        }
        if (!value_status.ok())
        {
            return Status::InvalidArg("Cell skipped (" + std::string(row_key) + "/" + std::string(column_ref.name)
                                      + "): " + value_status.message());
        }
    }
    return Status::OK();
}

static Status append_row_value_cell(HFileWriter& writer,
                                    const std::vector<BatchColumnPlan>& columns,
                                    int32_t row_idx,
                                    std::string_view row_key,
                                    const std::string& cf,
                                    int64_t default_ts,
                                    std::string* row_value,
                                    int64_t* kv_written,
                                    int64_t* kv_skipped,
                                    Pass2Profile* profile = nullptr)
{
    (void)kv_skipped;
    int64_t ts = default_ts > 0 ? default_ts
                                : std::chrono::duration_cast<std::chrono::milliseconds>(
                                      std::chrono::system_clock::now().time_since_epoch())
                                      .count();

    HFILE_RETURN_IF_ERROR(build_joined_row_value(columns, row_idx, row_key, row_value, profile));

    auto row_bytes = as_bytes(row_key);
    auto cf_bytes = as_bytes(cf);
    KeyValue kv;
    kv.row = row_bytes;
    kv.family = cf_bytes;
    kv.qualifier = {kEmptyQualifierBytes.data(), 0};
    kv.timestamp = ts;
    kv.key_type = KeyType::Put;
    kv.value = as_bytes(*row_value);

    const auto append_start
        = profile != nullptr ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
    auto s = writer.append_trusted_new_row(kv);
    if (profile)
    {
        profile->append_ns += std::chrono::steady_clock::now() - append_start;
        ++profile->appended_cells;
    }
    if (!s.ok())
    {
        return s;
    }
    if (kv_written)
    {
        ++(*kv_written);
    }
    return Status::OK();
}

struct SortSummary
{
    bool pure_numeric{false};
    size_t entry_count{0};
};

static SortSummary sort_entries(const arrow_convert::RowKeyBuilder& row_key_builder,
                                bool used_numeric_sort_path,
                                std::vector<StringSortEntry>* string_sort_index,
                                std::vector<NumericSortEntry>* numeric_sort_index,
                                memory::MemoryBudget* budget)
{
    const bool pure_numeric = used_numeric_sort_path && row_key_builder.segments().size() == 1;
    bool sorted = false;
    const size_t radix_budget_bytes = numeric_sort_index->size() * sizeof(NumericSortEntry);
    if (pure_numeric)
    {
        const bool budget_reserved = budget == nullptr || budget->reserve(radix_budget_bytes).ok();
        if (budget_reserved)
        {
            try
            {
                radix_sort_numeric_entries(numeric_sort_index);
                sorted = true;
            }
            catch (const std::bad_alloc&)
            {
                // Preserve the in-place comparator fallback under memory
                // pressure.
                sorted = false;
            }
            if (budget != nullptr && radix_budget_bytes > 0)
            {
                budget->release(radix_budget_bytes);
            }
        }
    }

    if (!sorted && pure_numeric)
    {
        std::sort(numeric_sort_index->begin(),
                  numeric_sort_index->end(),
                  [](const NumericSortEntry& left, const NumericSortEntry& right)
                  {
                      if (left.numeric_sort_key != right.numeric_sort_key)
                      {
                          return left.numeric_sort_key < right.numeric_sort_key;
                      }
                      if (left.batch_idx != right.batch_idx)
                      {
                          return left.batch_idx < right.batch_idx;
                      }
                      return left.row_idx < right.row_idx;
                  });
    }
    else if (!sorted)
    {
        std::sort(string_sort_index->begin(),
                  string_sort_index->end(),
                  [](const StringSortEntry& left, const StringSortEntry& right)
                  {
                      const int key_comparison = left.row_key.compare(right.row_key);
                      if (key_comparison != 0)
                      {
                          return key_comparison < 0;
                      }
                      if (left.batch_idx != right.batch_idx)
                      {
                          return left.batch_idx < right.batch_idx;
                      }
                      return left.row_idx < right.row_idx;
                  });
    }

    return {pure_numeric, pure_numeric ? numeric_sort_index->size() : string_sort_index->size()};
}

static std::pair<std::unique_ptr<HFileWriter>, Status> build_conversion_writer(const std::string& path,
                                                                               const WriterOptions& options,
                                                                               size_t memory_budget,
                                                                               WriterStats* failure_stats)
{
    return HFileWriter::builder()
        .set_path(path)
        .set_column_family(options.column_family)
        .set_compression(options.compression)
        .set_compression_level(options.compression_level)
        .set_block_size(options.block_size)
        .set_data_block_encoding(options.data_block_encoding)
        .set_bloom_type(options.bloom_type)
        .set_bloom_error_rate(options.bloom_error_rate)
        .set_comparator(options.comparator)
        .set_file_create_time_ms(options.file_create_time_ms)
        .set_sort_mode(options.sort_mode)
        .set_include_tags(options.include_tags)
        .set_include_mvcc(options.include_mvcc)
        .set_fsync_policy(options.fsync_policy)
        .set_error_policy(options.error_policy)
        .set_max_error_count(options.max_error_count)
        .set_error_callback(options.error_callback)
        .set_max_row_key_bytes(options.max_row_key_bytes)
        .set_max_value_bytes(options.max_value_bytes)
        .set_max_memory(memory_budget)
        .set_compression_threads(options.compression_threads)
        .set_compression_queue_depth(options.compression_queue_depth)
        .set_min_free_disk(options.min_free_disk_bytes)
        .set_disk_check_interval(options.disk_check_interval_bytes)
        .set_max_open_files(options.max_open_files)
        .build(failure_stats);
}

static void copy_writer_stats(const HFileWriter& writer, size_t converter_memory_at_open, ConvertResult* result)
{
    const WriterStats stats = writer.stats();
    result->data_block_encode_ms = std::chrono::duration_cast<std::chrono::milliseconds>(stats.data_block_encode_ns);
    result->data_block_compress_ms
        = std::chrono::duration_cast<std::chrono::milliseconds>(stats.data_block_compress_ns);
    result->data_block_write_ms = std::chrono::duration_cast<std::chrono::milliseconds>(stats.data_block_write_ns);
    result->leaf_index_write_ms = std::chrono::duration_cast<std::chrono::milliseconds>(stats.leaf_index_write_ns);
    result->bloom_chunk_write_ms = std::chrono::duration_cast<std::chrono::milliseconds>(stats.bloom_chunk_write_ns);
    result->load_on_open_write_ms = std::chrono::duration_cast<std::chrono::milliseconds>(stats.load_on_open_write_ns);
    result->data_block_count = stats.data_block_count;
    result->leaf_index_block_count = stats.leaf_index_block_count;
    result->bloom_chunk_flush_count = stats.bloom_chunk_flush_count;
    result->load_on_open_block_count = stats.load_on_open_block_count;
    const size_t aggregate_peak = converter_memory_at_open + stats.memory_budget_peak_bytes;
    result->tracked_memory_peak_bytes = std::max(result->tracked_memory_peak_bytes, saturating_metric(aggregate_peak));
}

static void update_memory_metrics(const memory::MemoryBudget* budget, ConvertResult* result)
{
    if (budget != nullptr)
    {
        result->tracked_memory_peak_bytes
            = std::max(result->tracked_memory_peak_bytes, saturating_metric(budget->peak()));
    }
}

static Status write_sorted_rows(HFileWriter& writer,
                                const ConvertOptions& options,
                                bool pure_numeric_sort,
                                int numeric_sort_pad_len,
                                const std::vector<StringSortEntry>& string_sort_index,
                                const std::vector<NumericSortEntry>& numeric_sort_index,
                                const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
                                ConvertResult* result,
                                Pass2Profile* profile)
{
    const size_t entry_count = pure_numeric_sort ? numeric_sort_index.size() : string_sort_index.size();
    const int64_t progress_step = std::max(int64_t{1}, static_cast<int64_t>(entry_count) / 20);
    int64_t rows_done = 0;
    std::string numeric_row_key_storage;
    std::string row_value_storage;
    std::vector<std::vector<BatchColumnPlan>> batch_column_plans;
    batch_column_plans.reserve(batches.size());
    for (const auto& batch : batches)
    {
        batch_column_plans.push_back(build_column_plans(batch));
    }

    for (size_t first_index = 0; first_index < entry_count;)
    {
        std::string_view row_key;
        uint64_t numeric_sort_key = 0;
        if (pure_numeric_sort)
        {
            numeric_sort_key = numeric_sort_index[first_index].numeric_sort_key;
            auto status
                = append_zero_left_padded_decimal(numeric_sort_key, numeric_sort_pad_len, &numeric_row_key_storage);
            if (!status.ok())
            {
                return Status::InvalidArg("INVALID_ROW_KEY_RULE: " + status.message());
            }
            row_key = numeric_row_key_storage;
        }
        else
        {
            row_key = string_sort_index[first_index].row_key;
        }

        size_t next_index = first_index;
        if (pure_numeric_sort)
        {
            while (next_index < entry_count && numeric_sort_index[next_index].numeric_sort_key == numeric_sort_key)
            {
                ++next_index;
            }
        }
        else
        {
            while (next_index < entry_count && string_sort_index[next_index].row_key == row_key)
            {
                ++next_index;
            }
        }

        const size_t source_row_count = next_index - first_index;
        if (source_row_count > 1)
        {
            const std::string display_key = row_key.size() > 64 ? std::string(row_key.substr(0, 64)) + "...(+"
                                                                      + std::to_string(row_key.size() - 64) + "B)"
                                                                : std::string(row_key);
            clog::warn("DUPLICATE_KEY: row key '" + display_key + "' was produced by "
                       + std::to_string(source_row_count) + " source rows — keeping first, discarding "
                       + std::to_string(source_row_count - 1) + " duplicate(s). Check your rowKeyRule for uniqueness.");
            ++result->duplicate_key_count;
        }

        const int32_t batch_index
            = pure_numeric_sort ? numeric_sort_index[first_index].batch_idx : string_sort_index[first_index].batch_idx;
        const int32_t row_index
            = pure_numeric_sort ? numeric_sort_index[first_index].row_idx : string_sort_index[first_index].row_idx;
        int64_t written_count = 0;
        int64_t skipped_count = 0;
        HFILE_RETURN_IF_ERROR(append_row_value_cell(writer,
                                                    batch_column_plans[static_cast<size_t>(batch_index)],
                                                    row_index,
                                                    row_key,
                                                    options.column_family,
                                                    options.default_timestamp,
                                                    &row_value_storage,
                                                    &written_count,
                                                    &skipped_count,
                                                    profile));
        skipped_count += static_cast<int64_t>(source_row_count - 1);
        result->kv_written_count += written_count;
        result->kv_skipped_count += skipped_count;
        rows_done += static_cast<int64_t>(source_row_count);

        if (options.progress_cb && rows_done % progress_step == 0)
        {
            options.progress_cb(rows_done, static_cast<int64_t>(entry_count));
        }
        first_index = next_index;
    }
    return Status::OK();
}

static bool validate_conversion_options(const ConvertOptions& options, ConvertResult* result)
{
    if (options.arrow_path.empty())
    {
        result->error_code = ErrorCode::INVALID_ARGUMENT;
        result->error_message = "arrow_path is empty";
        return false;
    }
    if (options.hfile_path.empty())
    {
        result->error_code = ErrorCode::INVALID_ARGUMENT;
        result->error_message = "hfile_path is empty";
        return false;
    }
    if (!std::filesystem::exists(options.arrow_path))
    {
        result->error_code = ErrorCode::ARROW_FILE_ERROR;
        result->error_message = "Arrow file not found: " + options.arrow_path;
        return false;
    }
    if (options.row_key_rule.empty())
    {
        result->error_code = ErrorCode::INVALID_ROW_KEY_RULE;
        result->error_message = "row_key_rule is empty";
        return false;
    }
    return true;
}

static void log_conversion_result(const ConvertResult& result, const Pass2Profile* profile)
{
    clog::info("Convert done: kvs=" + std::to_string(result.kv_written_count) + " skipped="
               + std::to_string(result.kv_skipped_count) + " hfile=" + std::to_string(result.hfile_size_bytes / 1024)
               + "KB" + " elapsed=" + std::to_string(result.elapsed_ms.count()) + "ms");
    clog::info("Write breakdown: data_blocks=" + std::to_string(result.data_block_count)
               + " encode=" + std::to_string(result.data_block_encode_ms.count()) + "ms"
               + " compress=" + std::to_string(result.data_block_compress_ms.count()) + "ms"
               + " data_write=" + std::to_string(result.data_block_write_ms.count()) + "ms"
               + " leaf_index=" + std::to_string(result.leaf_index_write_ms.count()) + "ms"
               + " bloom_chunk=" + std::to_string(result.bloom_chunk_write_ms.count()) + "ms"
               + " load_on_open=" + std::to_string(result.load_on_open_write_ms.count()) + "ms");
    if (profile == nullptr)
    {
        return;
    }

    clog::info("Pass2 breakdown: materialize="
               + std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(profile->materialize_ns).count())
               + "ms" + " append="
               + std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(profile->append_ns).count())
               + "ms" + " materialized_cells=" + std::to_string(profile->materialized_cells)
               + " appended_cells=" + std::to_string(profile->appended_cells));
}

// --- convert() ----

ConvertResult convert(const ConvertOptions& opts)
{
    ConvertResult result;
    auto t_start = std::chrono::steady_clock::now();
    result.memory_budget_bytes = saturating_metric(opts.writer_opts.max_memory_bytes);
    result.numeric_sort_fast_path_mode = opts.numeric_sort_fast_path;

    clog::info("Convert started: arrow=" + opts.arrow_path + " hfile=" + opts.hfile_path
               + " rule=" + opts.row_key_rule);

    if (!validate_conversion_options(opts, &result))
    {
        return result;
    }

    // -- 2. Compile rowKeyRule ----
    auto [rkb, rk_status] = arrow_convert::RowKeyBuilder::compile(opts.row_key_rule);
    if (!rk_status.ok())
    {
        result.error_code = ErrorCode::INVALID_ROW_KEY_RULE;
        result.error_message = rk_status.message();
        return result;
    }
    int max_col_idx = rkb.max_col_index();

    // -- 3. Build column removal list ----
    // Compute which column indices (in the ORIGINAL Arrow schema) to physically
    // remove from each RecordBatch before any further processing.
    //
    // After removal the stored batches use the FILTERED schema.  rowKeyRule
    // indices then refer to positions in this filtered schema, so the caller
    // never needs to account for excluded columns in their index numbers.
    //
    // The extra schema-peek open is cheap: Arrow IPC Stream readers read the
    // schema message first; we never read actual row data here.
    std::vector<int> removal_indices; // sorted descending
    if (!opts.excluded_columns.empty() || !opts.excluded_column_prefixes.empty())
    {
        auto schema_reader_res = open_arrow_stream(opts.arrow_path);
        if (!schema_reader_res.ok())
        {
            result.error_code = ErrorCode::ARROW_FILE_ERROR;
            result.error_message = "Cannot open Arrow file for schema read: " + schema_reader_res.status().ToString();
            return result;
        }
        removal_indices = build_removal_indices(
            schema_reader_res.ValueOrDie()->schema(), opts.excluded_columns, opts.excluded_column_prefixes);
    }

    // -- 4. First pass: build sort index (load all batches into memory) ----
    clog::info("Pass 1: building sort index...");
    std::vector<StringSortEntry> string_sort_index;
    std::vector<NumericSortEntry> numeric_sort_index;
    std::deque<std::string> owned_row_keys;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    bool used_numeric_sort_path = false;
    int numeric_sort_pad_len = 0;
    std::unique_ptr<memory::MemoryBudget> budget;
    if (opts.writer_opts.max_memory_bytes > 0)
    {
        budget = std::make_unique<memory::MemoryBudget>(opts.writer_opts.max_memory_bytes);
    }
    auto t_sort_start = std::chrono::steady_clock::now();
    {
        Status s = build_sort_index(opts.arrow_path,
                                    rkb,
                                    max_col_idx,
                                    removal_indices,
                                    string_sort_index,
                                    numeric_sort_index,
                                    owned_row_keys,
                                    batches,
                                    budget.get(),
                                    result,
                                    &used_numeric_sort_path,
                                    &numeric_sort_pad_len,
                                    opts.numeric_sort_fast_path);
        if (!s.ok())
        {
            update_memory_metrics(budget.get(), &result);
            result.error_code = map_pass1_status_to_error_code(s);
            result.error_message = s.message();
            clog::err("Pass 1 failed: " + s.message());
            return result;
        }
    }

    const SortSummary sort_summary
        = sort_entries(rkb, used_numeric_sort_path, &string_sort_index, &numeric_sort_index, budget.get());
    const bool pure_numeric_sort = sort_summary.pure_numeric;
    const size_t sort_entry_count = sort_summary.entry_count;

    result.sort_ms
        = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t_sort_start);
    result.numeric_sort_fast_path_used = used_numeric_sort_path;

    clog::info("Pass 1 done: rows=" + std::to_string(sort_entry_count)
               + " sort=" + std::to_string(result.sort_ms.count()) + "ms" + " numeric_sort_fast_path="
               + std::string(numeric_sort_fast_path_mode_name(result.numeric_sort_fast_path_mode))
               + " used=" + (result.numeric_sort_fast_path_used ? "true" : "false"));

    if (sort_entry_count == 0)
    {
        clog::warn("No valid rows after sort index build — writing empty HFile");
    }

    // -- 4. Open HFile writer ----
    WriterOptions wo = opts.writer_opts;
    wo.column_family = opts.column_family;
    if (wo.file_create_time_ms <= 0 && opts.default_timestamp > 0)
    {
        wo.file_create_time_ms = opts.default_timestamp;
    }
    wo.sort_mode = WriterOptions::SortMode::PreSortedTrusted;

    // Pass 1 retains Arrow batches and the sort index throughout Pass 2. Give
    // the writer only the unconsumed portion of the same per-convert budget so
    // converter + writer cannot each consume the full configured limit.
    const size_t converter_budget_at_writer_open = budget ? budget->used() : 0;
    const size_t writer_memory_budget = budget ? budget->remaining() : wo.max_memory_bytes;
    if (budget && writer_memory_budget == 0)
    {
        update_memory_metrics(budget.get(), &result);
        result.error_code = ErrorCode::MEMORY_EXHAUSTED;
        result.error_message = "MemoryBudget: no memory remains for HFile writer";
        clog::err("HFile open failed: " + result.error_message);
        return result;
    }

    WriterStats writer_build_failure_stats;
    auto [writer, ws] = build_conversion_writer(opts.hfile_path, wo, writer_memory_budget, &writer_build_failure_stats);

    if (!ws.ok() || writer == nullptr)
    {
        if (ws.ok())
        {
            ws = Status::Internal("HFile writer build returned no writer instance");
        }
        const size_t aggregate_failure_peak
            = converter_budget_at_writer_open + writer_build_failure_stats.memory_budget_peak_bytes;
        result.tracked_memory_peak_bytes
            = std::max(result.tracked_memory_peak_bytes, saturating_metric(aggregate_failure_peak));
        update_memory_metrics(budget.get(), &result);
        result.error_code = map_status_to_error_code(ws);
        result.error_message = ws.message();
        clog::err("HFile open failed: " + ws.message());
        return result;
    }

    // -- 5. Second pass: write in sorted order ----
    clog::info("Pass 2: writing HFile in sorted order...");
    auto t_write_start = std::chrono::steady_clock::now();

    Pass2Profile pass2_profile;
    Pass2Profile* pass2_profile_ptr = hotpath_profiling_enabled() ? &pass2_profile : nullptr;
    const Status pass2_status = write_sorted_rows(*writer,
                                                  opts,
                                                  pure_numeric_sort,
                                                  numeric_sort_pad_len,
                                                  string_sort_index,
                                                  numeric_sort_index,
                                                  batches,
                                                  &result,
                                                  pass2_profile_ptr);
    if (!pass2_status.ok())
    {
        copy_writer_stats(*writer, converter_budget_at_writer_open, &result);
        update_memory_metrics(budget.get(), &result);
        if (pass2_status.message().find("SORT_ORDER_VIOLATION") != std::string::npos)
        {
            result.error_code = ErrorCode::SORT_VIOLATION;
        }
        else if (pass2_status.message().find("INVALID_ROW_KEY_RULE:") == 0)
        {
            result.error_code = ErrorCode::INVALID_ROW_KEY_RULE;
        }
        else
        {
            // Trusted appends may fail only due to encoder, resource, or I/O
            // errors; continuing could commit a silently incomplete HFile.
            result.error_code = map_status_to_error_code(pass2_status);
        }
        result.error_message = pass2_status.message();
        clog::err("Pass 2 aborted: " + pass2_status.message());
        return result;
    }

    // -- 6. Finish HFile ----
    auto fs = writer->finish();
    if (!fs.ok())
    {
        copy_writer_stats(*writer, converter_budget_at_writer_open, &result);
        update_memory_metrics(budget.get(), &result);
        result.error_code = map_status_to_error_code(fs);
        result.error_message = fs.message();
        clog::err("HFile finish failed: " + fs.message());
        return result;
    }
    copy_writer_stats(*writer, converter_budget_at_writer_open, &result);

    result.write_ms
        = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t_write_start);

    // HFile size
    std::error_code ec;
    auto sz = std::filesystem::file_size(opts.hfile_path, ec);
    if (!ec)
    {
        result.hfile_size_bytes = static_cast<int64_t>(sz);
    }

    result.elapsed_ms
        = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t_start);
    update_memory_metrics(budget.get(), &result);

    log_conversion_result(result, pass2_profile_ptr);

    return result;
}

} // namespace hfile
