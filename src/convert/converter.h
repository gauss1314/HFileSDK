#pragma once

#include "convert_options.h"

namespace hfile {

/// Convert an Arrow IPC Stream file to a single HFile v3 file.
///
/// Data flow:
///   1. Open Arrow IPC Stream file, read Schema
///   2. Compile rowKeyRule → RowKeyBuilder
///   3. First pass: scan all batches, build rowValue strings, generate row keys
///      → collect SortEntry{rowKey, batchIdx, rowIdx}
///   4. Sort all entries by HBase key order (Row ASC)
///   5. Second pass: replay in sorted order, encode each row's columns as KVs
///   6. AtomicFileWriter commit (fsync + rename)
///
/// This function is the main entry point called by the JNI layer.
ConvertResult convert(const ConvertOptions& opts);

} // namespace hfile
