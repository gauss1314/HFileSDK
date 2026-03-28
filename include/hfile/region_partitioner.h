#pragma once

#include "types.h"
#include "status.h"
#include <vector>
#include <string>
#include <span>
#include <memory>
#include <optional>

namespace hfile {

/// Determines which Region (HFile) a Row Key belongs to.
/// Splits are stored as byte boundaries: [start, splits[0]), [splits[0], splits[1]), …
class RegionPartitioner {
public:
    virtual ~RegionPartitioner() = default;

    /// Returns 0-based region index for the given row key.
    virtual int region_for(std::span<const uint8_t> row_key) const noexcept = 0;

    /// Number of regions (== number of output HFiles per CF).
    virtual int num_regions() const noexcept = 0;

    /// Returns the split points (boundaries between regions).
    virtual const std::vector<std::vector<uint8_t>>& split_points() const noexcept = 0;

    // ─── Factory methods ──────────────────────────────────────────────────────

    /// Manual offline mode: caller supplies split points.
    /// split_points must be sorted ascending; empty → single region.
    ///
    /// How to obtain split points before calling:
    ///   hbase shell>  get_splits 'my_table'
    ///   REST API:     GET /my_table/regions
    ///   Java Admin:   admin.getRegions(TableName.valueOf("my_table"))
    ///                 .stream().map(r -> r.getStartKey()).collect(toList())
    static std::unique_ptr<RegionPartitioner> from_splits(
        std::vector<std::vector<uint8_t>> split_points);

    /// Single-region mode (no splitting). BulkLoadHFilesTool will split files
    /// at Region boundaries during load. Suitable when split points are unknown
    /// or the table has few Regions.
    static std::unique_ptr<RegionPartitioner> none();

    // ── Design note: no from_hbase() / online query mode ─────────────────────
    //
    // An online ZooKeeper/Meta query is intentionally NOT provided because:
    //
    //  1. Separation of concerns: querying cluster topology is a data-preparation
    //     step, not a write-path concern.  Mixing live cluster I/O into the write
    //     path would couple latency of the ZooKeeper ensemble to write throughput.
    //
    //  2. Dependency weight: a C++ ZooKeeper/HBase client (libhbase, JNI, or
    //     Thrift) would add a heavy dependency that most embedding environments
    //     neither need nor want.
    //
    //  3. Resilience: the write path can make forward progress even when the
    //     HBase cluster is temporarily unavailable; an online query would block
    //     or fail in that scenario.
    //
    // Recommended workflow:
    //   - Query splits once before the write job starts (shell, REST, or Java).
    //   - Pass them to from_splits().
    //   - If splits are truly unknown, use none() and let BulkLoadHFilesTool
    //     handle the re-split during load (slower but always correct).
};

} // namespace hfile
