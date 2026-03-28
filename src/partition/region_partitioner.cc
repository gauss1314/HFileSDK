#include <hfile/region_partitioner.h>
#include <algorithm>
#include <stdexcept>

namespace hfile {

// ─── SingleRegion ─────────────────────────────────────────────────────────────

class SingleRegionPartitioner final : public RegionPartitioner {
public:
    int region_for(std::span<const uint8_t> /*row_key*/) const noexcept override {
        return 0;
    }
    int num_regions() const noexcept override { return 1; }
    const std::vector<std::vector<uint8_t>>& split_points() const noexcept override {
        return splits_;
    }
private:
    std::vector<std::vector<uint8_t>> splits_; // empty
};

// ─── ManualSplitPartitioner ───────────────────────────────────────────────────

class ManualSplitPartitioner final : public RegionPartitioner {
public:
    explicit ManualSplitPartitioner(std::vector<std::vector<uint8_t>> splits)
        : splits_(std::move(splits)) {
        // Validate that splits are sorted
        for (size_t i = 1; i < splits_.size(); ++i) {
            if (splits_[i] <= splits_[i - 1])
                throw std::invalid_argument("RegionPartitioner: split points must be sorted");
        }
    }

    int region_for(std::span<const uint8_t> row_key) const noexcept override {
        // Empty row key is invalid in HBase — treat as the very first region.
        // (HBase never stores a cell with a zero-length row key.)
        if (row_key.empty()) return 0;

        // Use a custom comparator to avoid constructing a std::vector on every
        // call — the original code allocated O(key_size) bytes per call, which
        // was the dominant allocation on the hot Bulk Load write path.
        auto cmp = [](const std::vector<uint8_t>& split,
                      std::span<const uint8_t>    key) noexcept -> bool {
            // Returns true if split < key (upper_bound semantics inverted: we
            // use lower_bound with reversed arguments via a wrapper below).
            const size_t lim = std::min(split.size(), key.size());
            const int r = std::memcmp(split.data(), key.data(), lim);
            if (r != 0) return r < 0;
            return split.size() < key.size();
        };

        // std::upper_bound(first, last, value, comp) finds the first element
        // for which comp(value, element) is true, i.e. value < element.
        // We need: first split > row_key.
        // comp(row_key_span, split_vec) → true when row_key < split.
        auto it = std::upper_bound(
            splits_.begin(), splits_.end(), row_key,
            [](std::span<const uint8_t>    key,
               const std::vector<uint8_t>& split) noexcept -> bool {
                const size_t lim = std::min(key.size(), split.size());
                const int r = std::memcmp(key.data(), split.data(), lim);
                if (r != 0) return r < 0;
                return key.size() < split.size();
            });
        return static_cast<int>(it - splits_.begin());
    }

    int num_regions() const noexcept override {
        return static_cast<int>(splits_.size()) + 1;
    }

    const std::vector<std::vector<uint8_t>>& split_points() const noexcept override {
        return splits_;
    }

private:
    std::vector<std::vector<uint8_t>> splits_;
};

// ─── Factory implementations ──────────────────────────────────────────────────

std::unique_ptr<RegionPartitioner> RegionPartitioner::from_splits(
        std::vector<std::vector<uint8_t>> split_points) {
    return std::make_unique<ManualSplitPartitioner>(std::move(split_points));
}

std::unique_ptr<RegionPartitioner> RegionPartitioner::none() {
    return std::make_unique<SingleRegionPartitioner>();
}

} // namespace hfile
