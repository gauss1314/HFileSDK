#pragma once

#include <hfile/types.h>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <functional>
#include <set>
#include <span>

namespace hfile {
namespace partition {

/// Routes incoming KeyValues to the correct (cf, region) bucket.
/// Each bucket maps to one output HFile.
class CFGrouper {
public:
    using BucketKey = std::pair<std::string, int>;  // (cf_name, region_idx)

    struct BucketKeyHash {
        size_t operator()(const BucketKey& k) const noexcept {
            size_t h1 = std::hash<std::string>{}(k.first);
            size_t h2 = std::hash<int>{}(k.second);
            return h1 ^ (h2 << 32) ^ (h2 >> 32);
        }
    };

    /// Register a known column family. Only registered CFs are accepted.
    void register_cf(std::string cf_name) {
        known_cfs_.insert(std::move(cf_name));
    }

    /// Returns true if the family is known.
    bool has_cf(std::string_view family) const noexcept {
        return known_cfs_.count(std::string(family)) > 0;
    }

    /// Validate that a KV's family is registered.
    bool validate_family(const KeyValue& kv) const noexcept {
        std::string_view fam(
            reinterpret_cast<const char*>(kv.family.data()), kv.family.size());
        return has_cf(fam);
    }

    const std::vector<std::string>& known_families() const noexcept {
        // Return sorted list for deterministic iteration
        if (cf_list_.empty() && !known_cfs_.empty()) {
            cf_list_.assign(known_cfs_.begin(), known_cfs_.end());
            std::sort(cf_list_.begin(), cf_list_.end());
        }
        return cf_list_;
    }

    void rebuild_list() {
        cf_list_.assign(known_cfs_.begin(), known_cfs_.end());
        std::sort(cf_list_.begin(), cf_list_.end());
    }

private:
    std::set<std::string>     known_cfs_;
    mutable std::vector<std::string> cf_list_;
};

} // namespace partition
} // namespace hfile
