#include "data_block_encoder.h"
#include "none_encoder.h"
#include "prefix_encoder.h"
#include "diff_encoder.h"
#include "fast_diff_encoder.h"

namespace hfile {
namespace block {

std::unique_ptr<DataBlockEncoder> DataBlockEncoder::create(Encoding enc, size_t block_size) {
    switch (enc) {
    case Encoding::None:     return std::make_unique<NoneEncoder>(block_size);
    case Encoding::Prefix:   return std::make_unique<PrefixEncoder>(block_size);
    case Encoding::Diff:     return std::make_unique<DiffEncoder>(block_size);
    case Encoding::FastDiff: return std::make_unique<FastDiffEncoder>(block_size);
    }
    return std::make_unique<NoneEncoder>(block_size);
}

} // namespace block
} // namespace hfile
