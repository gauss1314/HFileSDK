#include "data_block_encoder.h"
#include "none_encoder.h"

namespace hfile {
namespace block {

std::unique_ptr<DataBlockEncoder> DataBlockEncoder::create(Encoding enc, size_t block_size) {
    (void)enc;
    return std::make_unique<NoneEncoder>(block_size);
}

} // namespace block
} // namespace hfile
