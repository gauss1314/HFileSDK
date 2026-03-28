#pragma once

#include "buffered_writer.h"
#include <string>

namespace hfile {
namespace io {

#ifdef HFILE_HAS_HDFS

#include <hdfs/hdfs.h>

/// libhdfs3-based writer: writes directly to HDFS.
/// Internally buffers writes into a large buffer before flushing to HDFS
/// to minimise RPC round-trips.
class HdfsWriter final : public BlockWriter {
public:
    HdfsWriter(const std::string& namenode_uri,
               const std::string& hdfs_path,
               size_t             buffer_size = 8 * 1024 * 1024,
               short              replication = 3,
               uint32_t           block_size  = 128 * 1024 * 1024);
    ~HdfsWriter() override;

    Status write(std::span<const uint8_t> data) override;
    Status flush() override;
    Status close() override;
    int64_t position() const noexcept override { return pos_; }

private:
    Status drain();

    hdfsFS              fs_{nullptr};
    hdfsFile            file_{nullptr};
    int64_t             pos_{0};
    std::vector<uint8_t> buf_;
    size_t               buf_used_{0};
    size_t               buf_cap_;
};

#endif // HFILE_HAS_HDFS

} // namespace io
} // namespace hfile
