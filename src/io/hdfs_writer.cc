#include "hdfs_writer.h"

#ifdef HFILE_HAS_HDFS

#include <cstdio>
#include <stdexcept>
#include <cstring>
#include <cerrno>

namespace hfile {
namespace io {

HdfsWriter::HdfsWriter(const std::string& namenode_uri,
                       const std::string& hdfs_path,
                       size_t             buffer_size,
                       short              replication,
                       uint32_t           block_size)
    : buf_cap_{buffer_size} {
    buf_.resize(buffer_size);

    struct hdfsBuilder* builder = hdfsNewBuilder();
    hdfsBuilderSetNameNode(builder, namenode_uri.c_str());
    fs_ = hdfsBuilderConnect(builder);
    if (!fs_)
        throw std::runtime_error("Cannot connect to HDFS: " + namenode_uri);

    file_ = hdfsOpenFile(fs_, hdfs_path.c_str(),
                         O_WRONLY | O_CREAT,
                         static_cast<int>(buffer_size),
                         replication,
                         static_cast<tOffset>(block_size));
    if (!file_)
        throw std::runtime_error("Cannot open HDFS file: " + hdfs_path);
}

HdfsWriter::~HdfsWriter() {
    if (file_) {
        auto s = drain();
        if (!s.ok())
            std::fprintf(stderr, "[ERROR] hdfs_writer: %s\n", s.message().c_str());
        if (hdfsCloseFile(fs_, file_) != 0)
            std::fprintf(stderr, "[ERROR] hdfs_writer: close failed\n");
    }
    if (fs_) hdfsDisconnect(fs_);
}

Status HdfsWriter::write(std::span<const uint8_t> data) {
    const uint8_t* src = data.data();
    size_t         rem = data.size();
    while (rem > 0) {
        size_t space = buf_cap_ - buf_used_;
        if (space == 0) HFILE_RETURN_IF_ERROR(drain());
        size_t copy = std::min(rem, buf_cap_ - buf_used_);
        std::memcpy(buf_.data() + buf_used_, src, copy);
        buf_used_ += copy;
        src       += copy;
        rem       -= copy;
        pos_      += static_cast<int64_t>(copy);
    }
    return Status::OK();
}

Status HdfsWriter::drain() {
    if (buf_used_ == 0) return Status::OK();
    const uint8_t* p   = buf_.data();
    size_t         rem = buf_used_;
    while (rem > 0) {
        tSize n = hdfsWrite(fs_, file_, p, static_cast<tSize>(rem));
        if (n < 0)
            return Status::IoError("HDFS write failed");
        p   += n;
        rem -= static_cast<size_t>(n);
    }
    buf_used_ = 0;
    return Status::OK();
}

Status HdfsWriter::flush() {
    HFILE_RETURN_IF_ERROR(drain());
    if (hdfsHFlush(fs_, file_) < 0)
        return Status::IoError("HDFS hflush failed");
    return Status::OK();
}

Status HdfsWriter::close() {
    HFILE_RETURN_IF_ERROR(drain());
    if (file_) {
        if (hdfsCloseFile(fs_, file_) < 0)
            return Status::IoError("HDFS close failed");
        file_ = nullptr;
    }
    return Status::OK();
}

} // namespace io
} // namespace hfile

#endif // HFILE_HAS_HDFS
