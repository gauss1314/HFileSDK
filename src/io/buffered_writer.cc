#include "buffered_writer.h"

#include <cerrno>
#include <cstring>
#include <stdexcept>

#if defined(_WIN32) || defined(_WIN64)
#  include <io.h>
#  define HFILE_FSYNC(f) ::_commit(::_fileno(f))
#else
#  include <unistd.h>   // fsync, fileno
#  define HFILE_FSYNC(f) ::fsync(::fileno(f))
#endif

namespace hfile {
namespace io {

BufferedFileWriter::BufferedFileWriter(const std::string& path,
                                       size_t buffer_size)
    : buf_cap_{buffer_size} {
    buf_.resize(buffer_size);

    // "wb" = write binary; creates or truncates; works on all platforms.
    file_ = std::fopen(path.c_str(), "wb");
    if (!file_)
        throw std::runtime_error(
            std::string("BufferedFileWriter: cannot open '") + path +
            "': " + std::strerror(errno));

    // Turn off the C library's own buffering — we manage our own buffer.
    std::setvbuf(file_, nullptr, _IONBF, 0);
}

BufferedFileWriter::~BufferedFileWriter() {
    if (file_) {
        auto s = close();
        if (!s.ok())
            std::fprintf(stderr, "[ERROR] buffered_writer: %s\n", s.message().c_str());
    }
}

Status BufferedFileWriter::write(std::span<const uint8_t> data) {
    const uint8_t* src = data.data();
    size_t         rem = data.size();

    while (rem > 0) {
        size_t space = buf_cap_ - buf_used_;
        if (space == 0) {
            HFILE_RETURN_IF_ERROR(drain());
            space = buf_cap_;
        }
        size_t copy = std::min(rem, space);
        std::memcpy(buf_.data() + buf_used_, src, copy);
        buf_used_ += copy;
        src        += copy;
        rem        -= copy;
        pos_       += static_cast<int64_t>(copy);
    }
    return Status::OK();
}

Status BufferedFileWriter::drain() {
    if (buf_used_ == 0) return Status::OK();

    size_t written = std::fwrite(buf_.data(), 1, buf_used_, file_);
    if (written != buf_used_)
        return Status::IoError(
            std::string("fwrite failed: ") + std::strerror(errno));

    buf_used_ = 0;
    return Status::OK();
}

Status BufferedFileWriter::flush() {
    HFILE_RETURN_IF_ERROR(drain());

    if (std::fflush(file_) != 0)
        return Status::IoError(
            std::string("fflush failed: ") + std::strerror(errno));

    // Best-effort fsync (POSIX) / no-op (Windows)
    if (HFILE_FSYNC(file_) != 0)
        return Status::IoError(
            std::string("fsync failed: ") + std::strerror(errno));

    return Status::OK();
}

Status BufferedFileWriter::close() {
    Status drain_status = drain();
    if (file_) {
        if (std::fclose(file_) != 0)
            return Status::IoError(
                std::string("fclose failed: ") + std::strerror(errno));
        file_ = nullptr;
    }
    if (!drain_status.ok()) return drain_status;
    return Status::OK();
}

// ─── Factory ─────────────────────────────────────────────────────────────────

std::unique_ptr<BlockWriter> BlockWriter::open_file(const std::string& path,
                                                     size_t buffer_size) {
    return std::make_unique<BufferedFileWriter>(path, buffer_size);
}

} // namespace io
} // namespace hfile
