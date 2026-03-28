#pragma once

#include <hfile/status.h>
#include <span>
#include <cstdint>
#include <cstddef>
#include <vector>
#include <string>
#include <memory>
#include <cstdio>  // FILE*, fopen, fwrite, fclose, fflush — all standard C

namespace hfile {
namespace io {

/// Abstract block writer (I/O backend abstraction).
class BlockWriter {
public:
    virtual ~BlockWriter() = default;

    virtual Status write(std::span<const uint8_t> data) = 0;
    virtual Status flush()                               = 0;
    virtual Status close()                               = 0;
    virtual int64_t position() const noexcept            = 0;

    static std::unique_ptr<BlockWriter> open_file(const std::string& path,
                                                   size_t buffer_size = 4 * 1024 * 1024);
};

/// Buffered file writer — cross-platform implementation using standard C FILE*.
///
/// Uses an application-level write buffer (default 4 MB) on top of FILE* so
/// that small writes are coalesced before hitting the OS.  fflush() is called
/// on flush(); fclose() on close().
///
/// Platform support:
///   Linux / macOS   — FILE* backed by POSIX fd; fsync via fileno() optional.
///   Windows (MSVC)  — FILE* backed by Win32 HANDLE; fully supported.
///   Windows (Clang) — identical to MSVC path; supported.
class BufferedFileWriter final : public BlockWriter {
public:
    explicit BufferedFileWriter(const std::string& path,
                                size_t buffer_size = 4 * 1024 * 1024);
    ~BufferedFileWriter() override;

    Status write(std::span<const uint8_t> data) override;
    Status flush() override;
    Status close() override;
    int64_t position() const noexcept override { return pos_; }

private:
    Status drain();  // flush app buffer → FILE*

    std::FILE*           file_{nullptr};
    int64_t              pos_{0};
    std::vector<uint8_t> buf_;
    size_t               buf_used_{0};
    size_t               buf_cap_;
};

} // namespace io
} // namespace hfile
