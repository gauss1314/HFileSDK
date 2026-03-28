#pragma once

#include "buffered_writer.h"
#include <string>
#include <array>

namespace hfile {
namespace io {

#ifdef HFILE_HAS_IO_URING

#include <liburing.h>

/// io_uring-based writer with double-buffer rotation.
///
/// Two equal-size buffers (buf_[0] and buf_[1]) are used alternately:
///   - The "active" buffer (cur_ index) receives new write() data.
///   - When the active buffer is full, submit_pending() is called:
///       1. Wait for the previous in-flight SQE to complete (if any).
///       2. Submit an async write SQE pointing to the now-full active buffer.
///       3. Flip cur_ to the other buffer.
///   - The in-flight SQE always points to the "inactive" buffer, which
///     the writer thread never touches until the SQE completes.
///
/// This eliminates the data-corruption window present in a single-buffer
/// design where buf_used_ is reset while the SQE still references buf_.
class IoUringWriter final : public BlockWriter {
public:
    explicit IoUringWriter(const std::string& path,
                           size_t             buffer_size    = 4 * 1024 * 1024,
                           unsigned           ring_depth     = 4);
    ~IoUringWriter() override;

    Status write(std::span<const uint8_t> data) override;
    Status flush() override;
    Status close() override;
    int64_t position() const noexcept override { return pos_; }

private:
    Status submit_active();                 // submit buf_[cur_] and flip
    Status drain_completions(bool wait_all);

    int     fd_{-1};
    int64_t pos_{0};

    io_uring ring_{};
    bool     ring_initialized_{false};
    unsigned in_flight_{0};
    unsigned ring_depth_{4};

    // Double buffers
    std::array<std::vector<uint8_t>, 2> buf_;
    std::array<size_t, 2>               buf_used_{0, 0};
    int                                 cur_{0};   // index of the active buffer
    size_t                              buf_cap_{0};
};

#endif // HFILE_HAS_IO_URING

} // namespace io
} // namespace hfile
