#include "iouring_writer.h"

#ifdef HFILE_HAS_IO_URING

#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <liburing.h>

namespace hfile {
namespace io {

IoUringWriter::IoUringWriter(const std::string& path,
                             size_t             buffer_size,
                             unsigned           ring_depth)
    : ring_depth_{ring_depth}, buf_cap_{buffer_size} {

    // Initialise both buffers
    buf_[0].resize(buffer_size);
    buf_[1].resize(buffer_size);
    buf_used_[0] = buf_used_[1] = 0;
    cur_ = 0;

    fd_ = ::open(path.c_str(),
                 O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC | O_DIRECT, 0644);
    if (fd_ < 0)
        fd_ = ::open(path.c_str(),
                     O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd_ < 0)
        throw std::runtime_error(std::string("IoUringWriter: cannot open: ") + path);

    if (io_uring_queue_init(ring_depth, &ring_, 0) < 0) {
        ::close(fd_);
        fd_ = -1;
        throw std::runtime_error("IoUringWriter: io_uring_queue_init failed");
    }
    ring_initialized_ = true;
}

IoUringWriter::~IoUringWriter() {
    if (fd_ >= 0) {
        auto s = flush();
        if (!s.ok())
            std::fprintf(stderr, "[ERROR] iouring_writer: %s\n", s.message().c_str());
        ::close(fd_);
        fd_ = -1;
    }
    if (ring_initialized_) {
        io_uring_queue_exit(&ring_);
        ring_initialized_ = false;
    }
}

Status IoUringWriter::write(std::span<const uint8_t> data) {
    const uint8_t* src = data.data();
    size_t         rem = data.size();

    while (rem > 0) {
        size_t space = buf_cap_ - buf_used_[cur_];
        if (space == 0) {
            HFILE_RETURN_IF_ERROR(submit_active());
            space = buf_cap_;
        }
        size_t copy = std::min(rem, space);
        std::memcpy(buf_[cur_].data() + buf_used_[cur_], src, copy);
        buf_used_[cur_] += copy;
        src              += copy;
        rem              -= copy;
        pos_             += static_cast<int64_t>(copy);
    }
    return Status::OK();
}

// submit_active: submit the current active buffer, then flip to the other one.
//
// Protocol to avoid data corruption:
//   1. If the inactive buffer still has an in-flight SQE (in_flight_ == 1),
//      wait for it to complete first.  This ensures the inactive buffer is
//      fully written to disk before we repurpose the active one.
//   2. Submit an async write SQE pointing to buf_[cur_].
//   3. Flip cur_ so subsequent write() calls go to the other (now free) buffer.
//
// At any moment: at most ONE SQE is in-flight, and it points to the
// INACTIVE buffer.  The ACTIVE buffer is never referenced by a pending SQE.
Status IoUringWriter::submit_active() {
    if (buf_used_[cur_] == 0) return Status::OK();

    // Step 1: wait for the previous in-flight write to complete so that the
    // buffer we are about to flip INTO is safe to overwrite.
    if (in_flight_ > 0)
        HFILE_RETURN_IF_ERROR(drain_completions(true));

    // Step 2: get an SQE and submit
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) return Status::Internal("IoUringWriter: no SQE slot available");

    const int64_t write_offset = pos_ - static_cast<int64_t>(buf_used_[cur_]);
    io_uring_prep_write(sqe, fd_,
                        buf_[cur_].data(),
                        static_cast<unsigned>(buf_used_[cur_]),
                        static_cast<uint64_t>(write_offset));
    // Tag with buffer index so the completion handler can verify
    io_uring_sqe_set_data64(sqe, static_cast<uint64_t>(cur_));

    if (io_uring_submit(&ring_) < 0)
        return Status::IoError(std::string("io_uring_submit failed: ")
                               + std::strerror(errno));
    ++in_flight_;
    buf_used_[cur_] = 0;

    // Step 3: flip to the other buffer
    cur_ ^= 1;
    return Status::OK();
}

Status IoUringWriter::drain_completions(bool wait_all) {
    while (in_flight_ > 0) {
        io_uring_cqe* cqe = nullptr;
        int r = wait_all
            ? io_uring_wait_cqe(&ring_, &cqe)
            : io_uring_peek_cqe(&ring_, &cqe);
        if (r == -EAGAIN) break;
        if (r < 0)
            return Status::IoError(std::string("io_uring CQE error: ")
                                   + std::strerror(-r));
        if (cqe->res < 0)
            return Status::IoError(std::string("async write failed: ")
                                   + std::strerror(-cqe->res));
        io_uring_cqe_seen(&ring_, cqe);
        --in_flight_;
    }
    return Status::OK();
}

Status IoUringWriter::flush() {
    // Submit whatever is in the active buffer, then wait for all completions.
    HFILE_RETURN_IF_ERROR(submit_active());
    return drain_completions(true);
}

Status IoUringWriter::close() {
    HFILE_RETURN_IF_ERROR(flush());
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
    return Status::OK();
}

} // namespace io
} // namespace hfile

#endif // HFILE_HAS_IO_URING
