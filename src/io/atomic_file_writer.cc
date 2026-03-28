#include "atomic_file_writer.h"
#include <cerrno>
#include <cstring>
#include <cstdio>
#include <stdexcept>
#include <random>
#include <sstream>
#include <iomanip>

#if !defined(_WIN32) && !defined(_WIN64)
#  include <unistd.h>   // fsync, open
#  include <fcntl.h>
#endif

namespace hfile {
namespace io {

namespace fs = std::filesystem;

// ─── Temp path generation ─────────────────────────────────────────────────────

static std::string generate_uuid_hex() {
    // 16 random bytes → 32 hex chars, good enough for a unique temp suffix
    std::random_device rd;
    std::mt19937_64    rng(rd());
    uint64_t hi = rng(), lo = rng();
    std::ostringstream oss;
    oss << std::hex << std::setfill('0')
        << std::setw(16) << hi
        << std::setw(16) << lo;
    return oss.str();
}

std::string AtomicFileWriter::make_temp_path(const std::string& final_path) {
    fs::path fp{final_path};
    fs::path tmp_dir = fp.parent_path() / ".tmp";
    std::error_code ec;
    fs::create_directories(tmp_dir, ec);
    // basename: uuid_<filename>.tmp
    std::string tmp_name = generate_uuid_hex() + "_" + fp.filename().string() + ".tmp";
    return (tmp_dir / tmp_name).string();
}

// ─── Constructor / Destructor ─────────────────────────────────────────────────

AtomicFileWriter::AtomicFileWriter(const std::string& final_path,
                                    size_t buffer_size)
    : final_path_{final_path}
    , temp_path_{make_temp_path(final_path)} {
    inner_ = std::make_unique<BufferedFileWriter>(temp_path_, buffer_size);
}

AtomicFileWriter::~AtomicFileWriter() {
    if (!committed_ && !closed_) {
        abort();
    }
}

// ─── BlockWriter interface ────────────────────────────────────────────────────

Status AtomicFileWriter::write(std::span<const uint8_t> data) {
    return inner_->write(data);
}

Status AtomicFileWriter::flush() {
    return inner_->flush();
}

Status AtomicFileWriter::close() {
    if (closed_) return Status::OK();
    auto s = inner_->close();
    closed_ = true;
    return s;
}

int64_t AtomicFileWriter::position() const noexcept {
    return inner_ ? inner_->position() : 0;
}

// ─── commit ──────────────────────────────────────────────────────────────────

Status AtomicFileWriter::commit() {
    if (committed_) return Status::OK();

    // 1. Flush app buffer → OS buffer, then fsync temp file
    HFILE_RETURN_IF_ERROR(inner_->flush());
    HFILE_RETURN_IF_ERROR(inner_->close());
    closed_ = true;

    // 2. fsync the .tmp directory so the temp file's directory entry is durable
    fs::path tmp_dir = fs::path(temp_path_).parent_path();
    HFILE_RETURN_IF_ERROR(fsync_directory(tmp_dir));

    // 3. rename temp → final (atomic on POSIX)
    std::error_code ec;
    fs::rename(temp_path_, final_path_, ec);
    if (ec)
        return Status::IoError("rename failed: " + temp_path_ + " → " +
                               final_path_ + ": " + ec.message());

    // 4. fsync the final directory so the rename persists
    fs::path final_dir = fs::path(final_path_).parent_path();
    HFILE_RETURN_IF_ERROR(fsync_directory(final_dir));

    committed_ = true;
    return Status::OK();
}

// ─── abort ───────────────────────────────────────────────────────────────────

void AtomicFileWriter::abort() noexcept {
    if (!closed_) {
        inner_->close();  // best-effort, ignore error
        closed_ = true;
    }
    std::error_code ec;
    fs::remove(temp_path_, ec);  // best-effort delete of temp file
}

// ─── fsync directory helper ───────────────────────────────────────────────────

Status AtomicFileWriter::fsync_directory(const fs::path& dir) noexcept {
#if defined(_WIN32) || defined(_WIN64)
    // On Windows, flushing the directory is handled automatically by the FS.
    (void)dir;
    return Status::OK();
#else
    // Open the directory itself as a file descriptor, then fsync it.
    // This makes the rename / create visible across a power failure.
    int fd = ::open(dir.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return Status::IoError(std::string("open dir for fsync: ") +
                               dir.string() + ": " + std::strerror(errno));
    if (::fsync(fd) < 0) {
        int saved = errno;
        ::close(fd);
        return Status::IoError(std::string("fsync dir: ") +
                               dir.string() + ": " + std::strerror(saved));
    }
    ::close(fd);
    return Status::OK();
#endif
}

} // namespace io
} // namespace hfile
