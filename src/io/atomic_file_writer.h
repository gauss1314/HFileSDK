#pragma once

#include "buffered_writer.h"
#include <hfile/status.h>
#include <string>
#include <filesystem>

namespace hfile {
namespace io {

/// Crash-safe file writer: all data is written to a temporary file under
/// <parent_dir>/.tmp/<uuid>_<basename>.tmp, then on commit():
///   1. fsync the temp file (data durability)
///   2. fsync the .tmp directory
///   3. rename temp → final path (atomic on POSIX)
///   4. fsync the final directory (directory entry durability)
///
/// If commit() is never called (constructor throws, finish() fails, or
/// program crashes), the temp file either never existed or is left in .tmp/
/// and will be ignored by BulkLoadHFilesTool — the final output directory
/// is always free of partially-written files.
///
/// HDFS note: HDFS rename is not POSIX-atomic; for HDFS use the path
/// directly and rely on HDFS's own crash recovery.
class AtomicFileWriter final : public BlockWriter {
public:
    /// Open a temp file for writing. The final destination is `final_path`.
    /// Throws std::runtime_error if the temp file cannot be created.
    explicit AtomicFileWriter(const std::string& final_path,
                               size_t buffer_size = 4 * 1024 * 1024);

    ~AtomicFileWriter() override;

    // Non-copyable
    AtomicFileWriter(const AtomicFileWriter&)            = delete;
    AtomicFileWriter& operator=(const AtomicFileWriter&) = delete;

    Status write(std::span<const uint8_t> data) override;
    Status flush() override;       // flush app buffer → OS buffer
    Status close() override;       // close without committing (leaves temp file)
    int64_t position() const noexcept override;

    /// Finalise: flush, fsync temp file, rename to final_path, fsync directories.
    /// After a successful commit() the temp file no longer exists.
    Status commit();

    /// Abort: close and delete the temp file.  Called automatically by the
    /// destructor if commit() was not called.
    void abort() noexcept;

    const std::string& final_path() const noexcept { return final_path_; }
    const std::string& temp_path()  const noexcept { return temp_path_; }

private:
    static std::string make_temp_path(const std::string& final_path);
    static Status fsync_directory(const std::filesystem::path& dir) noexcept;

    std::string                     final_path_;
    std::string                     temp_path_;
    std::unique_ptr<BufferedFileWriter> inner_;
    bool                            committed_{false};
    bool                            closed_{false};
};

} // namespace io
} // namespace hfile
