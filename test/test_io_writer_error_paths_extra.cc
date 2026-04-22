#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#define private public
#include "io/atomic_file_writer.h"
#include "io/buffered_writer.h"
#undef private

using namespace hfile;
using namespace hfile::io;
namespace fs = std::filesystem;

namespace {

fs::path temp_file(const std::string& name) {
    auto path = fs::temp_directory_path() / ("io_writer_extra_" + name + ".bin");
    std::error_code ec;
    fs::remove(path, ec);
    fs::remove_all(path.parent_path() / ".tmp", ec);
    return path;
}

fs::path temp_dir(const std::string& name) {
    auto dir = fs::temp_directory_path() / ("io_writer_extra_dir_" + name);
    std::error_code ec;
    fs::remove_all(dir, ec);
    fs::create_directories(dir, ec);
    return dir;
}

std::vector<uint8_t> read_all(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    return std::vector<uint8_t>(
        std::istreambuf_iterator<char>(in),
        std::istreambuf_iterator<char>());
}

}  // namespace

TEST(IOWritersExtra, BufferedWriterRejectsDirectoryPath) {
    auto dir = temp_dir("ctor_dir");
    EXPECT_THROW(
        {
            BufferedFileWriter writer(dir.string(), 16);
        },
        std::runtime_error);
    fs::remove_all(dir);
}

TEST(IOWritersExtra, AtomicWriterFlushAndCommitAreIdempotent) {
    auto path = temp_file("flush_commit");
    AtomicFileWriter writer(path.string(), 8);

    std::vector<uint8_t> payload = {'o', 'k'};
    ASSERT_TRUE(writer.write(payload).ok());
    ASSERT_TRUE(writer.flush().ok());
    ASSERT_TRUE(writer.commit().ok());
    ASSERT_TRUE(writer.commit().ok());

    EXPECT_TRUE(fs::exists(path));
    EXPECT_EQ(read_all(path), payload);
    fs::remove(path);
}

TEST(IOWritersExtra, AtomicWriterCloseIsIdempotent) {
    auto path = temp_file("close");
    AtomicFileWriter writer(path.string(), 8);

    std::vector<uint8_t> payload = {'c', 'l', 'o', 's', 'e'};
    ASSERT_TRUE(writer.write(payload).ok());
    const auto tmp_path = writer.temp_path();

    ASSERT_TRUE(writer.close().ok());
    ASSERT_TRUE(writer.close().ok());
    EXPECT_TRUE(fs::exists(tmp_path));

    std::error_code ec;
    fs::remove(tmp_path, ec);
    fs::remove_all(path.parent_path() / ".tmp", ec);
}

TEST(IOWritersExtra, AtomicWriterCommitReportsRenameFailure) {
    auto dir = temp_dir("rename_failure");
    auto final_dir = dir / "already_a_directory";
    std::error_code ec;
    fs::create_directories(final_dir, ec);
    ASSERT_FALSE(ec);

    AtomicFileWriter writer(final_dir.string(), 8);
    std::vector<uint8_t> payload = {'n', 'o', 'p', 'e'};
    ASSERT_TRUE(writer.write(payload).ok());

    auto status = writer.commit();
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.message().find("rename failed"), std::string::npos);
    EXPECT_TRUE(fs::exists(writer.temp_path()));

    fs::remove(writer.temp_path(), ec);
    fs::remove_all(dir, ec);
}

TEST(IOWritersExtra, FsyncDirectoryReportsMissingDirectory) {
    auto dir = temp_dir("missing_dir");
    auto missing = dir / "does_not_exist";

    auto status = AtomicFileWriter::fsync_directory(missing);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.message().find("open dir for fsync"), std::string::npos);

    fs::remove_all(dir);
}
