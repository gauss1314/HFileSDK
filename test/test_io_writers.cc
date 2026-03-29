#include <gtest/gtest.h>

#include "io/buffered_writer.h"

#include <filesystem>
#include <fstream>
#include <vector>

using namespace hfile;
using namespace hfile::io;
namespace fs = std::filesystem;

namespace {

fs::path temp_file(const std::string& name) {
    auto path = fs::temp_directory_path() / ("io_writer_" + name + ".bin");
    std::error_code ec;
    fs::remove(path, ec);
    return path;
}

std::vector<uint8_t> read_all(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    return std::vector<uint8_t>(
        std::istreambuf_iterator<char>(in),
        std::istreambuf_iterator<char>());
}

}  // namespace

TEST(IOWriters, BufferedFileWriterFlushesBufferedBytes) {
    auto path = temp_file("flush");
    BufferedFileWriter writer(path.string(), 4);

    std::vector<uint8_t> part1 = {'a', 'b'};
    std::vector<uint8_t> part2 = {'c', 'd', 'e'};
    ASSERT_TRUE(writer.write(part1).ok());
    ASSERT_TRUE(writer.write(part2).ok());
    EXPECT_EQ(writer.position(), 5);
    ASSERT_TRUE(writer.flush().ok());
    ASSERT_TRUE(writer.close().ok());

    auto content = read_all(path);
    ASSERT_EQ(content.size(), 5u);
    EXPECT_EQ(content[0], 'a');
    EXPECT_EQ(content[4], 'e');
    fs::remove(path);
}

TEST(IOWriters, BlockWriterFactoryCreatesWorkingFileWriter) {
    auto path = temp_file("factory");
    auto writer = BlockWriter::open_file(path.string(), 8);
    ASSERT_NE(writer, nullptr);

    std::vector<uint8_t> payload = {'h', 'f', 'i', 'l', 'e'};
    ASSERT_TRUE(writer->write(payload).ok());
    EXPECT_EQ(writer->position(), 5);
    ASSERT_TRUE(writer->close().ok());

    auto content = read_all(path);
    EXPECT_EQ(content, payload);
    fs::remove(path);
}

TEST(IOWriters, BufferedWriterCloseIsIdempotent) {
    auto path = temp_file("close_twice");
    BufferedFileWriter writer(path.string(), 16);
    std::vector<uint8_t> payload = {'x'};
    ASSERT_TRUE(writer.write(payload).ok());
    ASSERT_TRUE(writer.close().ok());
    ASSERT_TRUE(writer.close().ok());
    fs::remove(path);
}
