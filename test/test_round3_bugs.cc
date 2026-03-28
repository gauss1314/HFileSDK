#include "hfile/types.h"
#include "hfile/writer.h"
#include "hfile/writer_options.h"
#include "hfile/region_partitioner.h"
#include <cassert>
#include <cstdio>
#include <cstring>
#include <vector>
#include <filesystem>

using namespace hfile;
namespace fs = std::filesystem;

static int TESTS = 0, PASSED = 0;
#define EXPECT(c) do{++TESTS;if(c){++PASSED;}else{\
    fprintf(stderr,"  FAIL line %d: %s\n",__LINE__,#c);}}while(0)
#define EXPECT_EQ(a,b) EXPECT((a)==(b))
#define EXPECT_LT(a,b) EXPECT((a)<(b))

// ─── VarInt ──────────────────────────────────────────────────────────────────

void test_varint_roundtrip() {
    for (uint64_t v : std::vector<uint64_t>{0,1,127,128,300,65535,(1ULL<<35),UINT64_MAX}) {
        uint8_t buf[10]; int n = encode_varint64(buf, v);
        EXPECT(n >= 1 && n <= 10);
        uint64_t out = 0; int r = decode_varint64(buf, out);
        EXPECT(r >= 1); EXPECT_EQ(out, v);
    }
}

void test_varint_exactly_10_bytes() {
    uint8_t buf[10]; int n = encode_varint64(buf, UINT64_MAX);
    EXPECT_EQ(n, 10);
    uint64_t out = 0; int r = decode_varint64(buf, out);
    EXPECT_EQ(r, 10); EXPECT_EQ(out, UINT64_MAX);
}

void test_varint_malformed_returns_neg1() {
    // 11 bytes all with continuation bit set → loop must terminate at 10, return -1
    uint8_t bad[11]; std::memset(bad, 0x80, 11);
    uint64_t out = 0;
    int r = decode_varint64(bad, out);
    EXPECT_EQ(r, -1);
}

// ─── RegionPartitioner ───────────────────────────────────────────────────────

void test_region_empty_key_no_crash() {
    auto p = RegionPartitioner::from_splits({{'m'}});
    std::span<const uint8_t> empty{};
    EXPECT_EQ(p->region_for(empty), 0);  // empty < all splits → region 0
}

void test_region_for_correctness() {
    auto p = RegionPartitioner::from_splits({{'d'},{'h'},{'n'},{'t'}});
    auto k = [](char c){ return std::vector<uint8_t>{static_cast<uint8_t>(c)}; };
    EXPECT_EQ(p->region_for(k('a')), 0);
    EXPECT_EQ(p->region_for(k('d')), 1);
    EXPECT_EQ(p->region_for(k('e')), 1);
    EXPECT_EQ(p->region_for(k('h')), 2);
    EXPECT_EQ(p->region_for(k('n')), 3);
    EXPECT_EQ(p->region_for(k('t')), 4);
    EXPECT_EQ(p->region_for(k('z')), 4);
}

void test_region_binary_keys() {
    std::vector<std::vector<uint8_t>> splits;
    for (int i = 1; i < 10; ++i) {
        uint8_t k[4]; write_be32(k, static_cast<uint32_t>(i*1000));
        splits.emplace_back(k, k+4);
    }
    auto p = RegionPartitioner::from_splits(splits);
    uint8_t k0[4]; write_be32(k0, 0);    EXPECT_EQ(p->region_for({k0,4}), 0);
    uint8_t k5[4]; write_be32(k5, 5000); EXPECT_EQ(p->region_for({k5,4}), 5);
    uint8_t kx[4]; write_be32(kx, 99999);EXPECT_EQ(p->region_for({kx,4}), 9);
}

// ─── compare_keys empty spans ─────────────────────────────────────────────────

void test_compare_empty_spans() {
    KeyValue a{}, b{};
    a.timestamp = b.timestamp = 0;
    a.key_type = b.key_type = KeyType::Put;
    EXPECT_EQ(compare_keys(a, b), 0);         // both empty → equal

    std::vector<uint8_t> r = {'x'};
    b.row = r;
    EXPECT_LT(compare_keys(a, b), 0);         // empty row < non-empty
}

// ─── finish() / destructor cleanup ───────────────────────────────────────────

static KeyValue make_kv(std::vector<uint8_t>& rk, std::vector<uint8_t>& fam,
                         std::vector<uint8_t>& q, std::vector<uint8_t>& v) {
    rk={'r'}; fam={'c','f'}; q={'c'}; v={'v'};
    KeyValue kv; kv.row=rk; kv.family=fam; kv.qualifier=q;
    kv.timestamp=1; kv.key_type=KeyType::Put; kv.value=v;
    return kv;
}

void test_successful_finish_file_persists() {
    auto path = fs::temp_directory_path() / "test_ok_finish.hfile";
    {
        auto [w, s] = HFileWriter::builder()
            .set_path(path.string()).set_column_family("cf")
            .set_compression(Compression::None)
            .set_data_block_encoding(Encoding::None)
            .set_bloom_type(BloomType::None).build();
        EXPECT(s.ok());
        std::vector<uint8_t> rk,fam,q,v;
        EXPECT(w->append(make_kv(rk,fam,q,v)).ok());
        EXPECT(w->finish().ok());
    }
    EXPECT(fs::exists(path));   // file must survive — finish() succeeded
    fs::remove(path);
}

void test_unfinished_writer_deletes_partial_file() {
    auto path = fs::temp_directory_path() / "test_partial.hfile";
    {
        auto [w, s] = HFileWriter::builder()
            .set_path(path.string()).set_column_family("cf")
            .set_compression(Compression::None)
            .set_data_block_encoding(Encoding::None)
            .set_bloom_type(BloomType::None).build();
        EXPECT(s.ok());
        std::vector<uint8_t> rk,fam,q,v;
        EXPECT(w->append(make_kv(rk,fam,q,v)).ok());
        // Deliberately skip finish()
    } // destructor fires: opened=true, finished=false → deletes file
    EXPECT(!fs::exists(path));   // partial file must be gone
}

void test_double_finish_idempotent() {
    auto path = fs::temp_directory_path() / "test_double_finish.hfile";
    {
        auto [w, s] = HFileWriter::builder()
            .set_path(path.string()).set_column_family("cf")
            .set_compression(Compression::None)
            .set_data_block_encoding(Encoding::None)
            .set_bloom_type(BloomType::None).build();
        EXPECT(s.ok());
        EXPECT(w->finish().ok());
        EXPECT(w->finish().ok());  // second call must be a no-op
    }
    EXPECT(fs::exists(path));
    fs::remove(path);
}

int main() {
    printf("\n=== Round-3 bug fix tests ===\n\n");
    test_varint_roundtrip();
    test_varint_exactly_10_bytes();
    test_varint_malformed_returns_neg1();
    test_region_empty_key_no_crash();
    test_region_for_correctness();
    test_region_binary_keys();
    test_compare_empty_spans();
    test_successful_finish_file_persists();
    test_unfinished_writer_deletes_partial_file();
    test_double_finish_idempotent();
    printf("Tests run: %d  Passed: %d  Failed: %d\n\n",
           TESTS, PASSED, TESTS-PASSED);
    return (PASSED==TESTS) ? 0 : 1;
}
