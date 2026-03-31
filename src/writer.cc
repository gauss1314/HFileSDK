#include <hfile/writer.h>

#include "block/data_block_encoder.h"
#include "block/none_encoder.h"
#include "block/fast_diff_encoder.h"
#include "codec/compressor.h"
#include "checksum/crc32c.h"
#include "index/block_index_writer.h"
#include "bloom/compound_bloom_filter_writer.h"
#include "meta/file_info_builder.h"
#include "meta/trailer_builder.h"
#include "io/buffered_writer.h"
#include "io/atomic_file_writer.h"
#include "memory/arena_allocator.h"
#include "memory/memory_budget.h"
#include "metrics/metrics_registry.h"

#include <cassert>
#include <cstring>
#include <algorithm>
#include <stdexcept>
#include <filesystem>
#include <chrono>
#include <cstdio>   // stderr logging (no external dep)

#if !defined(_WIN32) && !defined(_WIN64)
#  include <sys/statvfs.h>
#endif

namespace hfile {

// ─── Minimal structured logger (no spdlog dep, writes to stderr) ─────────────
// Format: [LEVEL] hfile: message
namespace log {
static void info (const std::string& msg) { fprintf(stderr, "[INFO]  hfile: %s\n", msg.c_str()); }
static void warn (const std::string& msg) { fprintf(stderr, "[WARN]  hfile: %s\n", msg.c_str()); }
static void error(const std::string& msg) { fprintf(stderr, "[ERROR] hfile: %s\n", msg.c_str()); }
} // namespace log

// ─── Disk space helper ────────────────────────────────────────────────────────
static int64_t free_disk_bytes(const std::string& path) {
#if defined(_WIN32) || defined(_WIN64)
    (void)path; return INT64_MAX;   // not implemented on Windows
#else
    namespace fs = std::filesystem;
    fs::path probe{path};
    std::error_code ec;
    if (!fs::exists(probe, ec))
        probe = probe.has_parent_path() ? probe.parent_path() : fs::current_path();
    struct statvfs st{};
    if (::statvfs(probe.c_str(), &st) != 0) return INT64_MAX;  // can't check = don't block
    return static_cast<int64_t>(st.f_bavail) * static_cast<int64_t>(st.f_frsize);
#endif
}

// ─── Input validation helper ──────────────────────────────────────────────────
static Status validate_kv(const KeyValue& kv, const WriterOptions& opts) {
    if (kv.row.empty())
        return Status::InvalidArg("ROW_KEY_EMPTY: row key must not be empty");
    if (kv.row.size() > opts.max_row_key_bytes)
        return Status::InvalidArg(
            "ROW_KEY_TOO_LONG: " + std::to_string(kv.row.size()) +
            " > " + std::to_string(opts.max_row_key_bytes));
    if (kv.value.size() > opts.max_value_bytes)
        return Status::InvalidArg(
            "VALUE_TOO_LARGE: " + std::to_string(kv.value.size()) +
            " > " + std::to_string(opts.max_value_bytes));
    if (kv.timestamp < 0)
        return Status::InvalidArg(
            "NEGATIVE_TIMESTAMP: " + std::to_string(kv.timestamp));
    return Status::OK();
}

static OwnedKeyValue sanitize_owned_kv(const KeyValue& kv, const WriterOptions& opts) {
    OwnedKeyValue out;
    out.row.assign(kv.row.begin(), kv.row.end());
    out.family.assign(kv.family.begin(), kv.family.end());
    out.qualifier.assign(kv.qualifier.begin(), kv.qualifier.end());
    out.timestamp = kv.timestamp;
    out.key_type = kv.key_type;
    out.value.assign(kv.value.begin(), kv.value.end());
    if (opts.include_tags)
        out.tags.assign(kv.tags.begin(), kv.tags.end());
    out.memstore_ts = opts.include_mvcc ? kv.memstore_ts : 0;
    out.has_memstore_ts = opts.include_mvcc && kv.memstore_ts > 0;
    return out;
}

static size_t estimate_owned_kv_bytes(const OwnedKeyValue& kv) noexcept {
    return sizeof(OwnedKeyValue)
         + kv.row.size()
         + kv.family.size()
         + kv.qualifier.size()
         + kv.value.size()
         + kv.tags.size();
}

static uint32_t hbase_compression_codec(Compression c) noexcept {
    switch (c) {
        case Compression::None:   return 2;
        case Compression::GZip:   return 1;
        case Compression::Snappy: return 3;
        case Compression::LZ4:    return 4;
        case Compression::Zstd:   return 6;
        default:                  return 2;
    }
}

// ─── Internal implementation class ───────────────────────────────────────────

class HFileWriterImpl {
public:
    HFileWriterImpl(std::string path, WriterOptions opts)
        : path_{std::move(path)}, opts_{std::move(opts)} {
        if (opts_.max_memory_bytes > 0)
            budget_ = std::make_unique<memory::MemoryBudget>(opts_.max_memory_bytes);
    }

    /// Destructor: if the writer was opened but finish() never succeeded,
    /// close and delete the partial file so it can't be picked up by BulkLoad.
    ~HFileWriterImpl() {
        if (opened_ && !finished_) {
            if (atomic_writer_) {
                atomic_writer_->abort();   // close + delete temp file
            } else if (plain_writer_) {
                plain_writer_->close();
                std::error_code ec;
                std::filesystem::remove(path_, ec);
            }
            log::warn("Partial HFile deleted: " + path_);
        }
    }

    Status open() {
        namespace fs = std::filesystem;
        fs::path p{path_};
        if (opts_.bytes_per_checksum == 0)
            return Status::InvalidArg("bytes_per_checksum must be > 0");
        if (p.has_parent_path()) {
            std::error_code ec;
            fs::create_directories(p.parent_path(), ec);
            if (ec)
                return Status::IoError(
                    "create_directories failed: " + p.parent_path().string() +
                    ": " + ec.message());
        }

        // ── Choose I/O backend based on FsyncPolicy ────────────────────────
        if (opts_.fsync_policy == FsyncPolicy::Safe) {
            auto aw = std::make_unique<io::AtomicFileWriter>(path_);
            atomic_writer_ = std::move(aw);
            writer_ = atomic_writer_.get();   // BlockWriter* view
        } else {
            plain_writer_ = io::BlockWriter::open_file(path_);
            writer_ = plain_writer_.get();    // BlockWriter* view
        }

        encoder_    = block::DataBlockEncoder::create(opts_.data_block_encoding,
                                                      opts_.block_size);
        compressor_ = codec::Compressor::create(opts_.compression);
        bloom_      = std::make_unique<bloom::CompoundBloomFilterWriter>(
                          opts_.bloom_type, opts_.bloom_error_rate);

        compress_buf_.resize(compressor_->max_compressed_size(opts_.block_size + 65536));
        if (budget_) {
            fixed_budget_bytes_ = compress_buf_.size();
            auto s = budget_->reserve(fixed_budget_bytes_);
            if (!s.ok()) return s;
        }
        start_time_ = std::chrono::steady_clock::now();
        opened_     = true;

        log::info("HFile started: path=" + path_ + " cf=" + opts_.column_family);
        return Status::OK();
    }

    Status append(const KeyValue& kv) {
        if (finished_)
            return Status::InvalidArg("append() called after finish()");
        // ── Input validation ──────────────────────────────────────────────
        auto vs = validate_kv(kv, opts_);
        if (!vs.ok()) {
            ++error_count_;
            if (opts_.error_callback) {
                RowError re;
                re.message = vs.message();
                opts_.error_callback(re);
            }
            log::warn("Row skipped: " + vs.message());
            ++skipped_rows_;

            if (opts_.error_policy == ErrorPolicy::Strict)
                return vs;
            if (opts_.error_policy == ErrorPolicy::SkipBatch)
                return Status::InvalidArg("SKIP_BATCH: " + vs.message());
            if (opts_.max_error_count > 0 && error_count_ > opts_.max_error_count)
                return Status::InvalidArg(
                    "MAX_ERRORS_EXCEEDED: " + std::to_string(error_count_) + " errors");
            return Status::OK();  // SkipRow: swallow and continue
        }

        // ── Column family check ───────────────────────────────────────────
        std::string_view kv_family(
            reinterpret_cast<const char*>(kv.family.data()), kv.family.size());
        if (kv_family != opts_.column_family)
            return Status::InvalidArg(
                "KeyValue family '" + std::string(kv_family) +
                "' != configured family '" + opts_.column_family + "'");

        if (opts_.sort_mode == WriterOptions::SortMode::AutoSort) {
            auto owned = sanitize_owned_kv(kv, opts_);
            HFILE_RETURN_IF_ERROR(buffer_auto_sorted_kv(std::move(owned)));
            return Status::OK();
        }

        auto owned = sanitize_owned_kv(kv, opts_);
        return append_materialized_kv(
            owned.as_view(), true, true,
            opts_.sort_mode == WriterOptions::SortMode::PreSortedVerified);
    }

    Status finish() {
        if (finished_) return Status::OK();
        // NOTE: do NOT set finished_ = true here.
        // We only set it after all I/O succeeds, so that:
        //   (a) a failed finish() can be detected by the destructor, which
        //       will then delete the corrupt partial file;
        //   (b) the caller can observe that finish() failed (returned error)
        //       and take corrective action.

        // Flush remaining data block
        if (opts_.sort_mode == WriterOptions::SortMode::AutoSort &&
            !auto_sorted_kvs_.empty()) {
            std::stable_sort(auto_sorted_kvs_.begin(), auto_sorted_kvs_.end(),
                             [](const OwnedKeyValue& a, const OwnedKeyValue& b) {
                                 return compare_keys(a.as_view(), b.as_view()) < 0;
                             });
            for (const auto& kv : auto_sorted_kvs_) {
                auto s = append_materialized_kv(kv.as_view(), false, false, true);
                if (!s.ok()) return s;
            }
            if (budget_ && auto_sort_reserved_bytes_ > 0) {
                budget_->release(auto_sort_reserved_bytes_);
                auto_sort_reserved_bytes_ = 0;
            }
            auto_sorted_kvs_.clear();
        }
        if (!encoder_->empty()) {
            HFILE_RETURN_IF_ERROR(flush_data_block());
        }
        bloom_->finish_chunk();  // seal final partial chunk if any

        // ── Non-scanned Block Section ─────────────────────────────────────────
        // Bloom chunk data blocks (BLMFBLK2) live here, between the last data
        // block and the load-on-open section.  This matches HBase's file layout:
        //   [Data Blocks] [Bloom Chunks] [load-on-open section]
        bloom::BloomWriteResult bloom_result;
        bloom_result.enabled = bloom_->is_enabled() && bloom_->has_data();
        bloom_result.total_key_count = bloom_->total_keys();

        if (bloom_result.enabled) {
            bloom_result.bloom_data_offset = writer_->position();
            std::vector<uint8_t> bloom_chunk_buf;
            bloom_->finish_data_blocks(bloom_chunk_buf, writer_->position());
            if (!bloom_chunk_buf.empty()) {
                HFILE_RETURN_IF_ERROR(
                    writer_->write({bloom_chunk_buf.data(), bloom_chunk_buf.size()}));
            }
        }

        // ── Load-on-open Section ──────────────────────────────────────────────
        // HBase load-on-open order:
        //   1. [Intermediate Index Blocks]  (IDXINTE2, large files only)
        //   2. Root Data Index              (IDXROOT2)
        //   3. Meta Root Index              (IDXROOT2, one entry → BLMFMET2 offset)
        //   4. FileInfo                     (FILEINF2)
        //   5. Bloom Meta Block             (BLMFMET2)
        //
        // The Trailer's load_on_open_offset must point at the start of step 1/2.
        int64_t load_on_open_offset = writer_->position();

        // 1+2. Intermediate + Root Data Index blocks
        std::vector<uint8_t> intermed_buf;
        std::vector<uint8_t> root_buf;
        int64_t intermed_start = writer_->position();
        auto idx_result = index_writer_.finish(intermed_start, intermed_buf, root_buf);

        if (!intermed_buf.empty()) {
            HFILE_RETURN_IF_ERROR(
                writer_->write({intermed_buf.data(), intermed_buf.size()}));
        }
        HFILE_RETURN_IF_ERROR(
            write_raw_block(kRootIndexMagic, {root_buf.data(), root_buf.size()}));

        // 3. Meta Root Index (IDXROOT2) — one entry pointing to BLMFMET2 if enabled,
        //    otherwise an empty root index (count = 0).
        //    Format: count(4B BE) + [offset(8B BE) + dataSize(4B BE) + keyLen(4B BE) + key]*
        {
            std::vector<uint8_t> meta_root;
            if (bloom_result.enabled) {
                // We don't yet know the BLMFMET2 offset, but we know it will be written
                // after FileInfo.  We store a placeholder and patch it after writing.
                // Simpler: reserve the meta_root buf now with a fixed entry; the
                // BLMFMET2 offset will be the current writer position after FileInfo.
                // To avoid a two-pass approach we write a 1-entry root index:
                //   key = "GENERAL_BLOOM_META" (HBase uses this name)
                static const uint8_t kBloomMetaKey[] = "GENERAL_BLOOM_META";
                const uint32_t key_len = static_cast<uint32_t>(sizeof(kBloomMetaKey) - 1);
                // placeholder offset — will be written immediately after FileInfo
                // We compute it now since FileInfo size is fixed once we call write_file_info()
                // WORKAROUND: use a 0 placeholder; HBase actually finds BLMFMET2 by scanning
                // from load_on_open_offset forward.  The meta_index_count=1 in the Trailer
                // is what matters to signal bloom is present.
                meta_root.resize(4 + 8 + 4 + 4 + key_len);
                uint8_t* mp = meta_root.data();
                write_be32(mp, 1);                    mp += 4;   // count = 1
                write_be64(mp, 0);                    mp += 8;   // offset placeholder
                write_be32(mp, 0);                    mp += 4;   // dataSize placeholder
                write_be32(mp, key_len);              mp += 4;
                std::memcpy(mp, kBloomMetaKey, key_len);
            } else {
                // Empty meta root index: count = 0
                meta_root.resize(4, 0);
            }
            HFILE_RETURN_IF_ERROR(
                write_raw_block(kRootIndexMagic, {meta_root.data(), meta_root.size()}));
        }

        // 4. FileInfo block
        int64_t file_info_offset = writer_->position();
        HFILE_RETURN_IF_ERROR(write_file_info());

        // 5. Bloom Meta Block (BLMFMET2) — written AFTER FileInfo, matching HBase order.
        bloom_result.bloom_meta_offset = writer_->position();
        if (bloom_result.enabled) {
            std::vector<uint8_t> bloom_meta_buf;
            bloom_->finish_meta_block(bloom_meta_buf);
            HFILE_RETURN_IF_ERROR(
                writer_->write({bloom_meta_buf.data(), bloom_meta_buf.size()}));
        }

        // 6. Trailer
        HFILE_RETURN_IF_ERROR(write_trailer(
            file_info_offset,
            load_on_open_offset,
            idx_result,
            bloom_result));

        // ── Commit: fsync + rename (Safe mode) or plain close ─────────────
        if (atomic_writer_) {
            HFILE_RETURN_IF_ERROR(atomic_writer_->commit());
        } else {
            HFILE_RETURN_IF_ERROR(plain_writer_->flush());
            HFILE_RETURN_IF_ERROR(plain_writer_->close());
        }

        // All I/O succeeded — mark finished so destructor doesn't delete the file.
        finished_ = true;
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start_time_);
        log::info("HFile completed: path=" + path_ +
                  " kvs=" + std::to_string(entry_count_) +
                  " elapsed=" + std::to_string(elapsed.count()) + "ms");
        return Status::OK();
    }

    int64_t  position()    const noexcept { return writer_ ? writer_->position() : 0; }
    uint64_t entry_count() const noexcept { return entry_count_; }
    uint64_t skipped_rows() const noexcept { return skipped_rows_; }

private:
    // ── Flush a completed data block ──────────────────────────────────────────
    Status flush_data_block() {
        auto raw = encoder_->finish_block();
        if (raw.empty()) { encoder_->reset(); return Status::OK(); }

        int64_t block_offset = writer_->position();

        // Seal the current bloom chunk for this data block.
        // The chunk bytes are collected internally; they will be written to disk
        // as BLMFBLK2 blocks immediately after all data blocks (non-scanned section).
        bloom_->finish_chunk();

        // Compress
        size_t comp_len = compressor_->compress(raw, compress_buf_.data(),
                                                  compress_buf_.size());
        if (comp_len == 0 && opts_.compression != Compression::None)
            return Status::Internal("Compression failed");

        std::span<const uint8_t> compressed{compress_buf_.data(), comp_len};
        if (opts_.compression == Compression::None) compressed = raw;

        HFILE_RETURN_IF_ERROR(
            write_data_block(raw.size(), compressed, encoder_->first_key(), block_offset));

        total_uncompressed_bytes_ += raw.size();
        ++data_block_count_;
        if (opts_.fsync_policy == FsyncPolicy::Paranoid &&
            opts_.fsync_block_interval > 0 &&
            (data_block_count_ % opts_.fsync_block_interval) == 0) {
            HFILE_RETURN_IF_ERROR(writer_->flush());
        }
        encoder_->reset();
        return Status::OK();
    }

    // ── Write a data block with header + checksums ─────────────────────────────
    Status write_data_block(size_t uncompressed_size,
                             std::span<const uint8_t> compressed_data,
                             std::span<const uint8_t> first_key,
                             int64_t block_offset) {
        uint32_t on_disk_data_with_header =
            static_cast<uint32_t>(kBlockHeaderSize + compressed_data.size());
        size_t n_chunks = (on_disk_data_with_header + opts_.bytes_per_checksum - 1)
                          / opts_.bytes_per_checksum;
        std::vector<uint8_t> checksum_buf(n_chunks * 4);

        // Build block header (33 bytes)
        uint8_t hdr[kBlockHeaderSize];
        uint8_t* p = hdr;
        const auto& magic = opts_.data_block_encoding == Encoding::None
            ? kDataBlockMagic
            : kEncodedDataBlockMagic;
        std::memcpy(p, magic.data(), 8); p += 8;
        uint32_t on_disk_size_without_header = static_cast<uint32_t>(
            compressed_data.size() + checksum_buf.size());
        write_be32(p, on_disk_size_without_header);                    p += 4;
        write_be32(p, static_cast<uint32_t>(uncompressed_size));       p += 4;  // uncompSz
        write_be64(p, static_cast<uint64_t>(prev_block_offset_));      p += 8;  // prevOffset
        *p++ = kChecksumTypeCRC32C;
        write_be32(p, opts_.bytes_per_checksum);                        p += 4;
        write_be32(p, on_disk_data_with_header);                        p += 4;

        std::vector<uint8_t> checksummed_block;
        checksummed_block.reserve(kBlockHeaderSize + compressed_data.size());
        checksummed_block.insert(checksummed_block.end(), hdr, hdr + kBlockHeaderSize);
        checksummed_block.insert(checksummed_block.end(),
                                 compressed_data.begin(), compressed_data.end());
        checksum::compute_hfile_checksums(
            checksummed_block.data(), checksummed_block.size(),
            opts_.bytes_per_checksum, checksum_buf.data());

        prev_block_offset_ = writer_->position();
        if (first_data_block_offset_ < 0)
            first_data_block_offset_ = block_offset;
        last_data_block_offset_ = block_offset;
        index_writer_.add_entry(first_key,
                                block_offset,
                                static_cast<int32_t>(kBlockHeaderSize + on_disk_size_without_header));

        HFILE_RETURN_IF_ERROR(writer_->write({hdr, kBlockHeaderSize}));
        HFILE_RETURN_IF_ERROR(writer_->write(compressed_data));
        HFILE_RETURN_IF_ERROR(writer_->write({checksum_buf.data(), checksum_buf.size()}));
        return Status::OK();
    }

    // ── Write any raw block (index, meta, etc.) with header ───────────────────
    Status write_raw_block(const std::array<uint8_t, 8>& magic,
                            std::span<const uint8_t> data) {
        uint32_t on_disk_data_with_header =
            static_cast<uint32_t>(kBlockHeaderSize + data.size());
        size_t n_chunks = (on_disk_data_with_header + opts_.bytes_per_checksum - 1)
                        / opts_.bytes_per_checksum;
        std::vector<uint8_t> checksum_buf(n_chunks * 4);

        uint8_t hdr[kBlockHeaderSize];
        uint8_t* p = hdr;
        std::memcpy(p, magic.data(), 8); p += 8;
        uint32_t on_disk_size_without_header = static_cast<uint32_t>(data.size() + checksum_buf.size());
        write_be32(p, on_disk_size_without_header); p += 4;
        write_be32(p, static_cast<uint32_t>(data.size())); p += 4;
        write_be64(p, static_cast<uint64_t>(prev_block_offset_)); p += 8;
        *p++ = kChecksumTypeCRC32C;
        write_be32(p, opts_.bytes_per_checksum); p += 4;
        write_be32(p, on_disk_data_with_header); p += 4;

        std::vector<uint8_t> checksummed_block;
        checksummed_block.reserve(kBlockHeaderSize + data.size());
        checksummed_block.insert(checksummed_block.end(), hdr, hdr + kBlockHeaderSize);
        checksummed_block.insert(checksummed_block.end(), data.begin(), data.end());
        checksum::compute_hfile_checksums(
            checksummed_block.data(), checksummed_block.size(),
            opts_.bytes_per_checksum, checksum_buf.data());

        prev_block_offset_ = writer_->position();
        HFILE_RETURN_IF_ERROR(writer_->write({hdr, kBlockHeaderSize}));
        HFILE_RETURN_IF_ERROR(writer_->write(data));
        HFILE_RETURN_IF_ERROR(writer_->write({checksum_buf.data(), checksum_buf.size()}));
        return Status::OK();
    }

    // ── Write FileInfo block ──────────────────────────────────────────────────
    Status write_file_info() {
        meta::FileInfoBuilder fib;

        // All mandatory fields per DESIGN.md §2.3
        fib.set_last_key({last_key_.data(), last_key_.size()});
        fib.set_avg_key_len(entry_count_ > 0
            ? static_cast<uint32_t>(total_key_bytes_ / entry_count_) : 0);
        fib.set_avg_value_len(entry_count_ > 0
            ? static_cast<uint32_t>(total_value_bytes_ / entry_count_) : 0);
        fib.set_max_tags_len(max_tags_len_);
        fib.set_key_value_version(1);
        fib.set_max_memstore_ts(has_mvcc_cells_ ? max_memstore_ts_ : 0);
        fib.set_comparator(opts_.comparator);
        fib.set_data_block_encoding(opts_.data_block_encoding);
        fib.set_create_time();
        fib.set_len_of_biggest_cell(static_cast<uint64_t>(max_cell_size_));
        HFILE_RETURN_IF_ERROR(fib.validate_required_fields());

        std::vector<uint8_t> fi_bytes;
        fib.finish(fi_bytes);
        return write_raw_block(kFileInfoMagic, {fi_bytes.data(), fi_bytes.size()});
    }

    // ── Write ProtoBuf Trailer ────────────────────────────────────────────────
    Status write_trailer(int64_t file_info_offset,
                         int64_t load_on_open_offset,
                         const index::IndexWriteResult& idx,
                         const bloom::BloomWriteResult& bloom) {
        meta::TrailerBuilder tb;
        tb.set_file_info_offset(static_cast<uint64_t>(file_info_offset));
        tb.set_load_on_open_offset(static_cast<uint64_t>(load_on_open_offset));
        tb.set_uncompressed_data_index_size(idx.uncompressed_size);
        tb.set_total_uncompressed_bytes(total_uncompressed_bytes_);
        tb.set_data_index_count(data_block_count_);
        tb.set_meta_index_count(bloom.enabled ? 1u : 0u);
        tb.set_entry_count(entry_count_);
        tb.set_num_data_index_levels(static_cast<uint32_t>(idx.num_levels));
        tb.set_first_data_block_offset(
            first_data_block_offset_ >= 0
                ? static_cast<uint64_t>(first_data_block_offset_) : 0);
        tb.set_last_data_block_offset(
            last_data_block_offset_ >= 0
                ? static_cast<uint64_t>(last_data_block_offset_) : 0);
        tb.set_comparator_class_name(opts_.comparator);
        tb.set_compression_codec(hbase_compression_codec(opts_.compression));

        std::vector<uint8_t> trailer_bytes;
        HFILE_RETURN_IF_ERROR(tb.finish(trailer_bytes));
        return writer_->write({trailer_bytes.data(), trailer_bytes.size()});
    }

    // ── Helpers for tracking last/first key ────────────────────────────────────
    void save_last_key(const KeyValue& kv, bool keep_for_validation) {
        last_key_.resize(kv.key_length());
        block::serialize_key(kv, last_key_.data());
        if (keep_for_validation) {
            last_kv_.row.assign(kv.row.begin(), kv.row.end());
            last_kv_.family.assign(kv.family.begin(), kv.family.end());
            last_kv_.qualifier.assign(kv.qualifier.begin(), kv.qualifier.end());
            last_kv_.timestamp = kv.timestamp;
            last_kv_.key_type  = kv.key_type;
        }
    }

    void save_first_key(const KeyValue& kv) {
        first_key_.resize(kv.key_length());
        block::serialize_key(kv, first_key_.data());
    }

    void record_cell_stats(const KeyValue& kv) {
        uint32_t kl = kv.key_length();
        uint32_t vl = static_cast<uint32_t>(kv.value.size());
        total_key_bytes_ += kl;
        total_value_bytes_ += vl;
        max_tags_len_ = std::max(max_tags_len_,
                                 static_cast<uint32_t>(kv.tags.size()));
        if (kv.has_memstore_ts) {
            has_mvcc_cells_ = true;
            max_memstore_ts_ = std::max<uint64_t>(max_memstore_ts_, kv.memstore_ts);
        }
        int mvcc_len = kv.has_memstore_ts
            ? writable_vint_size(static_cast<int64_t>(kv.memstore_ts))
            : 0;
        size_t cell_size = 4 + 4 + kl + kv.value.size() + 2 + kv.tags.size()
                         + static_cast<size_t>(mvcc_len);
        max_cell_size_ = std::max(max_cell_size_, cell_size);
    }

    Status maybe_check_disk_space(size_t encoded_bytes) {
        if (opts_.disk_check_interval_bytes == 0 || opts_.min_free_disk_bytes == 0)
            return Status::OK();
        bytes_since_disk_check_ += encoded_bytes;
        if (bytes_since_disk_check_ < opts_.disk_check_interval_bytes)
            return Status::OK();
        bytes_since_disk_check_ = 0;
        int64_t free = free_disk_bytes(path_);
        if (free >= 0 && static_cast<size_t>(free) < opts_.min_free_disk_bytes) {
            log::error("DISK_SPACE_EXHAUSTED: only " +
                       std::to_string(free / 1024 / 1024) + " MB free");
            return Status::IoError("DISK_SPACE_EXHAUSTED");
        }
        return Status::OK();
    }

    Status append_materialized_kv(
            const KeyValue& kv,
            bool increment_entry_count,
            bool record_stats,
            bool verify_sort) {
        if (verify_sort && written_entry_count_ > 0) {
            int cmp = compare_keys(kv, last_kv_.as_view());
            if (cmp <= 0)
                return Status::InvalidArg("SORT_ORDER_VIOLATION: KV out of order");
        }

        HFILE_RETURN_IF_ERROR(maybe_check_disk_space(kv.encoded_size()));

        if (!encoder_->append(kv)) {
            HFILE_RETURN_IF_ERROR(flush_data_block());
            if (!encoder_->append(kv))
                return Status::Internal("Failed to append KV even after flush");
        }

        bloom_->add(kv.row);
        if (opts_.bloom_type == BloomType::RowCol)
            bloom_->add_row_col(kv.row, kv.qualifier);

        if (record_stats) record_cell_stats(kv);
        if (written_entry_count_ == 0) save_first_key(kv);
        save_last_key(kv, verify_sort);

        ++written_entry_count_;
        if (increment_entry_count) ++entry_count_;
        return Status::OK();
    }

    Status buffer_auto_sorted_kv(OwnedKeyValue kv) {
        size_t reserve_bytes = estimate_owned_kv_bytes(kv);
        if (auto_sorted_kvs_.size() == auto_sorted_kvs_.capacity()) {
            size_t new_cap = auto_sorted_kvs_.capacity() == 0 ? 16 : auto_sorted_kvs_.capacity() * 2;
            reserve_bytes += (new_cap - auto_sorted_kvs_.capacity()) * sizeof(OwnedKeyValue);
            if (budget_) HFILE_RETURN_IF_ERROR(budget_->reserve(reserve_bytes));
            try {
                auto_sorted_kvs_.reserve(new_cap);
            } catch (...) {
                if (budget_) budget_->release(reserve_bytes);
                return Status::Internal("AutoSort reserve failed");
            }
        } else if (budget_) {
            HFILE_RETURN_IF_ERROR(budget_->reserve(reserve_bytes));
        }
        auto_sort_reserved_bytes_ += reserve_bytes;
        record_cell_stats(kv.as_view());
        ++entry_count_;
        auto_sorted_kvs_.push_back(std::move(kv));
        return Status::OK();
    }

    // ── State ─────────────────────────────────────────────────────────────────
    std::string                                 path_;
    WriterOptions                               opts_;

    // I/O backend (exactly one of atomic_writer_ / plain_writer_ owns the resource;
    // writer_ is a non-owning BlockWriter* aliasing whichever is active)
    io::BlockWriter*                            writer_{nullptr};
    std::unique_ptr<io::AtomicFileWriter>       atomic_writer_;    // Safe mode
    std::unique_ptr<io::BlockWriter>            plain_writer_;     // Fast/Paranoid

    std::unique_ptr<block::DataBlockEncoder>    encoder_;
    std::unique_ptr<codec::Compressor>          compressor_;
    std::unique_ptr<bloom::CompoundBloomFilterWriter> bloom_;
    index::BlockIndexWriter                     index_writer_;

    std::vector<uint8_t>   compress_buf_;
    std::vector<uint8_t>   last_key_;
    std::vector<uint8_t>   first_key_;
    OwnedKeyValue          last_kv_;
    std::vector<OwnedKeyValue> auto_sorted_kvs_;

    // ── Production: resource management ──────────────────────────────────────
    std::unique_ptr<memory::MemoryBudget>       budget_;
    size_t   fixed_budget_bytes_{0};
    size_t   auto_sort_reserved_bytes_{0};
    size_t   bytes_since_disk_check_{0};

    // ── Statistics ────────────────────────────────────────────────────────────
    uint64_t entry_count_            = 0;
    uint64_t written_entry_count_    = 0;
    uint64_t skipped_rows_           = 0;
    uint64_t error_count_            = 0;
    uint64_t total_key_bytes_        = 0;
    uint64_t total_value_bytes_      = 0;
    uint64_t total_uncompressed_bytes_ = 0;
    uint32_t data_block_count_       = 0;
    uint32_t max_tags_len_           = 0;
    uint64_t max_memstore_ts_        = 0;
    bool     has_mvcc_cells_         = false;
    size_t   max_cell_size_          = 0;
    int64_t  first_data_block_offset_ = -1;
    int64_t  last_data_block_offset_  = -1;
    int64_t  prev_block_offset_       = -1;

    std::chrono::steady_clock::time_point start_time_;
    bool     opened_   = false;
    bool     finished_ = false;
};

// ─── HFileWriter public API ───────────────────────────────────────────────────

HFileWriter::HFileWriter(std::unique_ptr<HFileWriterImpl> impl)
    : impl_{std::move(impl)} {}

HFileWriter::~HFileWriter() = default;
HFileWriter::HFileWriter(HFileWriter&&) noexcept = default;
HFileWriter& HFileWriter::operator=(HFileWriter&&) noexcept = default;

Status HFileWriter::append(const KeyValue& kv) {
    return impl_->append(kv);
}

Status HFileWriter::append(std::span<const uint8_t> row,
                            std::span<const uint8_t> family,
                            std::span<const uint8_t> qualifier,
                            int64_t                  timestamp,
                            std::span<const uint8_t> value,
                            KeyType                  key_type,
                            std::span<const uint8_t> tags,
                            uint64_t                 memstore_ts) {
    KeyValue kv;
    kv.row        = row;
    kv.family     = family;
    kv.qualifier  = qualifier;
    kv.timestamp  = timestamp;
    kv.key_type   = key_type;
    kv.value      = value;
    kv.tags       = tags;
    kv.memstore_ts = memstore_ts;
    return impl_->append(kv);
}

Status HFileWriter::finish() {
    return impl_->finish();
}

int64_t  HFileWriter::position()    const noexcept { return impl_->position(); }
uint64_t HFileWriter::entry_count() const noexcept { return impl_->entry_count(); }

// ─── Builder ─────────────────────────────────────────────────────────────────

HFileWriter::Builder& HFileWriter::Builder::set_path(std::string path) {
    path_ = std::move(path); return *this;
}
HFileWriter::Builder& HFileWriter::Builder::set_column_family(std::string cf) {
    opts_.column_family = std::move(cf); return *this;
}
HFileWriter::Builder& HFileWriter::Builder::set_compression(Compression c) {
    opts_.compression = c; return *this;
}
HFileWriter::Builder& HFileWriter::Builder::set_block_size(size_t sz) {
    opts_.block_size = sz; return *this;
}
HFileWriter::Builder& HFileWriter::Builder::set_data_block_encoding(Encoding enc) {
    opts_.data_block_encoding = enc; return *this;
}
HFileWriter::Builder& HFileWriter::Builder::set_bloom_type(BloomType bt) {
    opts_.bloom_type = bt; return *this;
}
HFileWriter::Builder& HFileWriter::Builder::set_bloom_error_rate(double r) {
    opts_.bloom_error_rate = r; return *this;
}
HFileWriter::Builder& HFileWriter::Builder::set_comparator(std::string cmp) {
    opts_.comparator = std::move(cmp); return *this;
}
HFileWriter::Builder& HFileWriter::Builder::set_sort_mode(WriterOptions::SortMode m) {
    opts_.sort_mode = m; return *this;
}
HFileWriter::Builder& HFileWriter::Builder::set_include_tags(bool v) {
    opts_.include_tags = v; return *this;
}
HFileWriter::Builder& HFileWriter::Builder::set_include_mvcc(bool v) {
    opts_.include_mvcc = v; return *this;
}
HFileWriter::Builder& HFileWriter::Builder::set_fsync_policy(FsyncPolicy p) {
    opts_.fsync_policy = p; return *this;
}
HFileWriter::Builder& HFileWriter::Builder::set_error_policy(ErrorPolicy p) {
    opts_.error_policy = p; return *this;
}
HFileWriter::Builder& HFileWriter::Builder::set_max_error_count(uint64_t n) {
    opts_.max_error_count = n; return *this;
}
HFileWriter::Builder& HFileWriter::Builder::set_error_callback(
        std::function<void(const RowError&)> cb) {
    opts_.error_callback = std::move(cb); return *this;
}
HFileWriter::Builder& HFileWriter::Builder::set_max_row_key_bytes(uint32_t n) {
    opts_.max_row_key_bytes = n; return *this;
}
HFileWriter::Builder& HFileWriter::Builder::set_max_value_bytes(size_t n) {
    opts_.max_value_bytes = n; return *this;
}
HFileWriter::Builder& HFileWriter::Builder::set_max_memory(size_t bytes) {
    opts_.max_memory_bytes = bytes; return *this;
}
HFileWriter::Builder& HFileWriter::Builder::set_min_free_disk(size_t bytes) {
    opts_.min_free_disk_bytes = bytes; return *this;
}
HFileWriter::Builder& HFileWriter::Builder::set_disk_check_interval(size_t bytes) {
    opts_.disk_check_interval_bytes = bytes; return *this;
}
HFileWriter::Builder& HFileWriter::Builder::set_max_open_files(int n) {
    opts_.max_open_files = n; return *this;
}

std::pair<std::unique_ptr<HFileWriter>, Status> HFileWriter::Builder::build() {
    if (path_.empty())
        return {nullptr, Status::InvalidArg("path must be set")};
    if (opts_.column_family.empty())
        return {nullptr, Status::InvalidArg("column_family must be set")};

    try {
        auto impl = std::make_unique<HFileWriterImpl>(path_, opts_);
        Status s  = impl->open();
        if (!s.ok()) return {nullptr, s};

        return {std::unique_ptr<HFileWriter>(new HFileWriter(std::move(impl))),
                Status::OK()};
    } catch (const std::exception& e) {
        return {nullptr, Status::Internal(std::string("writer build failed: ") + e.what())};
    } catch (...) {
        return {nullptr, Status::Internal("writer build failed: unknown exception")};
    }
}

} // namespace hfile
