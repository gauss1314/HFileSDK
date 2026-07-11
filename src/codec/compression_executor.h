#pragma once

#include "compressor.h"

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <thread>
#include <vector>

namespace hfile::codec
{

/// Process-wide bounded executor for independent HFile block compression.
///
/// Writers retain their own ordering, backpressure, and per-writer concurrency
/// limits.  This executor only owns the reusable worker threads and their
/// worker-local Compressor instances.  Tasks are intentionally represented by
/// a function pointer plus context pointer so submitting a block never
/// allocates a std::function closure.
class CompressionExecutor final
{
public:
    // compressor is null if worker-local codec initialization failed.  The
    // callback must translate that condition into the owning writer's normal
    // error channel; exceptions must never escape a background thread.
    using TaskFn = void (*)(void* context, Compressor* compressor) noexcept;

    struct Task
    {
        TaskFn run{nullptr};
        void* context{nullptr};
        Compression compression{Compression::GZip};
        int compression_level{1};
    };

    static CompressionExecutor& instance();

    CompressionExecutor(const CompressionExecutor&) = delete;
    CompressionExecutor& operator=(const CompressionExecutor&) = delete;

    /// Ensure the process-wide pool has at least `count` workers.  The pool may
    /// grow when a later writer requests more parallelism, but never shrinks
    /// while the process is alive.
    void ensure_workers(uint32_t count);

    /// Submit one task.  The call blocks when the global queue is full, which
    /// provides process-wide bounded backpressure.  Returns false only while
    /// the executor is shutting down.
    bool submit(Task task);

    size_t worker_count() const noexcept;
    uint32_t worker_limit() const noexcept
    {
        return worker_limit_;
    }

private:
    static constexpr size_t kQueueCapacity = 4096;

    CompressionExecutor();
    ~CompressionExecutor();

    void worker_loop() noexcept;

    mutable std::mutex mu_;
    std::condition_variable work_cv_;
    std::condition_variable space_cv_;
    std::vector<Task> queue_;
    size_t queue_head_{0};
    size_t queue_tail_{0};
    size_t queue_size_{0};
    std::vector<std::thread> workers_;
    uint32_t worker_limit_{1};
    bool stopping_{false};
};

} // namespace hfile::codec
