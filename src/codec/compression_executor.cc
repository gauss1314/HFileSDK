#include "compression_executor.h"

#include <algorithm>
#include <array>
#include <fstream>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#if defined(__linux__)
#include <sched.h>
#elif defined(__APPLE__)
#include <sys/sysctl.h>
#elif defined(_WIN32) || defined(_WIN64)
#include <windows.h>
#endif

namespace hfile::codec
{

namespace
{

constexpr size_t compressor_cache_index(int level) noexcept
{
    // WriterOptions uses 0 for zlib's default level and otherwise accepts the
    // normal 1..9 range.  Keep an extra fallback bucket for defensive handling
    // of a value outside that range; Compressor itself remains authoritative.
    if (level <= 0)
    {
        return 0;
    }
    return level <= 9 ? static_cast<size_t>(level) : 10u;
}

uint32_t positive_hardware_concurrency() noexcept
{
    return std::max(1u, std::thread::hardware_concurrency());
}

#if defined(__linux__)
uint32_t linux_affinity_limit() noexcept
{
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    if (::sched_getaffinity(0, sizeof(cpuset), &cpuset) == 0)
    {
        const int count = CPU_COUNT(&cpuset);
        if (count > 0)
        {
            return static_cast<uint32_t>(count);
        }
    }
    return positive_hardware_concurrency();
}

uint32_t quota_to_worker_limit(uint64_t quota, uint64_t period) noexcept
{
    if (quota == 0 || period == 0)
    {
        return 0;
    }
    // A 1.5-CPU quota can profitably run two compression workers; the kernel
    // still enforces the aggregate quota.  Flooring here needlessly left half
    // a CPU idle for the common fractional-quota case.
    const uint64_t workers = quota / period + (quota % period != 0 ? 1u : 0u);
    return static_cast<uint32_t>(std::clamp<uint64_t>(workers, 1, std::numeric_limits<uint32_t>::max()));
}

uint32_t read_cgroup_v2_cpu_limit_at(const std::string& directory)
{
    std::ifstream cpu_max(directory + "/cpu.max");
    if (!cpu_max)
    {
        return 0;
    }
    std::string quota;
    uint64_t period = 0;
    cpu_max >> quota >> period;
    if (!cpu_max || quota == "max" || period == 0)
    {
        return 0;
    }
    try
    {
        const uint64_t quota_value = std::stoull(quota);
        return quota_to_worker_limit(quota_value, period);
    }
    catch (...)
    {
        return 0;
    }
}

uint32_t read_cgroup_v1_cpu_limit_at(const std::string& directory)
{
    std::ifstream quota_file(directory + "/cpu.cfs_quota_us");
    std::ifstream period_file(directory + "/cpu.cfs_period_us");
    int64_t quota = -1;
    int64_t period = 0;
    if (!(quota_file >> quota) || !(period_file >> period) || quota <= 0 || period <= 0)
    {
        return 0;
    }
    return quota_to_worker_limit(static_cast<uint64_t>(quota), static_cast<uint64_t>(period));
}

void apply_limit(uint32_t candidate, uint32_t* limit) noexcept
{
    if (candidate == 0)
    {
        return;
    }
    *limit = *limit == 0 ? candidate : std::min(*limit, candidate);
}

template <typename ReadLimit>
uint32_t read_hierarchical_limit(const std::string& mount_root, std::string_view cgroup_path, ReadLimit read_limit)
{
    // /proc/self/cgroup is kernel-owned, but reject parent traversal anyway so
    // path construction remains constrained beneath the known cgroup mount.
    if (cgroup_path.find("..") != std::string_view::npos)
    {
        return 0;
    }
    while (!cgroup_path.empty() && cgroup_path.front() == '/')
    {
        cgroup_path.remove_prefix(1);
    }

    std::string current = mount_root;
    if (!cgroup_path.empty())
    {
        current.push_back('/');
        current.append(cgroup_path);
    }

    uint32_t limit = 0;
    for (;;)
    {
        apply_limit(read_limit(current), &limit);
        if (current.size() <= mount_root.size())
        {
            break;
        }
        const size_t slash = current.find_last_of('/');
        if (slash == std::string::npos || slash < mount_root.size())
        {
            break;
        }
        current.resize(std::max(slash, mount_root.size()));
    }
    return limit;
}

bool controller_list_has_cpu(std::string_view controllers) noexcept
{
    size_t begin = 0;
    while (begin <= controllers.size())
    {
        const size_t comma = controllers.find(',', begin);
        const size_t end = comma == std::string_view::npos ? controllers.size() : comma;
        if (controllers.substr(begin, end - begin) == "cpu")
        {
            return true;
        }
        if (comma == std::string_view::npos)
        {
            break;
        }
        begin = comma + 1;
    }
    return false;
}

uint32_t read_cgroup_cpu_limit()
{
    std::ifstream membership("/proc/self/cgroup");
    std::string line;
    std::string v2_path;
    std::string v1_path;
    while (std::getline(membership, line))
    {
        const size_t first = line.find(':');
        const size_t second = first == std::string::npos ? std::string::npos : line.find(':', first + 1);
        if (second == std::string::npos)
        {
            continue;
        }
        const std::string_view hierarchy(line.data(), first);
        const std::string_view controllers(line.data() + first + 1, second - first - 1);
        const std::string path = line.substr(second + 1);
        if (hierarchy == "0" && controllers.empty())
        {
            v2_path = path;
        }
        if (controller_list_has_cpu(controllers))
        {
            v1_path = path;
        }
    }

    uint32_t limit = 0;
    // Always inspect the mount root as a fallback.  In a cgroup namespace it
    // is commonly already remapped to the process's effective cgroup.
    apply_limit(read_hierarchical_limit("/sys/fs/cgroup", v2_path, read_cgroup_v2_cpu_limit_at), &limit);

    static constexpr std::array<std::string_view, 3> kV1CpuMounts = {
        "/sys/fs/cgroup/cpu",
        "/sys/fs/cgroup/cpu,cpuacct",
        "/sys/fs/cgroup/cpuacct,cpu",
    };
    for (const auto mount : kV1CpuMounts)
    {
        apply_limit(read_hierarchical_limit(std::string(mount), v1_path, read_cgroup_v1_cpu_limit_at), &limit);
    }
    return limit;
}
#endif

uint32_t available_cpu_limit() noexcept
{
    uint32_t limit = positive_hardware_concurrency();
    try
    {
#if defined(__linux__)
        limit = std::min(limit, linux_affinity_limit());
        const uint32_t quota_limit = read_cgroup_cpu_limit();
        if (quota_limit > 0)
        {
            limit = std::min(limit, quota_limit);
        }
#elif defined(__APPLE__)
        uint32_t physical = 0;
        size_t physical_size = sizeof(physical);
        if (::sysctlbyname("hw.physicalcpu", &physical, &physical_size, nullptr, 0) == 0 && physical > 0)
        {
            limit = std::min(limit, physical);
        }
#elif defined(_WIN32) || defined(_WIN64)
        const DWORD active = ::GetActiveProcessorCount(ALL_PROCESSOR_GROUPS);
        if (active > 0)
        {
            limit = std::min(limit, static_cast<uint32_t>(active));
        }
#endif
    }
    catch (...)
    {
        // CPU discovery is a tuning hint, never a reason to fail writer open.
        // hardware_concurrency remains the conservative portable fallback.
        (void)0;
    }
    return std::max(1u, limit);
}

} // namespace

CompressionExecutor& CompressionExecutor::instance()
{
    static CompressionExecutor executor;
    return executor;
}

CompressionExecutor::CompressionExecutor() : queue_(kQueueCapacity), worker_limit_(available_cpu_limit())
{
}

CompressionExecutor::~CompressionExecutor()
{
    {
        std::lock_guard<std::mutex> lock(mu_);
        stopping_ = true;
    }
    work_cv_.notify_all();
    space_cv_.notify_all();
    for (auto& worker : workers_)
    {
        if (worker.joinable())
        {
            worker.join();
        }
    }
}

void CompressionExecutor::ensure_workers(uint32_t count)
{
    if (count == 0)
    {
        return;
    }
    count = std::min(count, worker_limit_);

    std::lock_guard<std::mutex> lock(mu_);
    if (stopping_ || workers_.size() >= count)
    {
        return;
    }
    workers_.reserve(count);
    while (workers_.size() < count)
    {
        workers_.emplace_back(
            [this]()
            {
                worker_loop();
            });
    }
}

bool CompressionExecutor::submit(Task task)
{
    if (task.run == nullptr)
    {
        return false;
    }

    std::unique_lock<std::mutex> lock(mu_);
    space_cv_.wait(lock,
                   [this]()
                   {
                       return stopping_ || queue_size_ < queue_.size();
                   });
    if (stopping_)
    {
        return false;
    }

    queue_[queue_tail_] = task;
    queue_tail_ = (queue_tail_ + 1) % queue_.size();
    ++queue_size_;
    lock.unlock();
    work_cv_.notify_one();
    return true;
}

size_t CompressionExecutor::worker_count() const noexcept
{
    std::lock_guard<std::mutex> lock(mu_);
    return workers_.size();
}

void CompressionExecutor::worker_loop() noexcept
{
    // Streams are reused for the full process lifetime of this worker.  A
    // writer's requested zlib level selects a distinct stream, preserving the
    // exact deflateReset()/deflate() behavior of the former per-writer workers.
    std::array<std::unique_ptr<Compressor>, 11> gzip_compressors;
    std::unique_ptr<Compressor> none_compressor;

    for (;;)
    {
        Task task;
        {
            std::unique_lock<std::mutex> lock(mu_);
            work_cv_.wait(lock,
                          [this]()
                          {
                              return stopping_ || queue_size_ > 0;
                          });
            if (stopping_ && queue_size_ == 0)
            {
                return;
            }

            task = queue_[queue_head_];
            queue_head_ = (queue_head_ + 1) % queue_.size();
            --queue_size_;
        }
        space_cv_.notify_one();

        Compressor* compressor = nullptr;
        try
        {
            if (task.compression == Compression::GZip)
            {
                auto& cached = gzip_compressors[compressor_cache_index(task.compression_level)];
                if (!cached)
                {
                    cached = Compressor::create(task.compression, task.compression_level);
                }
                compressor = cached.get();
            }
            else
            {
                if (!none_compressor)
                {
                    none_compressor = Compressor::create(task.compression, 0);
                }
                compressor = none_compressor.get();
            }
        }
        catch (...)
        {
            // Allocation/codec initialization failures are reported through
            // the task callback.  They must never terminate the JVM from a
            // native worker thread.
            compressor = nullptr;
        }
        task.run(task.context, compressor);
    }
}

} // namespace hfile::codec
