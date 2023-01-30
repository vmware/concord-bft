// Concord
//
// Copyright (c) 2023 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of sub-components with separate copyright notices and license terms. Your use of
// these sub-components is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include "Timers.hpp"
#include "Metrics.hpp"
#include "assertUtils.hpp"
#include "throughput.hpp"
#include "diagnostics.h"
#include "performance_handler.h"

#include <boost/lockfree/queue.hpp>

#include <string_view>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <set>
#include <deque>

/**
 * Multi-Buffer Memory Pool (MultiSizeBufferPool):
 *
 * === For API: search for "Public API" ===
 *
 * Definitions:
 * - Buffer: a continuous, fixed size array of bytes dedicated to a caller.
 * - Chunk: Wraps a buffer. Pool deals internally with chunks, which include header and trailer metadata for internal
 * use.
 * - Used buffer/chunk: currently used by caller
 * - Unused buffer/chunk: currently not-used by caller, and stored in the pool.
 * - Allocated buffer/chunk: currently allocated by pool. Can be used or unused in that case.
 * - Non-allocated buffer/chunk: currently not allocated by pool - pool reserves the right to allocate this chunk
 * dynamically.
 * - Subpool: holds a group of buffers of a fixed constant size. Each subpool has its own configuration and contention.
 * lock-free domain: threads that try to get a buffer from subpool A, do not compete with threads that try to get a
 * buffer from subpool B. This allow (with an additional very small implementation changes) to create multiple
 * subpools with the same buffer size to reduce contention. Currently, all subpools should be different sizes.
 * - Client - a user of the pool's API in order to use the memory pool buffer.
 * - IPurger: a module that is responsible for purging the sub-queues, based on recently collected data. Can be
 * disabled. Can be passed externally by pool creator. If not passed externally, the pool creates a default
 * DefaultPurger. The main idea of the purger is to deallocate unused resources (chunks) from the pool to keep it
 * minimal according to a given purging policy. Purger can be configured to treat different sub-pools (buffer sizes)
 * in different ways.
 * - ISubpoolSelector: a module that is responsible for selecting the most suitable sub-pool (buffer size), based on
 * recently collected data. Can be disabled. It can be passed externally by the pool creator. If not passed externally,
 * pool creates a default DefaultSubpoolSelector which chooses to smallest buffer size sub-pool always.
 * - Pool - holds its configuration purger, subpool selector and an array of subpools (at least 1).
 *
 * Overview:
 * This memory pool has the following attributes:
 * - Thread safe for all of its operations (not thread safe, of course, when accessing the currently used buffers).
 * - Supports advanced configuration, in particular caller can define any amount of sub-pools of any size with initial
 * and maximal number of buffers for each subpool.
 * A caller may control the overall size of the allocated bytes in the pool, this can give more 'freedom' per subpool,
 * and restrict the total pool size.
 * - Rich statistics:
 * 1) Histograms (snapshots) reported to logs. Currently, histograms are for this pool can't be viewed
 * from concord-ctl.
 * 2) Metrics - reported to Wavefront.
 * 3) Statistics - reported to log. It is very important to mention, that due to speed consideration, each of the
 * statistics are "stand-alone" and atomic. That means, for a statistics report, you might find discrepancies between
 * statistics of the same type, due to the nature of this multi-threaded pool (statistics are updated all the time by
 * multiple clients in a non-atomic way). All types of statistics can be disabled/enabled.
 * - Blocking - calls to get a buffer should never fail, unless requested buffer size is too big or there is no more
 * memory - in rare cases user may get a larger buffer than requested, in more cases caller may block.
 * Currently, there is no implementation for wait_for(..) e.g async mode.
 * - Dynamic allocation - each pool allocates its initial chunks when created. In case there is no available
 * chunks in a specific subpool, a new one gets dynamically allocated - up to a maximum number of chunks defined for
 * this subpool.
 * - Throwing - asserts are done to detect internal coding errors. In case of user input error or System error -
 * an exception is thrown.
 * - Borrowing - if all buffers of a certain sub-pool SP1 are allocated, a calling thread checks for a free chunk in the
 * rest of the subpools (SP2, SP3 etc..) for speed, but won't try to allocate a new one on these subpools. If a buffer
 * isn't found - it returns to wait for a chunk to be released by another thread on the original subpool SP1.
 * - Unit-testing: done by GTest, see MultiSizeBufferPool_test.
 * - Safety checks:
 * 1) Pool assign magic flags on chunk's header and trailer. If they are changed - exception is thrown.
 * 2) Check for leaks - pool can be configured to check that all buffers have been returned to it before destroying
 * itself. If not, it throws.
 *
 * Locking is done only for conditional variables for this pool. This is in order to achieve speed. This pool currently
 * faces thousands of calls per second for its main use-case. This leads to sub-optimized cases in both statistics and
 * behavior. For example, the upper threshold for maximal pool size might cross more than user defined, due to the
 * multi-threaded nature and since there is no locking. Another example is for purge limit: we might purge a "little
 * more" than requested.
 *
 * Important comment about usage:
 * It is the responsibilty iof the user to make sure that:
 * 1) Pool is destroyed by a single thread
 * 2) whn pool is destroyed all buffers have been returned
 * 3) This implies that since all buffers have been returned, no more threads are waiting for buffers.
 */

namespace concordUtil {

namespace test {
class MultiSizeBufferPoolTestFixture;
}

class MultiSizeBufferPool {
  friend class concordUtil::test::MultiSizeBufferPoolTestFixture;  // Should be strictly used for testing!

 protected:
  class MultiSizeBufferPoolMetrics {
   public:
    MultiSizeBufferPoolMetrics(const std::string& name)
        : component_{std::string("MultiSizeBufferPoolMetrics") + name, std::make_shared<concordMetrics::Aggregator>()},
          overallUsedBuffersFailure_{component_.RegisterAtomicGauge("overallUsedBuffersFailure", 0)},
          overallUsedBuffersSuccess_{component_.RegisterAtomicGauge("overallUsedBuffersSuccess", 0)},
          overallUsedBuffers_{component_.RegisterAtomicGauge("overallUsedBuffers", 0)},
          overallMaxUsedBytes_{component_.RegisterAtomicGauge("overallMaxUsedBytes", 0)},
          currentUsedBytes_{component_.RegisterAtomicGauge("currentUsedBytes", 0)} {
      component_.Register();
    }

    void setAggregator(const std::shared_ptr<concordMetrics::Aggregator>& aggregator) {
      component_.SetAggregator(aggregator);
    }
    void updateAggregator() { component_.UpdateAggregator(); }
    concordMetrics::Component component_;

    // metrics handles

    // Number of times a buffer was given to caller, but caller couldn't used it since buffer wasn't big enough
    concordMetrics::AtomicGaugeHandle overallUsedBuffersFailure_;
    // Number of times a buffer was given to caller and could be used
    concordMetrics::AtomicGaugeHandle overallUsedBuffersSuccess_;
    // overallUsedBuffersFailure_+ overallUsedBuffersSuccess_
    concordMetrics::AtomicGaugeHandle overallUsedBuffers_;
    // Maximum sum of bytes used by all callers: sum of all used buffers
    concordMetrics::AtomicGaugeHandle overallMaxUsedBytes_;
    // current sum of bytes used by all callers: sum of all used buffers
    concordMetrics::AtomicGaugeHandle currentUsedBytes_;
  };

 public:
  // Members of the following struct are non-const due to the fact that some users need SubpoolConfig read/write and
  // some as read only. To set it as read only, use constness externally e 'const SubpoolConfig ...'
  struct SubpoolConfig {
    uint32_t bufferSize;         // size of data which can be used by a client
    uint32_t numInitialBuffers;  // number of buffers to allocate on creation
    uint32_t numMaxBuffers;      // maximum number of buffers that can be allocated (initially + dynamically)
  };
  using SubpoolsConfig = std::vector<MultiSizeBufferPool::SubpoolConfig>;

  struct Config {
    // The default timer interval, in seconds
    static constexpr uint32_t kDefaultBasePeriodicTimerIntervalSec = 1;

    // When maxAllocatedBytes is zero - do not enforce a limit on pool total size.
    // When non-zero - sum of total allocated initial chunks sizes should never exceed this parameter.
    // This parameter takes into account chunks = buffer size + header/trailer metadata.
    const uint64_t maxAllocatedBytes;

    // The next frequency-based members can be all disabled when set to 0. If non-zero: evaluation happens periodically
    // in a frequency multiplied by kDefaultBasePeriodicTimerIntervalSec
    const uint32_t purgeEvaluationFrequency;   // check if purge is needed
    const uint32_t statusReportFrequency;      // report statistics to log
    const uint32_t histogramsReportFrequency;  // take snapshot and write to log
    const uint32_t metricsReportFrequency;     // update metrics and aggregator

    // When true, log reporting is done for changes (vs previous report) only. If false, report is done no matter what.
    const bool enableStatusReportChangesOnly;

    /**
     * Minimum and maximum thresholds in the closed range [X,Y] which may trigger the subpool selector to switch to a
     * lower/higher level subpool.
     * If number of retries in a given period retriesMeasurePeriod is less than
     * subpoolSelectorEvaluationNumRetriesMinThreshold/subpoolSelectorEvaluationCounter, selector might decide to
     * move to a subpool with a smaller buffer size.
     * If number of retries in a given period is
     * more than subpoolSelectorEvaluationNumRetriesMaxThreshold/subpoolSelectorEvaluationCounter, selector might decide
     * to move to a subpool with a larger buffer size.
     * subpoolSelectorEvaluationNumRetriesMinThreshold must be less than
     * subpoolSelectorEvaluationNumRetriesMaxThreshold.
     */
    const uint32_t subpoolSelectorEvaluationNumRetriesMinThreshold;
    const uint32_t subpoolSelectorEvaluationNumRetriesMaxThreshold;
  };

  class ISubpoolSelector {
   public:
    ISubpoolSelector(const SubpoolsConfig& config) : config_{config} { ConcordAssert(!config_.empty()); };
    virtual ~ISubpoolSelector() = default;
    // Returns the selected subpool, cased on reports
    virtual const SubpoolConfig& selectSubpool() const = 0;
    virtual std::string_view name() const = 0;
    // Report about a buffer usage:
    // usedBufferSize - the size of buffer that was used by the caller
    // actualRequiredSize - the actual amount of bytes needed by caller. Usually less than usedBufferSize, but in case
    // of failure might be greater.
    virtual void reportBufferUsage(uint32_t usedBufferSize, uint32_t actualRequiredSize) = 0;

   protected:
    const SubpoolsConfig config_;
  };

  class IPurger {
   public:
    IPurger() = default;
    virtual ~IPurger() = default;
    virtual std::string_view name() const = 0;
    // Called periodically to evaluate the pool/sub-pools state. If purge is needed, it may raise a purge flag and a
    // purge limit during this evaluation. The decision if actual purging is done during evaluation, or only a flag is
    // raised is left for the future (// TODO GL).
    virtual void evaluate(MultiSizeBufferPool& pool) = 0;
  };

 protected:
  class BasicPurger : public IPurger {
   public:
    std::string_view name() const override { return "BasicPurger"; }
    void evaluate(MultiSizeBufferPool& pool) override{/* TODO, set purgeFlag_ and purgeLimit_ on each subpool */};
  };

  class MinimalSizeSubpoolSelector : public ISubpoolSelector {
   public:
    MinimalSizeSubpoolSelector(const SubpoolsConfig& config) : ISubpoolSelector(config) {}
    // comment: config_ is sorted by buffer size
    const SubpoolConfig& selectSubpool() const override { return *config_.begin(); }
    std::string_view name() const override { return "MinimalSizeSubpoolSelector"; }
    void reportBufferUsage(uint32_t usedBufferSize, uint32_t actualRequiredSize) override {
      ConcordAssertNE(actualRequiredSize, 0);
      // Ignore the report, since always choosing minimal
    }
  };

  struct Statistics {
    Statistics(bool enableStatusReportChangesOnly) : enableStatusReportChangesOnly_{enableStatusReportChangesOnly} {}
    void onChunkBecomeUsed(uint64_t chunkSize, bool chunkAllocatedFromHeap);
    enum class operationType { ALLOC, DELETE, PUSH };
    void onChunkBecomeUnused(uint64_t chunkSize, operationType opType);
    void onChunkAllocated(uint64_t chunkSize);
    void onChunkDeleted(uint64_t chunkSize);
    void onInit(uint32_t numMaxBuffers, uint32_t chunkSize);
    void setLimits(uint64_t maxAllocatedBytes);

    struct CurrentStateStatistics {
      // Pool level:
      // numAllocatedBytes_ + numNonAllocatedBytes_ = Config::maxAllocatedBytes + numAllocatedMetadataBytes_ (if non
      // zero) in pool level.
      // Subpool level:
      // numAllocatedBytes_ + numNonAllocatedBytes_ = SubpoolConfig::numMaxBuffers * (SubpoolConfig::bufferSize +
      // chunkMetadataSize())
      std::atomic_uint64_t numAllocatedBytes_{};
      std::atomic_uint64_t numNonAllocatedBytes_{};

      // numAllocatedBytes_ = numUnusedBytes_ + numUsedBytes_
      std::atomic_uint64_t numUsedBytes_{};
      std::atomic_uint64_t numUnusedBytes_{};

      // numAllocatedChunks_ + numNonAllocatedChunks_ = SubpoolConfig::numMaxBuffers
      std::atomic_uint64_t numAllocatedChunks_{};
      std::atomic_uint64_t numNonAllocatedChunks_{};

      // numAllocatedChunks_ = numUsedChunks_ + numUnusedChunks_
      std::atomic_uint64_t numUsedChunks_{};
      std::atomic_uint64_t numUnusedChunks_{};
    } current_;

    // Overall statistics are accumulating
    struct OverallStatistics {
      std::atomic_uint64_t numAllocatedBytes_{};
      std::atomic_uint64_t numDeletedBytes_{};
      std::atomic_uint64_t numUsedBytes_{};

      std::atomic_uint64_t numAllocatedChunks_{};
      std::atomic_uint64_t numDeletedChunks_{};
      std::atomic_uint64_t numUsedChunks_{};

      // Amount of times where a chunk did not fit into the needed buffer size and returned to pool without use
      std::atomic_uint64_t numBufferUsageFailed_{};
      // Amount of times where a chunk fit into the needed buffer size and returned to pool without use
      std::atomic_uint64_t numBufferUsageSuccess_{};

      std::atomic_uint64_t maxNumAllocatedBytes_{};
      std::atomic_uint64_t maxNumAllocatedChunks_{};
    } overall_;

    // relevant only if reportOnChangesOnly_ is true. Used to mark if the content need to be reported, only if there was
    // a change
    std::atomic_bool reportFlag_{false};
    // Report only if there were changes. This is in order to prevent endless reports into log when system is idle.
    const bool enableStatusReportChangesOnly_;
  };

  friend std::ostream& operator<<(std::ostream& os, const MultiSizeBufferPool::Statistics& s);

  struct ChunkHeader {
    ChunkHeader(uint32_t bufferSize) : magicField_{kMagicField}, bufferSize_(bufferSize) {}
    static constexpr int kMagicField = 0xFCFCFCFA;
    const int magicField_;
    // buffer size to be used by client, does not include chunkHeaderSize() since this part is fixed.
    const uint32_t bufferSize_;
  };

  struct ChunkTrailer {
    ChunkTrailer() : magicField_{kMagicField} {}
    static constexpr int kMagicField = 0xFAFAFAFC;
    const int magicField_;
  };

  class SubPool {
    friend class concordUtil::test::MultiSizeBufferPoolTestFixture;  // Should be strictly used for testing!
    friend class IPurger;

   public:
    virtual ~SubPool();
    SubPool() = delete;
    SubPool(const SubPool&) = delete;
    SubPool(SubPool&&) = delete;
    SubPool& operator=(const SubPool&) = delete;
    SubPool& operator=(SubPool&&) = delete;

    SubPool(logging::Logger& logger,
            const SubpoolConfig& config,
            std::string&& name,
            const MultiSizeBufferPool::Config& poolConfig,
            Statistics& poolStats);

   protected:
    boost::lockfree::queue<char*, boost::lockfree::fixed_sized<true>> chunks_;
    std::condition_variable waitForAvailChunkCond_;
    std::mutex waitForAvailChunkMutex_;
    const SubpoolConfig config_;

    std::atomic_bool stopFlag_;
    std::atomic_bool purgeFlag_;
    std::atomic_uint64_t purgeLimit_;
    const logging::Logger& logger_;
    std::string name_;
    const uint32_t chunkSize_;
    Statistics stats_;
    Statistics& poolStats_;

   public:
    enum class AllocationStatus { SUCCESS, EXCEED_MAX_BUFFERS, EXCEED_POOL_MAX_BYTES };
    using AllocationResult = std::pair<char*, AllocationStatus>;

    // TODO - we can extend this pool capabilities, and add timeoutMilli if needed (currently not implemented)
    AllocationResult getChunk(bool wait, bool allocate);
    void returnChunk(char* chunk);
    AllocationResult allocateChunk();
    void reportBufferUsage(char* chunk, uint32_t usageSize);
    void statusReport();
    void setPurgeState(bool purgeFlag, uint64_t purgeLimit = 0) {
      purgeFlag_ = purgeFlag;
      purgeLimit_ = purgeLimit;
    }

   protected:
    using CurrentStateStatistics = MultiSizeBufferPool::Statistics::CurrentStateStatistics;
    using OverallStatistics = MultiSizeBufferPool::Statistics::OverallStatistics;

    void deleteChunk(char*& chunk);
    void pushChunk(char* chunk);

    const MultiSizeBufferPool::Config& poolConfig_;
  };  // class SubPool

 public:
  using DefaultSubpoolSelector = MinimalSizeSubpoolSelector;
  using DefaultPurger = BasicPurger;

  /**
   *       Public API
   */

  // SubpoolsConfig must apply the configurations in an ascending order by buffer size.
  MultiSizeBufferPool(std::string_view name,
                      Timers& timers,
                      const SubpoolsConfig& subpoolsConfig,  // must be sorted by an ascending buffer size
                      const MultiSizeBufferPool::Config& config,
                      std::unique_ptr<ISubpoolSelector>&& subpoolSelector = nullptr,
                      std::unique_ptr<IPurger>&& purger = nullptr);
  virtual ~MultiSizeBufferPool();
  MultiSizeBufferPool() = delete;
  MultiSizeBufferPool(const MultiSizeBufferPool&) = delete;
  MultiSizeBufferPool(MultiSizeBufferPool&&) = delete;
  MultiSizeBufferPool& operator=(const MultiSizeBufferPool&) = delete;
  MultiSizeBufferPool& operator=(MultiSizeBufferPool&&) = delete;

  // Allocate by subpool selector - should be usually 1st choice of call. Should always succeed, never throw. Might
  // block till buffer is freed by another thread if all buffers are occupied.
  // Returns a pair: the allocated buffer and it's
  // size.
  std::pair<char*, uint32_t> getBufferBySubpoolSelector();

  // Allocate by minimal size - should be usually 2nd choice of call (after failure to call getBufferBySubpoolSelector)
  // minBufferSize is to make sure that returned buffer is at least of that given size. Throws if minBufferSize is
  // greater than the maximal subpool buffer size defined on creation.
  // Returns a pair: the allocated buffer and it's size.
  std::pair<char*, uint32_t> getBufferByMinBufferSize(uint32_t minBufferSize);

  // An optional call which can be used for statistics (internal fragmentation) or by the subpool selector and purger
  // buffer must be a currently used buffer.
  // buffer size is extracted from buffer:
  // 1) if usageSize > buffer size, caller failed to use the given buffer.
  // 2) if usageSize <= buffer size, caller succeed to use the given buffer.
  void reportBufferUsage(char* buffer, uint32_t usageSize);

  // Return a buffer to the pool
  void returnBuffer(char* buffer);

  // Used by purger: for a given subpool, set purge flag and purge limit. If purge flag is false, limit is irrelevant
  void setPurgeState(uint64_t subPoolBufferSize, bool purgeFlag, uint64_t purgeLimit = 0);

  // Return in an ascending order a vector of subpools buffer sizes
  const std::vector<uint64_t>& getSubpoolsBufferSizes() const { return subPoolsBufferSizes_; }

  void setAggregator(const std::shared_ptr<concordMetrics::Aggregator>& aggregator) {
    metrics_.setAggregator(aggregator);
  }

 protected:
  std::pair<char*, uint32_t> popBuffer(uint32_t bufferSize);
  void statusReport();
  void doPeriodic();
  void validateConfiguration(const MultiSizeBufferPool::Config& config, const SubpoolsConfig& subpoolsConfig);
  SubPool& getSubPool(uint32_t bufferSize) const;
  // Chunk is used internally, and holds the user data + metadata
  // buffer is the memory location (pointer) returned to user
  static char* chunkToBuffer(const char* chunk) { return const_cast<char*>(chunk) + chunkHeaderSize(); }
  static char* bufferToChunk(const char* buffer) { return const_cast<char*>(buffer) - chunkHeaderSize(); }
  static ChunkTrailer* getChunkTrailer(const char* chunk, uint32_t chunkSize) {
    return reinterpret_cast<ChunkTrailer*>(const_cast<char*>(chunk) + chunkSize - chunkTrailerSize());
  }
  static uint32_t getBufferSize(const char* buffer);  // assume buffer is valid
  static uint32_t chunkHeaderSize() { return static_cast<uint32_t>(sizeof(ChunkHeader)); }
  static uint32_t chunkTrailerSize() { return static_cast<uint32_t>(sizeof(ChunkTrailer)); }
  static uint32_t chunkMetadataSize() { return chunkHeaderSize() + chunkTrailerSize(); }
  static std::pair<bool, std::string> validateBuffer(const char* buffer);

  struct HistogramRecorders {
    static constexpr uint64_t MAX_BUFFER_SIZE = 32ULL * 1024ULL * 1024ULL;            // 32MB
    static constexpr uint64_t MAX_DURATION_MICROSECONDS = 60ULL * 1000ULL * 1000ULL;  // 60 sec
    HistogramRecorders(const std::string&& name) : name_(std::move(name)) {
      auto& registrar = concord::diagnostics::RegistrarSingleton::getInstance();
      // common component
      registrar.perf.registerComponent(name,
                                       {time_in_getBufferBySubpoolSelector,
                                        time_in_getBufferByMinBufferSize,
                                        time_in_reportBufferUsage,
                                        time_in_returnBuffer,
                                        // size of buffer given to caller
                                        caller_allocated_buffer_size,
                                        // actual size of buffer used by caller
                                        caller_allocated_buffer_usage});
    };

    ~HistogramRecorders() {
      auto& registrar = concord::diagnostics::RegistrarSingleton::getInstance();
      registrar.perf.unRegisterComponent(name_);
    }

    DEFINE_SHARED_RECORDER(
        time_in_getBufferBySubpoolSelector, 1, MAX_DURATION_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(
        time_in_getBufferByMinBufferSize, 1, MAX_DURATION_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(
        time_in_reportBufferUsage, 1, MAX_DURATION_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(
        time_in_returnBuffer, 1, MAX_DURATION_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(caller_allocated_buffer_size, 1, MAX_BUFFER_SIZE, 3, concord::diagnostics::Unit::BYTES);
    DEFINE_SHARED_RECORDER(caller_allocated_buffer_usage, 1, MAX_BUFFER_SIZE, 3, concord::diagnostics::Unit::BYTES);

    std::string name_;
  };

 protected:
  size_t periodicCounter_;
  std::map<uint32_t, std::unique_ptr<SubPool>> subpools_;
  logging::Logger logger_;
  std::set<uint32_t> bufferSizes_;
  std::string name_;
  const Config config_;
  const SubpoolsConfig subPoolsConfig_;
  std::unique_ptr<ISubpoolSelector> subPoolSelector_;
  std::unique_ptr<IPurger> purger_;
  Statistics stats_;
  HistogramRecorders histograms_;
  MultiSizeBufferPoolMetrics metrics_;
  Timers& timers_;
  Timers::Handle periodicTimer_;
  std::vector<uint64_t> subPoolsBufferSizes_;
};  // class MultiSizeBufferPool

}  // namespace concordUtil
