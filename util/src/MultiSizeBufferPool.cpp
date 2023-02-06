// Concord
//
// Copyright (c) 2018-2023 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may iMultiSizeBufferPoolnclude a number of subcomponents with separate copyright notices and license
// terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted
// in the LICENSE file.

#include "MultiSizeBufferPool.hpp"
#include "kvstream.h"

#include <thread>
#include <future>
#include <algorithm>
#include <string_view>
#include <cstdarg>
#include <optional>

namespace concordUtil {

using namespace std;
using namespace std::chrono;
using TimeRecorder = concord::diagnostics::TimeRecorder<true>;

// uncomment to add debug prints which are too expensive to skip during run time
// #define MultiSizeBufferPool_DO_DEBUG
#undef DEBUG_PRINT
#ifdef MultiSizeBufferPool_DO_DEBUG
#define DEBUG_PRINT(x, y) LOG_INFO(x, y)
#else
#define DEBUG_PRINT(x, y)
#endif

#define KVLOG_PREFIX name_, std::this_thread::get_id()

/* ==========================================
===
===     Statics / Globals
===
========================================== */
template <typename T>
static void throwException(const logging::Logger& logger,
                           const std::string& prefix_message,
                           const std::string& args_formatted_string) {
  LOG_ERROR(logger, prefix_message << args_formatted_string);
  throw T(__PRETTY_FUNCTION__ + std::string(" :") + prefix_message + args_formatted_string);
}

static inline std::ostream& operator<<(std::ostream& os, const MultiSizeBufferPool::Config& c) {
  os << " Configuration:"
     << KVLOG(c.kDefaultBasePeriodicTimerIntervalSec,
              c.maxAllocatedBytes,
              c.purgeEvaluationFrequency,
              c.statusReportFrequency,
              c.histogramsReportFrequency,
              c.metricsReportFrequency,
              c.enableStatusReportChangesOnly,
              c.subpoolSelectorEvaluationNumRetriesMinThreshold,
              c.subpoolSelectorEvaluationNumRetriesMaxThreshold);
  return os;
}

static inline std::ostream& operator<<(std::ostream& os, const MultiSizeBufferPool::SubpoolConfig& s) {
  os << " SubpoolConfig:" << KVLOG(s.bufferSize, s.numInitialBuffers, s.numMaxBuffers);
  return os;
}

inline std::ostream& operator<<(std::ostream& os, const MultiSizeBufferPool::Statistics& s) {
  const auto& c = s.current_;
  const auto& o = s.overall_;
  os << " Current Statistics: ["
     << KVLOG(c.numAllocatedBytes_,
              c.numNonAllocatedBytes_,
              c.numUnusedBytes_,
              c.numUsedBytes_,
              c.numAllocatedChunks_,
              c.numNonAllocatedChunks_,
              c.numUsedChunks_,
              c.numUnusedChunks_)
     << "]";
  os << " Overall Statistics: ["
     << KVLOG(o.numAllocatedBytes_,
              o.numDeletedBytes_,
              o.numUsedBytes_,
              o.numAllocatedChunks_,
              o.numDeletedChunks_,
              o.numUsedChunks_,
              o.numBufferUsageFailed_,
              o.numBufferUsageSuccess_,
              o.maxNumAllocatedBytes_,
              o.maxNumAllocatedChunks_)
     << "]";
  return os;
}

uint32_t MultiSizeBufferPool::getBufferSize(const char* buffer) {
  char* chunk = bufferToChunk(buffer);
  auto* chunkHeader = reinterpret_cast<MultiSizeBufferPool::ChunkHeader*>(chunk);
  return chunkHeader->bufferSize_;
}

/* ==========================================
===
===     MultiSizeBufferPool Definition
===
========================================== */

MultiSizeBufferPool::~MultiSizeBufferPool() {
  timers_.cancel(periodicTimer_);
  // This one triggers all subpools dtors
  // Important: we assume here that this is user responsibilty to make sure no more waiting threads.
  // Checking and warning if all chunks have been returned is done in Subpool dtor level, triggered in the next line.
  subpools_.clear();
  LOG_INFO(logger_, KVLOG(KVLOG_PREFIX) << " MultiSizeBufferPool destroyed");
}

void MultiSizeBufferPool::validateConfiguration(const MultiSizeBufferPool::Config& config,
                                                const SubpoolsConfig& subpoolsConfig) {
  if (subpoolsConfig.empty()) {
    throwException<std::invalid_argument>(
        logger_,
        "Invalid subpools configuration: Must have at least a single subpool configuration!",
        KVLOG(KVLOG_PREFIX));
  }

  // Make sure that initial allocation (in bytes) is less than configured maxAllocatedBytes (if enabled)
  if (config_.maxAllocatedBytes != 0) {
    uint64_t initialNumAllocatedChunksBytes{};
    for (const auto& subpoolConfig : subpoolsConfig) {
      initialNumAllocatedChunksBytes +=
          (subpoolConfig.bufferSize + chunkMetadataSize()) * subpoolConfig.numInitialBuffers;
    }
    if (initialNumAllocatedChunksBytes > config_.maxAllocatedBytes) {
      throwException<std::invalid_argument>(
          logger_,
          "Initial pool size is too large:",
          KVLOG(KVLOG_PREFIX, initialNumAllocatedChunksBytes, config_.maxAllocatedBytes));
    }
  }

  if ((config.subpoolSelectorEvaluationNumRetriesMaxThreshold <=
       config.subpoolSelectorEvaluationNumRetriesMinThreshold) ||
      (config.subpoolSelectorEvaluationNumRetriesMinThreshold == 0)) {
    throwException<std::invalid_argument>(logger_,
                                          "Invalid subpool selector configuration",
                                          KVLOG(KVLOG_PREFIX,
                                                config.subpoolSelectorEvaluationNumRetriesMaxThreshold,
                                                config.subpoolSelectorEvaluationNumRetriesMinThreshold));
  }

  std::optional<uint32_t> lastBufferSize;
  for (const auto& subpoolConfig : subpoolsConfig) {
    if ((lastBufferSize != std::nullopt) && (subpoolConfig.bufferSize <= lastBufferSize)) {
      throwException<std::invalid_argument>(logger_,
                                            "Invalid pool subpools configuration: bufferSize must be ascending!",
                                            KVLOG(KVLOG_PREFIX, subpoolConfig.bufferSize, lastBufferSize.value()));
    }
    lastBufferSize = subpoolConfig.bufferSize;
  }
}

MultiSizeBufferPool::MultiSizeBufferPool(std::string_view name,
                                         Timers& timers,
                                         const SubpoolsConfig& subpoolsConfig,
                                         const MultiSizeBufferPool::Config& config,
                                         std::unique_ptr<ISubpoolSelector>&& subpoolSelector,
                                         std::unique_ptr<IPurger>&& purger)
    : logger_{logging::getLogger("concord.memory.pool")},
      name_{name},
      config_{config},
      subPoolsConfig_{subpoolsConfig},
      stats_(config_.enableStatusReportChangesOnly),
      histograms_(std::string("multi_buffer_size_pool_") + std::string(name)),
      metrics_{std::string(name)},
      timers_{timers} {
  std::string subPoolName;
  std::optional<uint32_t> lastBufferSize;

  validateConfiguration(config, subpoolsConfig);
  for (const auto& subpoolConfig : subpoolsConfig) {
    lastBufferSize = subpoolConfig.bufferSize;
    bufferSizes_.insert(lastBufferSize.value());
    subPoolName = name_ + "_" + to_string(lastBufferSize.value());

    auto [_, insertFlag] =
        subpools_.emplace(std::make_pair(lastBufferSize.value(),
                                         std::make_unique<MultiSizeBufferPool::SubPool>(
                                             logger_, subpoolConfig, std::move(subPoolName), config_, stats_)));
    UNUSED(_);
    ConcordAssert(insertFlag == true);
  }  // for

  // Set limits on statistics
  stats_.setLimits(config_.maxAllocatedBytes);

  if ((config_.purgeEvaluationFrequency != 0) || (config_.statusReportFrequency != 0) ||
      (config_.histogramsReportFrequency != 0) || (config_.metricsReportFrequency != 0)) {
    periodicTimer_ = timers.add(std::chrono::seconds{config_.kDefaultBasePeriodicTimerIntervalSec},
                                Timers::Timer::RECURRING,
                                [this](Timers::Handle h) { this->doPeriodic(); });
  } else {
    LOG_WARN(logger_, "Periodic timer is disabled!" << KVLOG(KVLOG_PREFIX));
  }

  // Set/create subpool selector and purger
  subPoolSelector_ =
      subpoolSelector ? std::move(subpoolSelector) : std::make_unique<DefaultSubpoolSelector>(subpoolsConfig);
  purger_ = purger ? std::move(purger) : std::make_unique<DefaultPurger>();

  // initialize subpools buffer sizes vector
  for (const auto& spc : subPoolsConfig_) subPoolsBufferSizes_.push_back(spc.bufferSize);
  std::sort(subPoolsBufferSizes_.begin(), subPoolsBufferSizes_.end());

  LOG_INFO(logger_,
           "Done creating pool and allocating all Sub-Memory Pools:"
               << KVLOG(KVLOG_PREFIX, subpoolsConfig.size()) << config_
               << ". Subpool Selector:" << subPoolSelector_->name() << ". Purger:" << purger_->name() << stats_);
}

MultiSizeBufferPool::SubPool& MultiSizeBufferPool::getSubPool(uint32_t bufferSize) const {
  auto iter = subpools_.find(bufferSize);
  ConcordAssert(iter != subpools_.end());  // internal bug or memory corrupted - caller must validate
  return *iter->second.get();
}

std::pair<char*, uint32_t> MultiSizeBufferPool::popBuffer(uint32_t requestedBufferSize) {
  SubPool::AllocationResult result{};
  uint32_t allocatedBufferSize{};
  bool allocate{true};

  DEBUG_PRINT(logger_, "pop enter:" << KVLOG(KVLOG_PREFIX));
  /** 1st stage: repetitive, until reaching the largest subpool:
   * 1a) Attempt to obtain a chunk from the target subpool that matches bufferSize.
   * 1b) If such chunk is found - return chunk and exit (success), If not continue to 1c.
   * 1c) If there are no available chunks in the target subpool, try to allocate one from the next subpool (with a
   * larger chunk size), repeat 1a->1c. If reached largest subpool, move to stage 2. 2nd stage: 2a) Go back to the
   * target subpool (which matches bufferSize). 2b) Try to allocate a new chunk and/or wait for chunk to be released by
   * another consumer. This stage is blocking so a chunk must be found at some time.
   */
  auto iter = bufferSizes_.find(requestedBufferSize);
  ConcordAssertNE(iter, bufferSizes_.end());
  if (iter == bufferSizes_.end())
    throwException<std::invalid_argument>(logger_, "Invalid buffer size!", KVLOG(KVLOG_PREFIX, requestedBufferSize));

  do {
    result = getSubPool(*iter).getChunk(false, allocate);
    if (result.first) {
      allocatedBufferSize = *iter;
      break;
    }
    // continue on all errors codes (try next subpool)
    allocate = false;
    ++iter;
  } while (iter != bufferSizes_.end());

  if (result.second == SubPool::AllocationStatus::EXCEED_POOL_MAX_BYTES) {
    LOG_WARN(logger_,
             "The pool has reached the maximum pool total bytes allocation threshold (net buffers size "
             "plus metadata)!"
                 << KVLOG(KVLOG_PREFIX, config_.maxAllocatedBytes) << stats_);
  }
  if (!result.first) {
    // 2nd stage, if didn't get a buffer: wait infinitely on the initial subpool until a chunk is allocated
    result = getSubPool(requestedBufferSize).getChunk(true, true);
    allocatedBufferSize = requestedBufferSize;
  }

  DEBUG_PRINT(logger_, "pop exit:" << KVLOG(KVLOG_PREFIX));
  if (result.first) {
    ConcordAssertGT(allocatedBufferSize, 0);
    return std::make_pair(chunkToBuffer(result.first), allocatedBufferSize);
  }
  return std::make_pair(nullptr, 0);
}

std::pair<char*, uint32_t> MultiSizeBufferPool::getBufferBySubpoolSelector() {
  TimeRecorder scoped_timer(*histograms_.time_in_getBufferBySubpoolSelector);
  DEBUG_PRINT(logger_, "getBufferBySubpoolSelector enter:" << KVLOG(KVLOG_PREFIX));
  const auto& subpoolConfig{subPoolSelector_->selectSubpool()};
  auto result = popBuffer(subpoolConfig.bufferSize);
  DEBUG_PRINT(logger_, "getBufferBySubpoolSelector exit:" << KVLOG(KVLOG_PREFIX) << stats_);
  return result;
}

void MultiSizeBufferPool::returnBuffer(char* buffer) {
  TimeRecorder scoped_timer(*histograms_.time_in_returnBuffer);
  DEBUG_PRINT(logger_, "returnBuffer enter:" << KVLOG(KVLOG_PREFIX));
  if (auto [result, errMsg] = validateBuffer(buffer); !result) {
    throwException<std::invalid_argument>(logger_, errMsg, KVLOG(KVLOG_PREFIX, (void*)buffer));
  }
  auto subpoolBufferSize = getBufferSize(buffer);
  auto chunk = bufferToChunk(buffer);
  getSubPool(subpoolBufferSize).returnChunk(chunk);
  DEBUG_PRINT(logger_, "returnBuffer exit:" << KVLOG(KVLOG_PREFIX) << stats_);
}

// Used by purger: for a given subpool, set purge flag and purge limit. If purge flag is false, limit is irrelevant
void MultiSizeBufferPool::setPurgeState(uint64_t subPoolBufferSize, bool purgeFlag, uint64_t purgeLimit) {
  auto iter = subpools_.find(subPoolBufferSize);
  if (iter == subpools_.end()) {
    throwException<std::invalid_argument>(
        logger_, "Invalid subPoolBufferSize!", KVLOG(KVLOG_PREFIX, subPoolBufferSize));
  }
  iter->second->setPurgeState(purgeFlag, purgeLimit);
}

void MultiSizeBufferPool::reportBufferUsage(char* buffer, uint32_t usageSize) {
  TimeRecorder scoped_timer(*histograms_.time_in_reportBufferUsage);
  if (usageSize == 0) {
    throwException<std::invalid_argument>(logger_, "Invalid resultSize (must be non-zero)!", KVLOG(KVLOG_PREFIX));
  }
  if (auto [result, errMsg] = validateBuffer(buffer); !result) {
    throwException<std::invalid_argument>(logger_, errMsg, KVLOG(KVLOG_PREFIX, (void*)buffer));
  }

  auto bufferSize = getBufferSize(buffer);
  auto chunk = bufferToChunk(buffer);
  getSubPool(bufferSize).reportBufferUsage(chunk, usageSize);
  subPoolSelector_->reportBufferUsage(bufferSize, usageSize);
  if (bufferSize >= usageSize) {
    histograms_.caller_allocated_buffer_size->record(bufferSize);
    histograms_.caller_allocated_buffer_usage->record(usageSize);
  }
}

std::pair<char*, uint32_t> MultiSizeBufferPool::getBufferByMinBufferSize(uint32_t minBufferSize) {
  TimeRecorder scoped_timer(*histograms_.time_in_getBufferByMinBufferSize);

  // lower_bound returns an iterator to the 1st size in bufferSizes_ which is no less than  requested chunkSize
  // if not such, it returns last iterator
  DEBUG_PRINT(logger_, "getBufferByMinBufferSize enter:" << KVLOG(KVLOG_PREFIX));
  auto lowestSizeToFitIter = bufferSizes_.lower_bound(minBufferSize);
  if (lowestSizeToFitIter == bufferSizes_.end()) {
    auto maxSubPoolBufferSize = *bufferSizes_.rbegin();
    throwException<std::invalid_argument>(
        logger_, "Invalid requested bufferSize:", KVLOG(KVLOG_PREFIX, minBufferSize, maxSubPoolBufferSize));
  }
  auto result = popBuffer(*lowestSizeToFitIter);
  DEBUG_PRINT(logger_, "getBufferByMinBufferSize exit:" << KVLOG(KVLOG_PREFIX) << stats_);
  return result;
}

void MultiSizeBufferPool::doPeriodic() {
  // This is a timer callback: here the pool reports statistics and evaluate its state (for enabled attributes)
  std::future<void> purgeFuture, statusReportFuture, histogramsReportFuture, metricsReportFuture;
  ++periodicCounter_;
  if ((config_.purgeEvaluationFrequency > 0) && (periodicCounter_ % config_.purgeEvaluationFrequency) == 0) {
    purgeFuture = std::async(std::launch::async, [this]() {
      purger_->evaluate(*this);  // TODO , an empty call, nothing passed yet and no API to pool defined.
    });
  }
  if ((config_.statusReportFrequency > 0) && (periodicCounter_ % config_.statusReportFrequency) == 0) {
    // Report statistics to log
    statusReportFuture = std::async(std::launch::async, [this]() { this->statusReport(); });
  }
  if ((config_.histogramsReportFrequency > 0) && (periodicCounter_ % config_.histogramsReportFrequency) == 0) {
    // report histograms to log
    histogramsReportFuture = std::async(std::launch::async, [this]() {
      auto& registrar = concord::diagnostics::RegistrarSingleton::getInstance();
      registrar.perf.snapshot(name_);
      LOG_INFO(logger_, "Histograms report:" << registrar.perf.toString(registrar.perf.get(name_)));
    });
  }
  if ((config_.metricsReportFrequency > 0) && (periodicCounter_ % config_.metricsReportFrequency) == 0) {
    // update metrics /  update aggregator
    metricsReportFuture = std::async(std::launch::async, [this]() {
      metrics_.overallMaxUsedBytes_.Get().Set(stats_.overall_.maxNumAllocatedBytes_);
      metrics_.currentUsedBytes_.Get().Set(stats_.current_.numUsedBytes_);
      auto numBufferUsageFailed = stats_.overall_.numBufferUsageFailed_.load();
      auto numBufferUsageSuccess = stats_.overall_.numBufferUsageSuccess_.load();
      metrics_.overallUsedBuffersFailure_.Get().Set(numBufferUsageFailed);
      metrics_.overallUsedBuffersSuccess_.Get().Set(numBufferUsageSuccess);
      metrics_.overallUsedBuffers_.Get().Set(numBufferUsageFailed + numBufferUsageSuccess);
      this->metrics_.updateAggregator();
    });
  }
  // All was done by worker threads, now wait for all
  if (purgeFuture.valid()) purgeFuture.wait();
  if (statusReportFuture.valid()) statusReportFuture.wait();
  if (histogramsReportFuture.valid()) histogramsReportFuture.wait();
  if (metricsReportFuture.valid()) metricsReportFuture.wait();
}

void MultiSizeBufferPool::statusReport() {
  if (!config_.enableStatusReportChangesOnly || stats_.reportFlag_) {
    LOG_INFO(logger_, KVLOG(KVLOG_PREFIX) << " statusReport:" << stats_);
    stats_.reportFlag_ = false;
  }
  for (const auto& [_, sp] : subpools_) {
    UNUSED(_);
    sp->statusReport();
  }
}

std::pair<bool, std::string> MultiSizeBufferPool::validateBuffer(const char* buffer) {
  if (!buffer) return {false, "buffer is null"};
  auto chunk = bufferToChunk(buffer);
  auto chunkHeader = reinterpret_cast<MultiSizeBufferPool::ChunkHeader*>(chunk);
  auto chunkTrailer = getChunkTrailer(chunk, chunkHeader->bufferSize_ + chunkMetadataSize());
  if (chunkHeader->bufferSize_ == 0) return {false, "bufferSize_ is 0"};
  if (chunkHeader->magicField_ != MultiSizeBufferPool::ChunkHeader::kMagicField)
    return {false, "invalid header magicField_!"};
  if (chunkTrailer->magicField_ != MultiSizeBufferPool::ChunkTrailer::kMagicField)
    return {false, "invalid trailer magicField_!"};
  return {true, ""};
}

/* ==========================================
===
===     MultiSizeBufferPool::SubPool
===
========================================== */

MultiSizeBufferPool::SubPool::SubPool(logging::Logger& logger,
                                      const SubpoolConfig& config,
                                      std::string&& name,
                                      const MultiSizeBufferPool::Config& poolConfig,
                                      Statistics& poolStats)
    : chunks_{config.numMaxBuffers},
      config_{config},
      purgeFlag_{false},
      purgeLimit_{},
      logger_{logger},
      name_{std::move(name)},
      chunkSize_(config_.bufferSize + chunkMetadataSize()),
      stats_{poolConfig.enableStatusReportChangesOnly},
      poolStats_{poolStats},
      poolConfig_{poolConfig} {
  auto throwException_wrapper = [this](const std::string& msg) {
    concordUtil::throwException<std::invalid_argument>(
        logger_, msg, KVLOG(KVLOG_PREFIX, config_.bufferSize, config_.numInitialBuffers, config_.numMaxBuffers));
  };
  if (config_.bufferSize == 0) {
    throwException_wrapper("Wrong parameters specified (bufferSize == 0):");
  }
  if (config_.numInitialBuffers > config_.numMaxBuffers)
    throwException_wrapper("Wrong parameters specified (numInitialBuffers > numMaxBuffers):");
  if ((config_.numInitialBuffers == 0) && (config_.numMaxBuffers == config_.numInitialBuffers))
    throwException_wrapper("Wrong parameters specified (numInitialBuffers == numMaxBuffers == 0):");
  stats_.onInit(config_.numMaxBuffers, chunkSize_);
  stats_.setLimits(poolConfig_.maxAllocatedBytes);
  poolStats.onInit(config_.numMaxBuffers, chunkSize_);
  for (size_t i{}; i < config_.numInitialBuffers; ++i) {
    // Allocate a buffer of size chunkSize_ and store a pointer for the user in the pool
    const auto result = allocateChunk();
    ConcordAssertAND(result.first != nullptr, result.second == AllocationStatus::SUCCESS);
    stats_.onChunkBecomeUnused(chunkSize_, Statistics::operationType::ALLOC);
    poolStats_.onChunkBecomeUnused(chunkSize_, Statistics::operationType::ALLOC);
    pushChunk(result.first);
  }
  LOG_INFO(logger_, "Done creating Sub-Memory Pool" << KVLOG(KVLOG_PREFIX, chunkSize_) << config_ << stats_);
}

MultiSizeBufferPool::SubPool::~SubPool() {
  {
    std::lock_guard<std::mutex> lock(waitForAvailChunkMutex_);
    for (SubPool::AllocationResult result = getChunk(false, false); result.first != nullptr;
         result = getChunk(false, false)) {
      deleteChunk(result.first);
    }
  }

  // important comment: subpool is destroyed. It is the user responsibilty to make sure there are not waiting threads.
  // If not -assume we have 2 groups of threads. group A is waiting threads (on conditional variable) and B is the
  // consuming threads which currently user the buffers. B will leak and crash. so no reason to free A. pool is doomed
  // (it is destroyed now) due to improper use.

  // check for a leak / inconsistency
  if (stats_.current_.numUnusedChunks_ != 0) {
    LOG_FATAL(logger_, "numUnusedChunks_ is non-zero!" << KVLOG(KVLOG_PREFIX) << stats_);
    ConcordAssert(false);
  }
  // we can still only leak of no waiting threads, at least warn. Cannot throw here.
  if (stats_.current_.numAllocatedChunks_ != stats_.current_.numUnusedChunks_) {
    auto kvlogArgs = KVLOG(stats_.current_.numAllocatedChunks_, stats_.current_.numUnusedChunks_);
    LOG_ERROR(logger_, "There is probably a memory leak! Not all chunks returned to subpool:" << kvlogArgs);
  }
  LOG_DEBUG(logger_, "Subpool destroyed:" << KVLOG(KVLOG_PREFIX, config_.bufferSize));
}

void MultiSizeBufferPool::SubPool::reportBufferUsage(char* chunk, uint32_t usageSize) {
  auto* chunkHeader = reinterpret_cast<MultiSizeBufferPool::ChunkHeader*>(chunk);
  DEBUG_PRINT(logger_, "Report buffer usage:" << KVLOG(KVLOG_PREFIX, chunkHeader->bufferSize_, usageSize));
  if (usageSize > chunkHeader->bufferSize_) {
    ++stats_.overall_.numBufferUsageFailed_;
    ++poolStats_.overall_.numBufferUsageFailed_;
  } else {
    ++stats_.overall_.numBufferUsageSuccess_;
    ++poolStats_.overall_.numBufferUsageSuccess_;
  }
}

void MultiSizeBufferPool::SubPool::deleteChunk(char*& chunk) {
  DEBUG_PRINT(logger_, "deleteChunk enter:" << KVLOG(KVLOG_PREFIX, (void*)chunk));
  delete[] chunk;
  stats_.onChunkDeleted(chunkSize_);
  poolStats_.onChunkDeleted(chunkSize_);

  DEBUG_PRINT(logger_, "deleteChunk exit:" << KVLOG(KVLOG_PREFIX, (void*)chunk) << stats_);
  chunk = nullptr;
}

MultiSizeBufferPool::SubPool::AllocationResult MultiSizeBufferPool::SubPool::getChunk(bool wait, bool allocate) {
  AllocationResult result{nullptr, AllocationStatus::SUCCESS};
  auto& chunk = result.first;
  bool chunkAllocatedFromHeap{false};

  DEBUG_PRINT(logger_, "getChunk enter" << KVLOG(KVLOG_PREFIX));

  if (!chunks_.pop(chunk)) {
    // No available chunks => allocate a new one from the heap, if permitted
    if (allocate) {
      // allowed to allocate case:
      result = allocateChunk();
      chunkAllocatedFromHeap = (chunk != nullptr);
    }
    if (!chunk && wait) {
      // allowed to wait case:
      // Pool size limit has been reached: wait until some chunk gets released. The wait is done infinity, until a
      // chunk is freed by another thread.
      // why not stop? Since the only simple way to make a thread to go out is by pushing back a chunk.
      unique_lock<mutex> lock(waitForAvailChunkMutex_);
      bool isChunkAvailable{false};
      const auto waitFunc = [&, this]() {
        isChunkAvailable = chunks_.pop(chunk);
        return isChunkAvailable;
      };

      DEBUG_PRINT(logger_, "Wait for chunk:" << KVLOG(KVLOG_PREFIX));
      waitForAvailChunkCond_.wait(lock, waitFunc);
      DEBUG_PRINT(logger_, "Done wait for chunk:" << KVLOG(KVLOG_PREFIX));
    }
  }
  if (chunk) {
    stats_.onChunkBecomeUsed(chunkSize_, chunkAllocatedFromHeap);
    poolStats_.onChunkBecomeUsed(chunkSize_, chunkAllocatedFromHeap);
    DEBUG_PRINT(logger_, "Chunk popped for use:" << KVLOG(KVLOG_PREFIX, (void*)chunk) << stats_);
  }
  DEBUG_PRINT(logger_, "getChunk exit" << KVLOG(KVLOG_PREFIX));
  return result;
}

void MultiSizeBufferPool::SubPool::returnChunk(char* chunk) {
  DEBUG_PRINT(logger_, "returnChunk enter" << KVLOG(KVLOG_PREFIX));
  if ((stats_.current_.numUsedChunks_ == 0) || (stats_.current_.numUnusedChunks_ == config_.numMaxBuffers)) {
    std::stringstream ss;
    ss << KVLOG(KVLOG_PREFIX, config_.numMaxBuffers, (void*)chunk) << stats_;
    throwException<std::runtime_error>(logger_, "No more chunks are marked as unused:", ss.str());
  }

  bool shouldPurge = (purgeFlag_ && (stats_.current_.numAllocatedChunks_ > purgeLimit_));
  auto opType = shouldPurge ? Statistics::operationType::DELETE : Statistics::operationType::PUSH;
  stats_.onChunkBecomeUnused(chunkSize_, opType);
  poolStats_.onChunkBecomeUnused(chunkSize_, opType);
  // Comment: no locking over numAllocatedChunks_ vs here - worst case - purge a little more
  if (shouldPurge)
    deleteChunk(chunk);
  else
    pushChunk(chunk);
  DEBUG_PRINT(logger_, "returnChunk exit" << KVLOG(KVLOG_PREFIX));
}

void MultiSizeBufferPool::SubPool::pushChunk(char* chunk) {
  DEBUG_PRINT(logger_, "pushChunk enter" << KVLOG(KVLOG_PREFIX));
  {
    std::lock_guard<std::mutex> lock(waitForAvailChunkMutex_);
    ConcordAssert(chunks_.push(chunk));
  }
  DEBUG_PRINT(logger_, "Chunk push:" << KVLOG(KVLOG_PREFIX, (void*)chunk) << stats_);
  waitForAvailChunkCond_.notify_one();
  DEBUG_PRINT(logger_, "pushChunk exit" << KVLOG(KVLOG_PREFIX));
}

std::pair<char*, MultiSizeBufferPool::SubPool::AllocationStatus> MultiSizeBufferPool::SubPool::allocateChunk() {
  char* chunk = nullptr;
  const auto& numAllocatedChunks{stats_.current_.numAllocatedChunks_};

  DEBUG_PRINT(logger_, "allocateChunk enter" << KVLOG(KVLOG_PREFIX));
  if (numAllocatedChunks >= config_.numMaxBuffers) {
    LOG_WARN(logger_,
             "The sub-pool size has reached the maximum buffer allocation threshold!"
                 << KVLOG(KVLOG_PREFIX, numAllocatedChunks) << config_ << stats_);
    DEBUG_PRINT(logger_, "allocateChunk exit" << KVLOG(KVLOG_PREFIX));
    return std::make_pair(nullptr, AllocationStatus::EXCEED_MAX_BUFFERS);
  }
  if ((poolConfig_.maxAllocatedBytes != 0) &&
      ((poolStats_.current_.numAllocatedBytes_ + chunkSize_) > poolConfig_.maxAllocatedBytes)) {
    LOG_WARN(logger_, "allocateChunk exit" << KVLOG(KVLOG_PREFIX));
    return std::make_pair(nullptr, AllocationStatus::EXCEED_POOL_MAX_BYTES);
  }

  ConcordAssertGT(chunkSize_, config_.bufferSize);
  chunk = new char[chunkSize_];
  new (chunk) MultiSizeBufferPool::ChunkHeader(config_.bufferSize);              // placement new to header
  new (getChunkTrailer(chunk, chunkSize_)) MultiSizeBufferPool::ChunkTrailer();  // placement new to trailer

  stats_.onChunkAllocated(chunkSize_);
  poolStats_.onChunkAllocated(chunkSize_);
  DEBUG_PRINT(logger_, "allocateChunk exit:" << KVLOG(KVLOG_PREFIX, (void*)chunk) << stats_);
  return std::make_pair(chunk, AllocationStatus::SUCCESS);
}

void MultiSizeBufferPool::SubPool::statusReport() {
  if (!poolConfig_.enableStatusReportChangesOnly || stats_.reportFlag_) {
    LOG_INFO(logger_, KVLOG(KVLOG_PREFIX) << " statusReport: " << stats_);
    stats_.reportFlag_ = false;
  }
}

/* ==========================================
===
===     MultiSizeBufferPool::Statistics
===
========================================== */

void MultiSizeBufferPool::Statistics::onChunkBecomeUsed(uint64_t chunkSize, bool chunkAllocatedFromHeap) {
  // Comment: no overflow happens here due to subtraction, removed assertions after billions of runs
  if (enableStatusReportChangesOnly_) {
    reportFlag_ = true;
  }
  ++current_.numUsedChunks_;
  ++overall_.numUsedChunks_;
  if (!chunkAllocatedFromHeap) {
    --current_.numUnusedChunks_;
    current_.numUnusedBytes_ -= chunkSize;
  }
  current_.numUsedBytes_ += chunkSize;
  overall_.numUsedBytes_ += chunkSize;
}

void MultiSizeBufferPool::Statistics::onChunkBecomeUnused(uint64_t chunkSize, operationType opType) {
  // Comment: no overflow happens here due to subtraction, removed assertions after billions of runs
  if (enableStatusReportChangesOnly_) {
    reportFlag_ = true;
  }
  if ((opType == operationType::PUSH) || (opType == operationType::DELETE)) {
    --current_.numUsedChunks_;
    current_.numUsedBytes_ -= chunkSize;
  }
  if ((opType == operationType::PUSH) || (opType == operationType::ALLOC)) {
    ++current_.numUnusedChunks_;
    current_.numUnusedBytes_ += chunkSize;
  }
}

void MultiSizeBufferPool::Statistics::onChunkAllocated(uint64_t chunkSize) {
  // Comment: no overflow happens here due to subtraction, removed assertions after billions of runs
  if (enableStatusReportChangesOnly_) {
    reportFlag_ = true;
  }
  ++current_.numAllocatedChunks_;
  current_.numAllocatedBytes_ += chunkSize;
  if (current_.numAllocatedBytes_ > overall_.maxNumAllocatedBytes_) {
    overall_.maxNumAllocatedBytes_.store(current_.numAllocatedBytes_);
  }
  current_.numNonAllocatedBytes_ -= chunkSize;
  --current_.numNonAllocatedChunks_;
  if (current_.numAllocatedChunks_ > overall_.maxNumAllocatedChunks_) {
    overall_.maxNumAllocatedChunks_.store(current_.numAllocatedChunks_);
  }

  overall_.numAllocatedBytes_ += chunkSize;
  ++overall_.numAllocatedChunks_;
}

void MultiSizeBufferPool::Statistics::onChunkDeleted(uint64_t chunkSize) {
  // Comment: no overflow happens here due to subtraction, removed assertions after billions of runs
  if (enableStatusReportChangesOnly_) {
    reportFlag_ = true;
  }
  --current_.numAllocatedChunks_;
  current_.numAllocatedBytes_ -= chunkSize;
  current_.numNonAllocatedBytes_ += chunkSize;
  ++current_.numNonAllocatedChunks_;

  overall_.numDeletedBytes_ += chunkSize;
  ++overall_.numDeletedChunks_;
}

void MultiSizeBufferPool::Statistics::onInit(uint32_t numMaxBuffers, uint32_t chunkSize) {
  ConcordAssertNE(chunkSize, 0);
  current_.numNonAllocatedBytes_ += numMaxBuffers * chunkSize;
  current_.numNonAllocatedChunks_ += numMaxBuffers;
}

void MultiSizeBufferPool::Statistics::setLimits(uint64_t maxAllocatedBytes) {
  if (maxAllocatedBytes == 0) return;
  if (maxAllocatedBytes < current_.numNonAllocatedBytes_) {
    current_.numNonAllocatedBytes_ = maxAllocatedBytes;
  }
}

}  // namespace concordUtil
