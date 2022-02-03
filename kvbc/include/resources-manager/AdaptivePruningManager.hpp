#pragma once

#include "IRequestHandler.hpp"
#include "IResourceManager.hpp"
#include "InternalBFTClient.hpp"
<<<<<<< HEAD
#include "db_interfaces.h"
#include "concord.cmf.hpp"
=======
#include "Metrics.hpp"

>>>>>>> Improvements
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>

namespace concord::performance {
enum PruningMode : uint8_t { LEGACY = 0x0, ADAPTIVE = 0x1 };

class AdaptivePruningManager {
 public:
  AdaptivePruningManager(const std::shared_ptr<concord::performance::IResourceManager> &resourceManager,
                         const std::chrono::duration<double, std::milli> &interval,
                         const std::shared_ptr<concordMetrics::Aggregator> &aggregator,
                         concord::kvbc::IReader &ro_storage);

  virtual ~AdaptivePruningManager();

  void switchMode(PruningMode newMode) {
    mode = newMode;
    conditionVar.notify_one();
  }
  PruningMode getCurrentMode() { return mode.load(); }

  void setPrimay(bool isPrimary) {
    LOG_INFO(ADPTV_PRUNING, "am I the new primary? " << isPrimary);
    if (isPrimary) {
      amIPrimary = true;
      conditionVar.notify_one();
    } else {
      amIPrimary = false;
    }
  }

  void setResourceManager(const std::shared_ptr<concord::performance::IResourceManager> &resourceManagerNew,
                          bool start_ = true) {
    {
      std::unique_lock<std::mutex> lk(conditionLock);
      resourceManager = resourceManagerNew;
    }
    if (start_) start();
  }
  void initBFTClient(const std::shared_ptr<bftEngine::impl::IInternalBFTClient> &cl);
  void start();
  void stop();
  void notifyReplicas(const long double &rate, const uint64_t batchSize);
  const concord::messages::PruneSwitchModeRequest &getLatestConfiguration();
  void onTickChangeRequest(concord::messages::PruneTicksChangeRequest req) {
    current_pruning_pace_ = req.ticks_per_second;
    current_batch_size_ = req.batch_blocks_num;
  }
  uint32_t getCurrentPace() { return current_pruning_pace_; }
  uint64_t getCurrentBatch() { return current_batch_size_; }

 private:
  void threadFunction();

 private:
  const std::uint16_t repId;
  std::shared_ptr<concord::performance::IResourceManager> resourceManager;
  std::atomic<PruningMode> mode;
  std::chrono::duration<double, std::milli> interval;
  std::shared_ptr<bftEngine::impl::IInternalBFTClient> bftClient;
  std::thread workThread;
  std::atomic<bool> isRunning, amIPrimary;
  std::condition_variable conditionVar;
  std::mutex conditionLock;
  concord::kvbc::IReader &ro_storage_;
  concord::messages::PruneSwitchModeRequest latestConfiguration_{0, PruningMode::LEGACY, {}};
  uint32_t current_pruning_pace_;
  uint64_t current_batch_size_;
  concordMetrics::Component metricComponent;
  concordMetrics::Component::Handle<concordMetrics::AtomicGauge> ticksPerSecondMetric, batchSizeMetric;

};
}  // namespace concord::performance