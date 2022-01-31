#pragma once

#include "IRequestHandler.hpp"
#include "IResourceManager.hpp"
#include "InternalBFTClient.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>

enum PruningMode { LEGACY, ADAPTIVE };

namespace concord::performance {
class AdaptivePruningManager {
 public:
  AdaptivePruningManager(const std::shared_ptr<concord::performance::IResourceManager> &resourceManager,
                         const std::chrono::duration<double, std::milli> &interval,
                         const std::shared_ptr<bftEngine::impl::IInternalBFTClient> &bftClient);
  virtual ~AdaptivePruningManager();

  void switchMode(PruningMode newMode) {
    mode = newMode;
    conditionVar.notify_one();
  }
  PruningMode getCurrentMode() { return mode.load(); }

  void youArePrimary() {
    amIPrimary = true;
    conditionVar.notify_one();
  }

  void youAreNotPrimary() { amIPrimary = false; }

  void setResourceManager(const std::shared_ptr<concord::performance::IResourceManager> &resourceManagerNew) {
    {
      std::unique_lock<std::mutex> lk(conditionLock);
      resourceManager = resourceManagerNew;
    }
    start();
  }

  void start();
  void stop();

 protected:
  void notifyReplicas(const long double &rate, const uint64_t batchSize);

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
};
}  // namespace concord::performance