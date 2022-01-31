#include "AdaptivePruningManager.hpp"
#include "Bitmap.hpp"
#include "ReplicaConfig.hpp"
#include "concord.cmf.hpp"
#include "SigManager.hpp"

using namespace concord::performance;

AdaptivePruningManager::AdaptivePruningManager(
    const std::shared_ptr<concord::performance::IResourceManager> &resourceManager,
    const std::chrono::duration<double, std::milli> &interval,
    const std::shared_ptr<bftEngine::impl::IInternalBFTClient> &bftClient)
    : repId(bftEngine::ReplicaConfig::instance().getreplicaId()),
      resourceManager(resourceManager),
      mode(LEGACY),
      interval(interval),
      bftClient(bftClient) {
  start();
}

AdaptivePruningManager::~AdaptivePruningManager() { stop(); }

void AdaptivePruningManager::notifyReplicas(const long double &rate, const uint64_t batchSize) {
  concord::messages::ReconfigurationRequest rreq;

  concord::messages::PruneTicksChangeRequest pruneRequest;
  pruneRequest.sender_id = bftClient->getClientId();
  pruneRequest.tick_period_seconds =
      rate / bftClient->numOfConnectedReplicas(bftEngine::ReplicaConfig::instance().numReplicas);
  pruneRequest.batch_blocks_num = batchSize;

  rreq.command = pruneRequest;
  rreq.sender = bftClient->getClientId();

  std::vector<uint8_t> serialized_req;

  concord::messages::serialize(serialized_req, rreq);

  uint64_t flags{};
  const std::string cid = "adaptive-pruning-manager-cid";
  std::string serializedString(serialized_req.begin(), serialized_req.end());

  std::string sig(SigManager::instance()->getMySigLength(), '\0');
  uint16_t sig_length{0};
  SigManager::instance()->sign(
      reinterpret_cast<char *>(serialized_req.data()), serialized_req.size(), sig.data(), sig_length);
  rreq.signature = std::vector<uint8_t>(sig.begin(), sig.end());

  bftClient->sendRequest(flags, serializedString.length(), serializedString.c_str(), cid);
}

void AdaptivePruningManager::threadFunction() {
  while (isRunning.load()) {
    {
      std::unique_lock<std::mutex> lk(conditionLock);
      conditionVar.wait(lk, [this]() { return mode.load() == ADAPTIVE && amIPrimary.load(); });
    }
    if (isRunning.load()) {
      std::unique_lock<std::mutex> lk(conditionLock);
      auto info = resourceManager->getPruneInfo();
      notifyReplicas(info.blocksPerSecond, info.batchSize);
      conditionVar.wait_for(lk, interval);
    }
  }
}

void AdaptivePruningManager::start() {
  std::unique_lock<std::mutex> lk(conditionLock);
  if (!isRunning.load() && resourceManager.get() != nullptr) {
    isRunning = true;
    workThread = std::thread(&AdaptivePruningManager::threadFunction, this);
  }
}

void AdaptivePruningManager::stop() {
  if (isRunning.load()) {
    isRunning = false;
    conditionVar.notify_one();
    workThread.join();
  }
}