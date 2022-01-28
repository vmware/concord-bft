#include "AdaptivePruningManager.hpp"
#include "Bitmap.hpp"
#include "ReplicaConfig.hpp"
#include "concord.cmf.hpp"

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
  // numOfConnectedReplicas requires cluster size. How can I get it?
  // pruneRequest.tick_period_seconds = rate / bftClient->numOfConnectedReplicas();
  pruneRequest.batch_blocks_num = batchSize;

  rreq.command = pruneRequest;
  rreq.sender = bftClient->getClientId();

  std::vector<uint8_t> serialized_req;
  rreq.signature = {};
  concord::messages::serialize(serialized_req, rreq);

  // auto sig = signer_->sign(std::string(serialized_req.begin(), serialized_req.end()));
  // request.signature = std::vector<uint8_t>(sig.begin(), sig.end());
  // rreq.signature =
  uint64_t flags{};
  const std::string cid = "adaptive-pruning-manager-cid";
  std::string serializedString(serialized_req.begin(), serialized_req.end());

  bftClient->sendRequest(flags, serializedString.length(), serializedString.c_str(), cid);
}

void AdaptivePruningManager::threadFunction() {
  while (isRunning.load()) {
    {
      std::unique_lock<std::mutex> lk(conditionLock);
      conditionVar.wait(lk, [this]() { return mode.load() == ADAPTIVE && amIPrimary.load(); });
    }
    if (isRunning.load()) {
      auto info = resourceManager->getPruneInfo();
      notifyReplicas(info.blocksPerSecond, info.batchSize);
      std::unique_lock<std::mutex> lk(conditionLock);
      conditionVar.wait_for(lk, interval);
    }
  }
}

void AdaptivePruningManager::start() {
  if (!isRunning.load()) {
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