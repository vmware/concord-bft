#include "AdaptivePruningManager.hpp"
#include "Bitmap.hpp"
#include "ReplicaConfig.hpp"
#include "concord.cmf.hpp"
#include "SigManager.hpp"
#include "kvbc_key_types.hpp"
#include "categorization/db_categories.h"
#include "concord.cmf.hpp"
#include "bftclient/base_types.h"
using namespace concord::performance;

AdaptivePruningManager::AdaptivePruningManager(
    const std::shared_ptr<concord::performance::IResourceManager> &resourceManager,
    const std::chrono::duration<double, std::milli> &interval,
    concord::kvbc::IReader &ro_storage)
    : repId(bftEngine::ReplicaConfig::instance().getreplicaId()),
      resourceManager(resourceManager),
      mode(LEGACY),
      interval(interval),
      ro_storage_(ro_storage) {
  (void)ro_storage_;
  resourceManager->setPeriod(bftEngine::ReplicaConfig::instance().adaptivePruningIntervalPeriod);
}

AdaptivePruningManager::~AdaptivePruningManager() { stop(); }

void AdaptivePruningManager::notifyReplicas(const long double &rate, const uint64_t batchSize) {
  if (!bftClient) {
    LOG_ERROR(ADPTV_PRUNING, "BFT client is not set");
    return;
  }
  concord::messages::ReconfigurationRequest rreq;

  concord::messages::PruneTicksChangeRequest pruneRequest;

  pruneRequest.sender_id = bftClient->getClientId();
  pruneRequest.tick_period_seconds = 1;

  pruneRequest.batch_blocks_num = rate / bftEngine::ReplicaConfig::instance().numReplicas;

  rreq.command = pruneRequest;
  rreq.sender = bftClient->getClientId();

  std::vector<uint8_t> serialized_req;

  concord::messages::serialize(serialized_req, rreq);

  uint64_t flags = bft::client::Flags::RECONFIG_FLAG;
  const std::string cid = "adaptive-pruning-manager-cid";
  std::string serializedString(serialized_req.begin(), serialized_req.end());

  std::string sig(SigManager::instance()->getMySigLength(), '\0');
  uint16_t sig_length{0};
  SigManager::instance()->sign(
      reinterpret_cast<char *>(serialized_req.data()), serialized_req.size(), sig.data(), sig_length);
  rreq.signature = std::vector<uint8_t>(sig.begin(), sig.end());

  bftClient->sendRequest(flags, serializedString.length(), serializedString.c_str(), cid);
}

const concord::messages::PruneSwitchModeRequest &AdaptivePruningManager::getLatestConfiguration() {
  const auto val =
      ro_storage_.getLatest(concord::kvbc::categorization::kConcordReconfigurationCategoryId,
                            std::string{kvbc::keyTypes::reconfiguration_pruning_key,
                                        static_cast<char>(kvbc::keyTypes::PRUNING_COMMAND_TYPES::SWITCH_MODE_REQUEST)});
  if (val.has_value()) {
    const auto &strval = std::get<kvbc::categorization::VersionedValue>(*val).data;
    std::vector<uint8_t> data_buf(strval.begin(), strval.end());
    concord::messages::deserialize(data_buf, latestConfiguration_);
  }
  return latestConfiguration_;
  // TODO: Add log line that describes the given stored configuration
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
  if (!isRunning.load() && resourceManager.get() != nullptr && bftClient.get() != nullptr) {
    isRunning = true;
    workThread = std::thread(&AdaptivePruningManager::threadFunction, this);
  } else {
    LOG_INFO(ADPTV_PRUNING, "Failed to start thread");
  }
}

void AdaptivePruningManager::stop() {
  if (isRunning.load()) {
    isRunning = false;
    conditionVar.notify_one();
    workThread.join();
  }
}

void AdaptivePruningManager::initBFTClient(const std::shared_ptr<bftEngine::impl::IInternalBFTClient> &cl) {
  bftClient = cl;
  start();
  LOG_INFO(ADPTV_PRUNING, "Initializing client and starting the thread");
}