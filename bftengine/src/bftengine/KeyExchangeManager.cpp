// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "bftengine/KeyExchangeManager.hpp"
#include "Replica.hpp"
#include "InternalBFTClient.hpp"
#include "kvstream.h"
#include "json_output.hpp"
#include <thread>

namespace bftEngine::impl {

KeyExchangeManager::KeyExchangeManager(InitData* id)
    : repID_{ReplicaConfig::instance().getreplicaId()},
      clusterSize_{ReplicaConfig::instance().getnumReplicas()},
      publicKeys_{clusterSize_},
      private_keys_(id->secretsMgr),
      client_(id->cl),
      multiSigKeyHdlr_(id->kg),
      timers_(*(id->timers)) {
  registerForNotification(id->ke);
  if (publicKeys_.numOfExchangedReplicas() == clusterSize_)  // don't notify on first start
    notifyRegistry();
}

void KeyExchangeManager::initMetrics(std::shared_ptr<concordMetrics::Aggregator> a, std::chrono::seconds interval) {
  metrics_.reset(new Metrics(a, interval));
  metrics_->component.Register();
  metricsTimer_ = timers_.add(
      std::chrono::milliseconds(100), concordUtil::Timers::Timer::RECURRING, [this](concordUtil::Timers::Handle h) {
        metrics_->component.UpdateAggregator();
        auto currTime =
            std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now().time_since_epoch());
        if (currTime - metrics_->lastMetricsDumpTime >= metrics_->metricsDumpIntervalInSec) {
          metrics_->lastMetricsDumpTime = currTime;
          LOG_INFO(KEY_EX_LOG, "-- KeyManager metrics dump--" + metrics_->component.ToJson());
        }
      });
}

std::string KeyExchangeManager::generateCid() {
  std::string cid{"KEY-EXCHANGE-"};
  auto now = getMonotonicTime().time_since_epoch();
  auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now);
  auto sn = now_ms.count();
  cid += std::to_string(repID_) + "-" + std::to_string(sn);
  return cid;
}

std::string KeyExchangeManager::onKeyExchange(const KeyExchangeMsg& kemsg, const SeqNum& sn, const std::string& cid) {
  SCOPED_MDC_SEQ_NUM(std::to_string(sn));
  LOG_INFO(KEY_EX_LOG, kemsg.toString() << KVLOG(sn, cid, exchanged()));
  // client query
  if (kemsg.op == KeyExchangeMsg::HAS_KEYS) {
    LOG_INFO(KEY_EX_LOG, "Has keys: " << std::boolalpha << exchanged() << std::noboolalpha);
    if (!exchanged()) return std::string(KeyExchangeMsg::hasKeysFalseReply);
    return std::string(KeyExchangeMsg::hasKeysTrueReply);
  }

  publicKeys_.push(kemsg, sn);
  if (kemsg.repID == repID_) {  // initiated by me
    ConcordAssert(private_keys_.key_data().generated.pub == kemsg.pubkey);
    private_keys_.onKeyExchange(cid, sn);
    for (auto e : registryToExchange_) e->onPrivateKeyExchange(private_keys_.key_data().keys[sn], kemsg.pubkey, sn);
    metrics_->self_key_exchange_counter.Get().Inc();
  } else {  // initiated by others
    for (auto e : registryToExchange_) e->onPublicKeyExchange(kemsg.pubkey, kemsg.repID, sn);
    metrics_->public_key_exchange_for_peer_counter.Get().Inc();
  }
  if (ReplicaConfig::instance().getkeyExchangeOnStart() && (publicKeys_.numOfExchangedReplicas() <= clusterSize_))
    LOG_INFO(KEY_EX_LOG, "Exchanged [" << publicKeys_.numOfExchangedReplicas() << "] out of [" << clusterSize_ << "]");
  return "ok";
}

void KeyExchangeManager::notifyRegistry() {
  for (auto ke : registryToExchange_) {
    for (auto [sn, pk] : private_keys_.key_data().keys)
      ke->onPrivateKeyExchange(pk, publicKeys_.getKey(repID_, sn), sn);
  }

  for (uint32_t i = 0; i < clusterSize_; i++) {  // for every replica
    if (!publicKeys_.keyExists(i)) {
      LOG_WARN(KEY_EX_LOG, "public key doesn't exist for replica: " << i);
      continue;
    }
    for (auto ke : registryToExchange_) {  // update other replicas' public keys
      for (auto [sn, pk] : publicKeys_.keys(i).keys) ke->onPublicKeyExchange(pk, i, sn);
    }
  }
}

void KeyExchangeManager::loadPublicKeys() {
  // after State Transfer public keys for all replicas are expected to exist
  auto num_loaded = publicKeys_.loadAllReplicasKeyStoresFromReservedPages();
  if (ReplicaConfig::instance().getkeyExchangeOnStart()) ConcordAssert(num_loaded == clusterSize_);
  LOG_INFO(KEY_EX_LOG, "building crypto system after state transfer");
  notifyRegistry();
}

void KeyExchangeManager::sendKeyExchange(const SeqNum& sn) {
  // first check whether we've already generated keys lately
  if (private_keys_.lastGeneratedSeqnum() &&  // if not init  ial
      (sn - private_keys_.lastGeneratedSeqnum()) / checkpointWindowSize < 2) {
    LOG_INFO(KEY_EX_LOG, "ignore request - already generated keys for seqnum: " << private_keys_.lastGeneratedSeqnum());
    return;
  }
  KeyExchangeMsg msg;
  auto cid = generateCid();
  auto [prv, pub] = multiSigKeyHdlr_->generateMultisigKeyPair();
  private_keys_.key_data().generated.priv = prv;
  private_keys_.key_data().generated.pub = pub;
  private_keys_.key_data().generated.cid = cid;
  private_keys_.key_data().generated.sn = sn;
  private_keys_.save();

  LOG_INFO(KEY_EX_LOG, "Sending key exchange :" << KVLOG(cid, pub));
  msg.pubkey = pub;
  msg.repID = repID_;
  std::stringstream ss;
  concord::serialize::Serializable::serialize(ss, msg);
  auto strMsg = ss.str();
  client_->sendRquest(bftEngine::KEY_EXCHANGE_FLAG, strMsg.size(), strMsg.c_str(), cid);
  metrics_->sent_key_exchange_counter.Get().Inc();
}

void KeyExchangeManager::sendInitialKey() {
  LOG_INFO(KEY_EX_LOG, "");
  if (private_keys_.hasGeneratedKeys()) {
    LOG_INFO(KEY_EX_LOG, "Replica has already generated keys");
    return;
  }
  // First Key exchange is on start, in order not to trigger view change, we'll wait for all replicas to be connected.
  // In order not to block it's done as async operation.
  auto ret = std::async(std::launch::async, [this]() {
    SCOPED_MDC(MDC_REPLICA_ID_KEY, std::to_string(ReplicaConfig::instance().replicaId));
    waitForFullCommunication();
    sendKeyExchange(0);
    metrics_->sent_key_exchange_on_start_status.Get().Set("True");
  });
}

void KeyExchangeManager::waitForFullCommunication() {
  auto avlble = client_->numOfConnectedReplicas(clusterSize_);
  LOG_INFO(KEY_EX_LOG, "Consensus engine: " << avlble << " replicas are connected");
  // Num of connections should be: (clusterSize - 1)
  while (avlble < clusterSize_ - 1) {
    LOG_INFO(KEY_EX_LOG, "Consensus engine not available, " << avlble << " replicas are connected");
    std::this_thread::sleep_for(std::chrono::seconds(1));
    avlble = client_->numOfConnectedReplicas(clusterSize_);
  }
  // If transport is UDP, we can't know the connection status, and we are in Apollo context therefore giving 2sec grace.
  if (client_->isUdp()) {
    LOG_INFO(KEY_EX_LOG, "UDP communication");
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }
  LOG_INFO(KEY_EX_LOG, "Consensus engine available, " << avlble << " replicas are connected");
}

std::string KeyExchangeManager::getStatus() const {
  using concordUtils::toPair;
  std::ostringstream oss;
  std::unordered_map<std::string, std::string> result;
  result.insert(toPair("isInitialKeyExchangeCompleted", exchanged()));
  result.insert(
      toPair("keyExchangedCounter", metrics_->aggregator->GetCounter("KeyManager", "KeyExchangedCounter").Get()));
  result.insert(toPair("KeyExchangedOnStartCounter",
                       metrics_->aggregator->GetCounter("KeyManager", "KeyExchangedOnStartCounter").Get()));
  result.insert(toPair("publicKeyRotated", metrics_->aggregator->GetCounter("KeyManager", "publicKeyRotated").Get()));
  oss << concordUtils::kContainerToJson(result);
  return oss.str();
}

bool KeyExchangeManager::PrivateKeys::load() {
  LOG_INFO(KEY_EX_LOG, "");
  auto secrets = secretsMgr_->decryptFile(secrets_file_);
  if (!secrets.has_value()) {
    LOG_WARN(KEY_EX_LOG, "Got empty string from loader. This is expected on first startup with an empty database");
    return false;
  }
  std::stringstream ss;
  std::string str = secrets.value();
  ss.write(str.c_str(), std::streamsize(str.size()));
  concord::serialize::Serializable::deserialize(ss, data_);
  if (data_.generated.sn)
    LOG_INFO(
        KEY_EX_LOG,
        "loaded generated private key for: " << KVLOG(data_.generated.sn, data_.generated.cid, data_.generated.pub));
  for (const auto& it : data_.keys) LOG_INFO(KEY_EX_LOG, "loaded private key for sn: " << it.first);
  return true;
}

}  // namespace bftEngine::impl
