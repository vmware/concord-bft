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

#include "KeyManager.h"
#include "thread"
#include "ReplicaImp.hpp"
#include "ReplicaConfig.hpp"
#include <memory>
#include "messages/ClientRequestMsg.hpp"
#include "bftengine/CryptoManager.hpp"

////////////////////////////// KEY MANAGER//////////////////////////////
namespace bftEngine::impl {
KeyManager::KeyManager(InitData* id)
    : repID_(id->id),
      clusterSize_(id->clusterSize),
      client_(id->cl),
      keyStore_{id->clusterSize, *id->reservedPages, id->sizeOfReservedPage},
      multiSigKeyHdlr_(id->kg),
      keysView_(id->sec, id->clusterSize),
      timers_(*(id->timers)) {
  registryToExchange_.push_back(id->ke);
  if (keyStore_.exchangedReplicas.size() == 0) {
    if (keysView_.load()) {
      LOG_INFO(KEY_EX_LOG, "Loaded key view file but reserved pages are empty, did someone cleaned the DB ?");
    }
    return;
  }
  LOG_INFO(KEY_EX_LOG, "Loading replcia's key view");
  ConcordAssert(keysView_.load());
  // If all keys were exchange on start
  if (keyStore_.exchangedReplicas.size() == clusterSize_) {
    LOG_INFO(KEY_EX_LOG, "building crypto system ");
    notifyRegistry();
    keysExchanged = true;
    LOG_INFO(KEY_EX_LOG, "All replicas keys loaded from reserved pages, can start accepting msgs");
  }
}

void KeyManager::initMetrics(std::shared_ptr<concordMetrics::Aggregator> a, std::chrono::seconds interval) {
  metrics_.reset(new Metrics(a, interval));
  metrics_->component.Register();
  metricsTimer_ = timers_.add(std::chrono::milliseconds(100), Timers::Timer::RECURRING, [this](Timers::Handle h) {
    metrics_->component.UpdateAggregator();
    auto currTime =
        std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now().time_since_epoch());
    if (currTime - metrics_->lastMetricsDumpTime >= metrics_->metricsDumpIntervalInSec) {
      metrics_->lastMetricsDumpTime = currTime;
      LOG_INFO(KEY_EX_LOG, "-- KeyManager metrics dump--" + metrics_->component.ToJson());
    }
  });
  for (uint32_t i = 0; i < (uint32_t)keyStore_.exchangedReplicas.size(); ++i) {
    metrics_->keyExchangedOnStartCounter.Get().Inc();
  }
}

std::string KeyManager::generateCid() {
  std::string cid{"KEY-EXCHANGE-"};
  auto now = getMonotonicTime().time_since_epoch();
  auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now);
  auto sn = now_ms.count();
  cid += std::to_string(repID_) + "-" + std::to_string(sn);
  return cid;
}

// Key exchange msg for replica has been recieved:
// update the replica public key store.
std::string KeyManager::onKeyExchange(KeyExchangeMsg& kemsg, const uint64_t& sn) {
  if (kemsg.op == KeyExchangeMsg::HAS_KEYS) {
    LOG_INFO(KEY_EX_LOG, "Has key query arrived, returning " << std::boolalpha << keysExchanged << std::noboolalpha);
    if (!keysExchanged) return std::string(KeyExchangeMsg::hasKeysFalseReply);
    return std::string(KeyExchangeMsg::hasKeysTrueReply);
  }

  LOG_INFO(KEY_EX_LOG, "Recieved onKeyExchange " << kemsg.toString() << " seq num " << sn);
  if (!keysExchanged) {
    onInitialKeyExchange(kemsg, sn);
    return "ok";
  }

  if (!keyStore_.push(kemsg, sn)) return "ok";

  metrics_->keyExchangedCounter.Get().Inc();

  if (kemsg.repID == repID_) {
    keysView_.rotate(keysView_.keys().outstandingPrivateKey, keysView_.keys().generatedPrivateKey);
  }

  return "ok";
}

// Once the set contains all replicas the crypto system is being updated and the gate is open.
void KeyManager::onInitialKeyExchange(KeyExchangeMsg& kemsg, const uint64_t& sn) {
  // For some reason we recieved a key for a replica that already exchanged it's key.
  if (keyStore_.exchangedReplicas.find(kemsg.repID) != keyStore_.exchangedReplicas.end()) {
    LOG_DEBUG(KEY_EX_LOG, "Replica [" << kemsg.repID << "] already exchanged initial key");
    ConcordAssert(false);
  }

  if (kemsg.repID == repID_) {
    keysView_.rotate(keysView_.keys().outstandingPrivateKey, keysView_.keys().generatedPrivateKey);
  }

  LOG_INFO(KEY_EX_LOG, "Initial key exchanged for replica [" << kemsg.repID << "]");
  keyStore_.exchangedReplicas.insert(kemsg.repID);
  metrics_->keyExchangedOnStartCounter.Get().Inc();
  LOG_INFO(KEY_EX_LOG, "Exchanged [" << keyStore_.exchangedReplicas.size() << "] out of [" << clusterSize_ << "]");

  if (keyStore_.exchangedReplicas.size() < clusterSize_) {
    keyStore_.push(kemsg, sn);
    return;
  }
  // All keys were exchanged
  LOG_INFO(KEY_EX_LOG, "building crypto system ");
  keysView_.rotate(keysView_.keys().privateKey, keysView_.keys().outstandingPrivateKey);
  keyStore_.push(kemsg, sn);
  notifyRegistry();
  keysExchanged = true;
  LOG_INFO(KEY_EX_LOG, "All replicas exchanged keys, can start accepting msgs");
}

void KeyManager::notifyRegistry() {
  for (uint32_t i = 0; i < clusterSize_; i++) {
    keysView_.keys().publicKeys[i] = keyStore_.getReplicaPublicKey(i).key;
    if (i == repID_) {
      std::for_each(registryToExchange_.begin(), registryToExchange_.end(), [this](IKeyExchanger* e) {
        e->onPrivateKeyExchange(keysView_.keys().privateKey, keyStore_.getReplicaPublicKey(repID_).key);
      });
      continue;
    }
    std::for_each(registryToExchange_.begin(), registryToExchange_.end(), [&, this](IKeyExchanger* e) {
      e->onPublicKeyExchange(keyStore_.getReplicaPublicKey(i).key, i);
    });
  }
  keysView_.save();
}

void KeyManager::loadCryptoFromKeyView(std::shared_ptr<ISecureStore> sec,
                                       const uint16_t repID,
                                       const uint16_t numReplicas) {
  KeysView kv(sec, numReplicas);
  if (!kv.load()) {
    LOG_ERROR(KEY_EX_LOG, "Couldn't load keys");
    return;
  }
  for (int i = 0; i < numReplicas; ++i) {
    if (i == repID) {
      CryptoManager::instance().onPrivateKeyExchange(kv.keys().privateKey, kv.keys().publicKeys[i]);
      continue;
    }
    CryptoManager::instance().onPublicKeyExchange(kv.keys().publicKeys[i], i);
  }
}

// The checkpoint is the point where keys are rotated.
void KeyManager::onCheckpoint(const int& num) {
  auto rotatedReplicas = keyStore_.rotate(num);
  if (rotatedReplicas.empty()) return;

  for (auto id : rotatedReplicas) {
    metrics_->publicKeyRotated.Get().Inc();
    if (id != repID_) continue;
    LOG_INFO(KEY_EX_LOG, "Rotating private key");
    keysView_.rotate(keysView_.keys().privateKey, keysView_.keys().outstandingPrivateKey);
  }
  LOG_INFO(KEY_EX_LOG, "Check point  " << num << " trigerred rotation ");
  LOG_INFO(KEY_EX_LOG, "building crypto system ");
  notifyRegistry();
}

void KeyManager::registerForNotification(IKeyExchanger* ke) { registryToExchange_.push_back(ke); }

KeyExchangeMsg KeyManager::getReplicaPublicKey(const uint16_t& repID) const {
  return keyStore_.getReplicaPublicKey(repID);
}

// Called at the end of a state transfer.
void KeyManager::loadKeysFromReservedPages() {
  // If we reached state transfer, all keys should have beed exchanged in the source replica
  // TODO might be changed when full rotation is imp
  ConcordAssert(keyStore_.loadAllReplicasKeyStoresFromReservedPages());
  // TODO E.L  KeyRotation implementation will need to rotate privete keys here, checkpoint number will be essential
  if (keysView_.keys().privateKey.empty()) {
    if (keysView_.keys().outstandingPrivateKey.size() > 0) {
      keysView_.rotate(keysView_.keys().privateKey, keysView_.keys().outstandingPrivateKey);
    } else {
      keysView_.rotate(keysView_.keys().privateKey, keysView_.keys().generatedPrivateKey);
    }
    ConcordAssert(keysView_.keys().privateKey.empty() == false);
  }
  keysExchanged = true;
  LOG_INFO(KEY_EX_LOG, "building crypto system after state transfer");
  notifyRegistry();
}

void KeyManager::sendKeyExchange() {
  KeyExchangeMsg msg;
  auto [prv, pub] = multiSigKeyHdlr_->generateMultisigKeyPair();
  keysView_.keys().generatedPrivateKey = prv;
  msg.key = pub;
  msg.repID = repID_;
  std::stringstream ss;
  concord::serialize::Serializable::serialize(ss, msg);
  auto strMsg = ss.str();
  client_->sendRquest(bftEngine::KEY_EXCHANGE_FLAG, strMsg.size(), strMsg.c_str(), generateCid());
  keysView_.save();
  LOG_INFO(KEY_EX_LOG, "Sending key exchange msg, sleeping");
}

// First Key exchange is on start, in order not to trigger view change, we'll wait for all replicas to be connected.
// In order not to block it's done as async operation.
// If transport is UDP, we can't know the connection status, and we are in Apollo context therefore giving 2sec grace.
void KeyManager::sendInitialKey() {
  if (keysView_.hasGeneratedKeys()) {
    LOG_INFO(KEY_EX_LOG, "Replica already generated keys ");
    return;
  }
  auto l = [this]() {
    waitForFullCommunication();
    if (keyStore_.exchangedReplicas.find(repID_) != keyStore_.exchangedReplicas.end()) return;
    LOG_DEBUG(KEY_EX_LOG, "Didn't find replica's first generated keys, sending");
    if (client_->isUdp()) {
      LOG_INFO(KEY_EX_LOG, "UDP communication");
      std::this_thread::sleep_for(std::chrono::seconds(2));
    }
    sendKeyExchange();
  };
  futureRet = std::async(std::launch::async, l);
}

void KeyManager::waitForFullCommunication() {
  auto avlble = client_->numOfConnectedReplicas(clusterSize_);
  LOG_INFO(KEY_EX_LOG, "Consensus engine: " << avlble << " replicas are connected");
  // Num of connections should be: (clusterSize - 1)
  while (avlble < clusterSize_ - 1) {
    LOG_INFO(KEY_EX_LOG, "Consensus engine not available, " << avlble << " replicas are connected");
    std::this_thread::sleep_for(std::chrono::seconds(1));
    avlble = client_->numOfConnectedReplicas(clusterSize_);
  }
  LOG_INFO(KEY_EX_LOG, "Consensus engine available, " << avlble << " replicas are connected");
}

/////////////KEYS VIEW////////////////////////////

const std::string KeyManager::KeysViewData::getVersion() const { return "1"; }

void KeyManager::KeysViewData::serializeDataMembers(std::ostream& outStream) const {
  serialize(outStream, privateKey);
  serialize(outStream, outstandingPrivateKey);
  serialize(outStream, generatedPrivateKey);
  serialize(outStream, publicKeys);
}

void KeyManager::KeysViewData::deserializeDataMembers(std::istream& inStream) {
  deserialize(inStream, privateKey);
  deserialize(inStream, outstandingPrivateKey);
  deserialize(inStream, generatedPrivateKey);
  deserialize(inStream, publicKeys);
}

KeyManager::KeysView::KeysView(std::shared_ptr<ISecureStore> sec, uint32_t clusterSize) : secStore(sec) {
  data.publicKeys.resize(clusterSize);
}

void KeyManager::KeysView::rotate(std::string& dst, std::string& src) {
  ConcordAssert(src.empty() == false);
  dst = src;
  src.clear();
  save();
}
void KeyManager::KeysView::save() {
  if (!secStore) {
    LOG_DEBUG(KEY_EX_LOG, "Saver is not set");
    return;
  }
  std::stringstream ss;
  concord::serialize::Serializable::serialize(ss, data);
  secStore->save(ss.str());
}

bool KeyManager::KeysView::load() {
  if (!secStore) {
    LOG_DEBUG(KEY_EX_LOG, "Loader is not set");
    return false;
  }
  auto str = secStore->load();
  if (str.empty()) {
    LOG_ERROR(KEY_EX_LOG, "Got empty string from loader");
    return false;
  }
  std::stringstream ss;
  ss.write(str.c_str(), std::streamsize(str.size()));
  concord::serialize::Serializable::deserialize(ss, data);
  LOG_DEBUG(KEY_EX_LOG, "Loaded successfully " << data.generatedPrivateKey << " " << data.outstandingPrivateKey);
  return true;
}

void KeyManager::FileSecureStore::save(const std::string& str) {
  if (str.empty()) {
    LOG_INFO(KEY_EX_LOG, "Got empty string to save to key file");
    return;
  }
  std::ofstream myfile;
  myfile.open(fileName.c_str());
  if (!myfile.good()) {
    LOG_FATAL(KEY_EX_LOG, "Couldn't save key file to " << fileName);
    ConcordAssert(false);
  }
  myfile << str;
  myfile.close();
}

std::string KeyManager::FileSecureStore::load() {
  std::ifstream inFile;
  inFile.open(fileName.c_str());
  if (!inFile.good()) {
    LOG_WARN(KEY_EX_LOG, "key file wasn't loaded " << fileName);
    return "";
  }
  std::stringstream strStream;
  strStream << inFile.rdbuf();
  inFile.close();
  return strStream.str();
}

}  // namespace bftEngine::impl