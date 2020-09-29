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
      pathDetector_(id->pathDetect),
      multiSigKeyHdlr_(id->kg),
      keysView_(id->sl, id->clusterSize),
      timers_(*(id->timers)) {
  registryToExchange_.push_back(id->kg);
  // If all keys were exchange on start
  if (keyStore_.exchangedReplicas.size() == clusterSize_) {
    // it's possible that all keys were exchanged but this replica crashed before the rotation.
    // So it has an outstandingPrivateKey.
    if (keysView_.privateKey.empty()) {
      keysView_.rotate(keysView_.privateKey, keysView_.outstandingPrivateKey);
      keysView_.save();
      ConcordAssert(keysView_.privateKey.empty() == false);
    }
    LOG_DEBUG(KEY_EX_LOG, "building crypto system ");
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
  LOG_INFO(KEY_EX_LOG, "Recieved onKeyExchange " << kemsg.toString() << " seq num " << sn);
  if (!keysExchanged) {
    onInitialKeyExchange(kemsg, sn);
    return "ok";
  }

  if (!keyStore_.push(kemsg, sn)) return "ok";

  metrics_->keyExchangedCounter.Get().Inc();

  if (kemsg.repID == repID_) {
    keysView_.rotate(keysView_.outstandingPrivateKey, keysView_.publishPrivateKey);
    keysView_.save();
  }

  return "ok";
}

// Responsible to open the gate when all keys where exchanged on start.
// The consensus for the initial key exchange must be performed on the fast path i.e. to ensure that all replicas have
// recieved the key. once the set contains all replicas the crypto system is being updated and the gate is open.
void KeyManager::onInitialKeyExchange(KeyExchangeMsg& kemsg, const uint64_t& sn) {
  // For some reason we recieved a key for a replica that already exchanged it's key.
  if (keyStore_.exchangedReplicas.find(kemsg.repID) != keyStore_.exchangedReplicas.end()) {
    if (!pathDetector_->isSlowPath(sn)) {
      // It shouldn't happen, that two keys from the same replica arrive in the fast path.
      // It means that a race on a key between the replicas.
      // The missing data mechanism can cause this, but it was handled to use slow path during key-exchange.
      LOG_ERROR(KEY_EX_LOG, "Conflicting Keys for Replica [" << kemsg.repID << "] already exchanged initial key");
      ConcordAssert(false);
    }
    LOG_DEBUG(KEY_EX_LOG, "Replica [" << kemsg.repID << "] already exchanged initial key");
    return;
  }
  // If replicπcl id is not in set, check that it arrived in fast path in order to ensure n out of n.
  // If arrived in slow path do not insert to data structure and if repID == this.repID re-send keyexchange.
  // If arrived in fast path set private key to oustanding.
  if (pathDetector_->isSlowPath(sn)) {
    LOG_INFO(KEY_EX_LOG,
             "Initial key exchanged for replica ["
                 << kemsg.repID << "] is dropped, Consensus reached without n out of n participation");
    if (kemsg.repID == repID_) {
      waitForFullCommunication();
      // In order not to bomb the system, send within 50-150 ms
      srand(time(NULL));
      auto millSleep = rand() % 100 + 50;
      LOG_INFO(KEY_EX_LOG, "Resending initial key exchange within " << millSleep << "ms");
      std::this_thread::sleep_for(std::chrono::milliseconds(millSleep));
      sendKeyExchange();
    }
    metrics_->DroppedMsgsCounter.Get().Inc();
    return;
  }

  if (kemsg.repID == repID_) {
    keysView_.rotate(keysView_.outstandingPrivateKey, keysView_.publishPrivateKey);
    keysView_.save();
  }

  keyStore_.push(kemsg, sn);
  LOG_INFO(KEY_EX_LOG, "Initial key exchanged for replica [" << kemsg.repID << "]");
  keyStore_.exchangedReplicas.insert(kemsg.repID);
  metrics_->keyExchangedOnStartCounter.Get().Inc();
  LOG_DEBUG(KEY_EX_LOG, "Exchanged [" << keyStore_.exchangedReplicas.size() << "] out of [" << clusterSize_ << "]");
  if (keyStore_.exchangedReplicas.size() == clusterSize_) {
    keysView_.rotate(keysView_.privateKey, keysView_.outstandingPrivateKey);
    keysView_.save();
    LOG_INFO(KEY_EX_LOG, "building crypto system ");
    notifyRegistry();
    keysExchanged = true;
    LOG_INFO(KEY_EX_LOG, "All replicas exchanged keys, can start accepting msgs");
  }
}

void KeyManager::notifyRegistry() {
  for (uint32_t i = 0; i < clusterSize_; i++) {
    keysView_.publicKeys[i] = keyStore_.getReplicaPublicKey(i).key;
    if (i == repID_) {
      std::for_each(registryToExchange_.begin(), registryToExchange_.end(), [this](IKeyExchanger* e) {
        e->onPrivateKeyExchange(keysView_.privateKey, keyStore_.getReplicaPublicKey(repID_).key);
      });
      continue;
    }
    std::for_each(registryToExchange_.begin(), registryToExchange_.end(), [&, this](IKeyExchanger* e) {
      e->onPublicKeyExchange(keyStore_.getReplicaPublicKey(i).key, i);
    });
  }
  keysView_.save();
}

void KeyManager::loadCryptoKeysFromFile(const std::string& path, const uint16_t repID, const uint16_t numReplicas) {
  std::shared_ptr<ISaverLoader> sl(new FileSaverLoader(path, repID));
  KeysView kv(sl, numReplicas);
  for (int i = 0; i < numReplicas; ++i) {
    if (i == repID) {
      CryptoManager::instance().onPrivateKeyExchange(kv.privateKey, kv.publicKeys[i]);
      continue;
    }
    CryptoManager::instance().onPublicKeyExchange(kv.publicKeys[i], i);
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
    keysView_.rotate(keysView_.privateKey, keysView_.outstandingPrivateKey);
    keysView_.save();
  }
  LOG_DEBUG(KEY_EX_LOG, "Check point  " << num << " trigerred rotation ");
  LOG_DEBUG(KEY_EX_LOG, "building crypto system ");
  notifyRegistry();
}

void KeyManager::registerForNotification(IKeyExchanger* ke) { registryToExchange_.push_back(ke); }

KeyExchangeMsg KeyManager::getReplicaPublicKey(const uint16_t& repID) const {
  return keyStore_.getReplicaPublicKey(repID);
}

void KeyManager::loadKeysFromReservedPages() {
  if (!keyStore_.loadAllReplicasKeyStoresFromReservedPages()) return;
  // TODO E.L  KeyRotation implementation will need to rotate privete keys here, checkpoint number will be essential
  LOG_DEBUG(KEY_EX_LOG, "building crypto system ");
  notifyRegistry();
}

void KeyManager::sendKeyExchange() {
  KeyExchangeMsg msg;
  auto [prv, pub] = multiSigKeyHdlr_->generateMultisigKeyPair();
  keysView_.publishPrivateKey = prv;
  msg.key = pub;
  keysView_.save();
  msg.repID = repID_;
  std::stringstream ss;
  concord::serialize::Serializable::serialize(ss, msg);
  auto strMsg = ss.str();
  client_->sendRquest(bftEngine::KEY_EXCHANGE_FLAG, strMsg.size(), strMsg.c_str(), generateCid());
  LOG_DEBUG(KEY_EX_LOG, "Sending key exchange msg");
}

// First Key exchange is on start, in order not to trigger view change, we'll wait for all replicas to be connected.
// In order not to block it's done as async operation.
// If transport is UDP, we can't know the connection status, and we are in Apollo context therefore giving 2sec grace.
void KeyManager::sendInitialKey() {
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

/////////////PRIVATE KEYS////////////////////////////
KeyManager::KeysView::KeysView(std::shared_ptr<ISaverLoader> sl, uint32_t clusterSize) : sl(sl) {
  publicKeys.resize(clusterSize);
  load();
}

void KeyManager::KeysView::rotate(std::string& dst, std::string& src) {
  ConcordAssert(src.empty() == false);
  dst = src;
  src.clear();
}

const std::string KeyManager::KeysView::getVersion() const { return "1"; }

void KeyManager::KeysView::serializeDataMembers(std::ostream& outStream) const {
  serialize(outStream, privateKey);
  serialize(outStream, outstandingPrivateKey);
  serialize(outStream, publishPrivateKey);
  serialize(outStream, publicKeys);
}

void KeyManager::KeysView::deserializeDataMembers(std::istream& inStream) {
  deserialize(inStream, privateKey);
  deserialize(inStream, outstandingPrivateKey);
  deserialize(inStream, publishPrivateKey);
  deserialize(inStream, publicKeys);
}

void KeyManager::KeysView::save() {
  if (!sl) return;
  std::stringstream ss;
  concord::serialize::Serializable::serialize(ss, *this);
  sl->save(ss.str());
}

void KeyManager::KeysView::load() {
  if (!sl) return;
  auto str = sl->load();
  if (str.empty()) return;
  std::stringstream ss;
  ss.write(str.c_str(), std::streamsize(str.size()));
  concord::serialize::Serializable::deserialize(ss, *this);
}

void KeyManager::FileSaverLoader::save(const std::string& str) {
  if (str.empty()) return;
  std::ofstream myfile;
  myfile.open(fileName.c_str());
  if (!myfile.good()) {
    LOG_ERROR(KEY_EX_LOG, "Couldn't save key file to " << fileName);
    return;
  }
  myfile << str;
  myfile.close();
}

std::string KeyManager::FileSaverLoader::load() {
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