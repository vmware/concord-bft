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

#include "thread"
#include "ReplicaImp.hpp"
#include "ReplicaConfig.hpp"
#include <memory>
#include "messages/ClientRequestMsg.hpp"
#include "bftengine/CryptoManager.hpp"
#include "KeyExchangeManager.h"
#include "json_output.hpp"

////////////////////////////// KEY MANAGER//////////////////////////////
namespace bftEngine::impl {

using concordUtils::toPair;

KeyExchangeManager::KeyExchangeManager(InitData* id)
    : repID_(id->id),
      clusterSize_(id->clusterSize),
      client_(id->cl),
      keyStore_{id->clusterSize, id->reservedPages, id->sizeOfReservedPage},
      multiSigKeyHdlr_(id->kg),
      keysView_(id->sec, id->backupSec, id->clusterSize),
      keyExchangeOnStart_(id->keyExchangeOnStart),
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
    notifyRegistry(false);
    keysExchanged = true;
    LOG_INFO(KEY_EX_LOG, "All replicas keys loaded from reserved pages, can start accepting msgs");
  }
}

void KeyExchangeManager::initMetrics(std::shared_ptr<concordMetrics::Aggregator> a, std::chrono::seconds interval) {
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

std::string KeyExchangeManager::generateCid() {
  std::string cid{"KEY-EXCHANGE-"};
  auto now = getMonotonicTime().time_since_epoch();
  auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now);
  auto sn = now_ms.count();
  cid += std::to_string(repID_) + "-" + std::to_string(sn);
  return cid;
}

// Key exchange msg for replica has been recieved:
// update the replica public key store.
std::string KeyExchangeManager::onKeyExchange(KeyExchangeMsg& kemsg, const uint64_t& sn) {
  SCOPED_MDC_SEQ_NUM(std::to_string(sn));
  if (kemsg.op == KeyExchangeMsg::HAS_KEYS) {
    LOG_INFO(KEY_EX_LOG, "Has key query arrived, returning " << std::boolalpha << keysExchanged << std::noboolalpha);
    if (!keysExchanged && keyExchangeOnStart_) return std::string(KeyExchangeMsg::hasKeysFalseReply);
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
void KeyExchangeManager::onInitialKeyExchange(KeyExchangeMsg& kemsg, const uint64_t& sn) {
  SCOPED_MDC_SEQ_NUM(std::to_string(sn));
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
  notifyRegistry(true);
  keysView_.backup();
  keysExchanged = true;
  LOG_INFO(KEY_EX_LOG, "All replicas exchanged keys, can start accepting msgs");
}

void KeyExchangeManager::notifyRegistry(bool save) {
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
  if (save) {
    keysView_.save();
  } else {
    LOG_DEBUG(KEY_EX_LOG, "Not saving keyfile after updating crypto system");
  }
}

void KeyExchangeManager::loadCryptoFromKeyView(std::shared_ptr<ISecureStore> sec,
                                               const uint16_t repID,
                                               const uint16_t numReplicas) {
  KeysView kv(sec, nullptr, numReplicas);
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
void KeyExchangeManager::onCheckpoint(const int& num) {
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
  notifyRegistry(true);
}

void KeyExchangeManager::registerForNotification(IKeyExchanger* ke) { registryToExchange_.push_back(ke); }

KeyExchangeMsg KeyExchangeManager::getReplicaPublicKey(const uint16_t& repID) const {
  return keyStore_.getReplicaPublicKey(repID);
}

// Called at the end of a state transfer.
void KeyExchangeManager::loadKeysFromReservedPages() {
  // Until Key-rotation is implemented, state transfer after the key-exchange has been performed has no side effect.
  // in order to mitigate BC-5530, we'll not do a redundent work
  if (keysExchanged) {  // TODO remove when implementing key-rotation!
    LOG_INFO(KEY_EX_LOG, "After state transfer, return due to already updated crypto system");
    return;
  }
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
  notifyRegistry(true);
}

void KeyExchangeManager::sendKeyExchange() {
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
void KeyExchangeManager::sendInitialKey() {
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

void KeyExchangeManager::waitForFullCommunication() {
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

std::string KeyExchangeManager::getStatus() {
  std::ostringstream oss;
  std::unordered_map<std::string, std::string> result;
  result.insert(toPair("isInitialKeyExchangeCompleted", keysExchanged));
  result.insert(
      toPair("keyExchangedCounter", metrics_->aggregator->GetCounter("KeyManager", "KeyExchangedCounter").Get()));
  result.insert(toPair("KeyExchangedOnStartCounter",
                       metrics_->aggregator->GetCounter("KeyManager", "KeyExchangedOnStartCounter").Get()));
  result.insert(toPair("publicKeyRotated", metrics_->aggregator->GetCounter("KeyManager", "publicKeyRotated").Get()));
  oss << concordUtils::kContainerToJson(result);
  return oss.str();
}

/////////////KEYS VIEW////////////////////////////

const std::string KeyExchangeManager::KeysViewData::getVersion() const { return "1"; }

void KeyExchangeManager::KeysViewData::serializeDataMembers(std::ostream& outStream) const {
  serialize(outStream, privateKey);
  serialize(outStream, outstandingPrivateKey);
  serialize(outStream, generatedPrivateKey);
  serialize(outStream, publicKeys);
}

void KeyExchangeManager::KeysViewData::deserializeDataMembers(std::istream& inStream) {
  deserialize(inStream, privateKey);
  deserialize(inStream, outstandingPrivateKey);
  deserialize(inStream, generatedPrivateKey);
  deserialize(inStream, publicKeys);
}

KeyExchangeManager::KeysView::KeysView(std::shared_ptr<ISecureStore> sec,
                                       std::shared_ptr<ISecureStore> backSec,
                                       uint32_t clusterSize)
    : secStore(sec), backupSecStore(backSec) {
  data.publicKeys.resize(clusterSize);
}

void KeyExchangeManager::KeysView::rotate(std::string& dst, std::string& src) {
  ConcordAssert(src.empty() == false);
  dst = src;
  src.clear();
  save();
}
void KeyExchangeManager::KeysView::save() {
  if (!secStore) {
    LOG_DEBUG(KEY_EX_LOG, "Saver is not set");
    return;
  }
  save(secStore);
}

void KeyExchangeManager::KeysView::backup() {
  if (!backupSecStore) {
    LOG_DEBUG(KEY_EX_LOG, "Backup is not set");
    return;
  }
  save(backupSecStore);
}

void KeyExchangeManager::KeysView::save(std::shared_ptr<ISecureStore>& secureStore) {
  std::stringstream ss;
  concord::serialize::Serializable::serialize(ss, data);
  secureStore->save(ss.str());
}

bool KeyExchangeManager::KeysView::load() {
  if (!secStore) {
    LOG_DEBUG(KEY_EX_LOG, "Loader is not set");
    return false;
  }
  auto str = secStore->load();
  if (str.empty()) {
    LOG_WARN(KEY_EX_LOG, "Got empty string from loader. This is expected on first startup with an empty database");
    return false;
  }
  std::stringstream ss;
  ss.write(str.c_str(), std::streamsize(str.size()));
  concord::serialize::Serializable::deserialize(ss, data);
  LOG_DEBUG(KEY_EX_LOG, "Loaded successfully " << data.generatedPrivateKey << " " << data.outstandingPrivateKey);
  return true;
}

void KeyExchangeManager::FileSecureStore::save(const std::string& str) {
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

std::string KeyExchangeManager::FileSecureStore::load() {
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

void KeyExchangeManager::EncryptedFileSecureStore::save(const std::string& str) {
  auto enc = secretsMgr->encryptString(str);
  if (!enc.has_value()) {
    LOG_FATAL(KEY_EX_LOG, "Couldn't encrypt value while saving key file to " << store.fileName);
    ConcordAssert(false);
  }
  store.save(enc.value());
}

std::string KeyExchangeManager::EncryptedFileSecureStore::load() {
  auto enc = store.load();
  if (enc.length() == 0) {
    return enc;
  }

  auto res = secretsMgr->decryptString(enc);
  if (res.has_value()) {
    return res.value();
  } else {
    return "";  // this is the behaviour of FileSecureStore
  }
}

}  // namespace bftEngine::impl
