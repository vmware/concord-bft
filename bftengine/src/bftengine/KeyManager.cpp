#include "KeyManager.h"
#include "thread"
#include "ReplicaImp.hpp"
#include "ReplicaConfig.hpp"
#include <memory>
#include "messages/ClientRequestMsg.hpp"

const std::string KeyManager::KeyExchangeMsg::getVersion() const { return "1"; }

KeyManager::KeyExchangeMsg::KeyExchangeMsg(std::string k, std::string s, int id)
    : key(std::move(k)), signature(std::move(s)), repID(id) {}

KeyManager::KeyExchangeMsg KeyManager::KeyExchangeMsg::deserializeMsg(const char* serializedMsg, const int& size) {
  std::stringstream ss;
  KeyManager::KeyExchangeMsg ke;
  ss.write(serializedMsg, std::streamsize(size));
  deserialize(ss, ke);
  return ke;
}

void KeyManager::KeyExchangeMsg::serializeDataMembers(std::ostream& outStream) const {
  serialize(outStream, key);
  LOG_TRACE(GL, "KEY EXCHANGE MANAGER  ser  key_ " << key);
  serialize(outStream, signature);
  LOG_TRACE(GL, "KEY EXCHANGE MANAGER  ser  signature_ " << signature);
  serialize(outStream, repID);
  LOG_TRACE(GL, "KEY EXCHANGE MANAGER  ser  repID_ " << repID);
}

void KeyManager::KeyExchangeMsg::deserializeDataMembers(std::istream& inStream) {
  deserialize(inStream, key);
  deserialize(inStream, signature);
  deserialize(inStream, repID);
}

std::string KeyManager::KeyExchangeMsg::toString() const {
  std::stringstream ss;
  ss << "key [" << key << "] signature [" << signature << "] replica id [" << repID << "]";
  return ss.str();
}

KeyManager::KeyManager(InternalBFTClient* cl, const int& id, const uint32_t& clusterSize)
    : repID_(id), clusterSize_(clusterSize), client_(cl) {}

/*
Usage:
  KeyExchangeMsg msg{"3c9dac7b594efaea8acd66a18f957f2e", "82c0700a4b907e189529fcc467fd8a1b", repID_};
  std::stringstream ss;
  concord::serialize::Serializable::serialize(ss, msg);
  auto strMsg = ss.str();
  client_->sendRquest(bftEngine::KEY_EXCHANGE_FLAG, strMsg.size(), strMsg.c_str(), generateCid());
*/
void KeyManager::sendKeyExchange() {
  (void)client_;
  LOG_DEBUG(GL, "KEY EXCHANGE MANAGER send msg");
}

std::string KeyManager::generateCid() {
  std::string cid{"KEY-EXCHANGE-"};
  auto now = getMonotonicTime().time_since_epoch();
  auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now);
  auto sn = now_ms.count();
  cid += std::to_string(repID_) + "-" + std::to_string(sn);
  return cid;
}

std::string KeyManager::onKeyExchange(const KeyExchangeMsg& kemsg) {
  LOG_DEBUG(GL, "KEY EXCHANGE MANAGER  msg " << kemsg.toString());
  if (!keysExchanged) {
    exchangedReplicas_.insert(kemsg.repID);
    LOG_DEBUG(GL, "Exchanged [" << exchangedReplicas_.size() << "] out of [" << clusterSize_ << "]");
    if (exchangedReplicas_.size() == clusterSize_) {
      keysExchanged = true;
      LOG_INFO(GL, "KEY EXCHANGE: start accepting msgs");
    }
  }

  return "ok";
}

void KeyManager::onCheckpoint(const int& num) { LOG_DEBUG(GL, "KEY EXCHANGE MANAGER check point  " << num); }
