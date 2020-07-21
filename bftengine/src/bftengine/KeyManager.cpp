#include "KeyManager.h"
#include "thread"
#include "ReplicaImp.hpp"
#include "ReplicaConfig.hpp"

const std::string KeyManager::KeyExchangeMsg::getVersion() const { return "1"; }

KeyManager::KeyExchangeMsg KeyManager::KeyExchangeMsg::deserializeMsg(const char* serializedMsg, const uint32_t& size) {
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

void KeyManager::set(bftEngine::IBasicClient* cl, const int& id) {
  cl_ = cl;
  repID_ = id;
}

/*
Usage:
  KeyExchangeMsg msg{key,sig,id};
  std::stringstream ss;
  concord::serialize::Serializable::serialize(ss, msg);
  auto strMsg = ss.str();
  char buff[128];
  uint32_t actSize{};
  // magic numbers, need to check valid values.
  cl_->invokeCommandSynch(strMsg.c_str(),
                          strMsg.size(),
                          bftEngine::KEY_EXCHANGE_FLAG,
                          std::chrono::milliseconds(60000),
                          44100,
                          buff,
                          &actSize);
*/
void KeyManager::sendKeyExchange() { LOG_DEBUG(GL, "KEY EXCHANGE MANAGER  send msg"); }

std::string KeyManager::onKeyExchange(const KeyExchangeMsg& kemsg) {
  static int counter = 0;
  counter++;
  LOG_DEBUG(GL, "KEY EXCHANGE MANAGER  msg " << kemsg.toString());
  auto numRep = bftEngine::ReplicaConfigSingleton::GetInstance().GetNumReplicas();
  if (counter < numRep) {
    LOG_DEBUG(GL, "Exchanged [" << counter << "] out of [" << numRep << "]");
  } else if (counter == numRep) {
    LOG_INFO(GL, "KEY EXCHANGE: start accepting msgs");
    keysExchanged = true;
  }
  return "ok";
}

void KeyManager::onCheckpoint(const int& num) { LOG_DEBUG(GL, "KEY EXCHANGE MANAGER check point  " << num); }
