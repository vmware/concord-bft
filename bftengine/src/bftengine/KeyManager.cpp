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

////////////////////////////// KEY MANAGER//////////////////////////////
namespace bftEngine::impl {
KeyManager::KeyManager(InternalBFTClient* cl,
                       const int& id,
                       const uint32_t& clusterSize,
                       IReservedPages* reservedPages,
                       const uint32_t sizeOfReservedPage)
    : repID_(id), clusterSize_(clusterSize), client_(cl), keyStore_{clusterSize, *reservedPages, sizeOfReservedPage} {
  if (keyStore_.exchangedReplicas.size() == clusterSize_) {
    keysExchanged = true;
    LOG_INFO(KEY_EX_LOG, "All replicas keys loaded from reserved pages, can start accepting msgs");
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

std::string KeyManager::onKeyExchange(KeyExchangeMsg& kemsg, const uint64_t& sn) {
  LOG_DEBUG(KEY_EX_LOG, "Recieved " << kemsg.toString() << " seq num " << sn);
  if (!keysExchanged) {
    keyStore_.exchangedReplicas.insert(kemsg.repID);
    LOG_DEBUG(KEY_EX_LOG, "Exchanged [" << keyStore_.exchangedReplicas.size() << "] out of [" << clusterSize_ << "]");
    if (keyStore_.exchangedReplicas.size() == clusterSize_) {
      keysExchanged = true;
      LOG_INFO(KEY_EX_LOG, "All replics exchanged keys, can start accepting msgs");
    }
  }

  keyStore_.push(kemsg, sn, registryToExchange_);

  return "ok";
}

void KeyManager::onCheckpoint(const int& num) {
  if (!keyStore_.rotate(num, registryToExchange_)) return;
  LOG_DEBUG(KEY_EX_LOG, "Check point  " << num << " trigerred rotation ");
}

void KeyManager::registerForNotification(IKeyExchanger* ke) { registryToExchange_.push_back(ke); }

KeyExchangeMsg KeyManager::getReplicaKey(const uint16_t& repID) const { return keyStore_.getReplicaKey(repID); }

void KeyManager::loadKeysFromReservedPages() { keyStore_.loadAllReplicasKeyStoresFromReservedPages(); }

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
  LOG_DEBUG(KEY_EX_LOG, "Sending key exchange msg");
}

}  // namespace bftEngine::impl