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

KeyManager::KeyManager(InternalBFTClient* cl, const int& id, const uint32_t& clusterSize)
    : repID_(id), clusterSize_(clusterSize), client_(cl), keyStore_{clusterSize} {}

std::string KeyManager::generateCid() {
  std::string cid{"KEY-EXCHANGE-"};
  auto now = getMonotonicTime().time_since_epoch();
  auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now);
  auto sn = now_ms.count();
  cid += std::to_string(repID_) + "-" + std::to_string(sn);
  return cid;
}

std::string KeyManager::onKeyExchange(KeyExchangeMsg& kemsg, const uint64_t& sn) {
  LOG_DEBUG(GL, "KEY EXCHANGE MANAGER  msg " << kemsg.toString() << " seq num " << sn);
  if (!keysExchanged) {
    exchangedReplicas_.insert(kemsg.repID);
    LOG_DEBUG(GL, "KEY EXCHANGE: exchanged [" << exchangedReplicas_.size() << "] out of [" << clusterSize_ << "]");
    if (exchangedReplicas_.size() == clusterSize_) {
      keysExchanged = true;
      LOG_INFO(GL, "KEY EXCHANGE: start accepting msgs");
    }
  }

  keyStore_.push(kemsg, sn, registryToExchange_);

  return "ok";
}

void KeyManager::onCheckpoint(const int& num) {
  if (!keyStore_.rotate(num, registryToExchange_)) return;
  LOG_DEBUG(GL, "KEY EXCHANGE MANAGER check point  " << num << " trigerred rotation ");
}

void KeyManager::registerForNotification(IKeyExchanger* ke) { registryToExchange_.push_back(ke); }

KeyExchangeMsg KeyManager::replicaKey(const uint16_t& repID) const { return keyStore_.replicaKey(repID); }

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
