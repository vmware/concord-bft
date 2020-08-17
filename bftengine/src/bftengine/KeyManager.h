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

#pragma once

#include "InternalBFTClient.h"
#include "KeyStore.h"

class KeyManager {
 public:
  static KeyManager& get(InternalBFTClient* cl = nullptr, const int id = 0, const uint32_t clusterSize = 0) {
    static KeyManager km{cl, id, clusterSize};
    return km;
  }

  void sendKeyExchange();
  std::string onKeyExchange(KeyExchangeMsg& kemsg, const uint64_t& sn);
  void onCheckpoint(const int& num);
  void registerForNotification(IKeyExchanger* ke);
  KeyExchangeMsg replicaKey(const uint16_t& repID) const;

  std::atomic_bool keysExchanged{false};

 private:
  KeyManager(InternalBFTClient* cl, const int& id, const uint32_t& clusterSize);

  uint16_t repID_{};
  uint32_t clusterSize_{};
  std::set<NodeIdType> exchangedReplicas_;  // TODO to/from Storage
  std::string generateCid();
  // Raw pointer is ok, since this class does not manage this resource.
  InternalBFTClient* client_{nullptr};

  std::vector<IKeyExchanger*> registryToExchange_;
  ClusterKeyStore keyStore_;

  // deleted
  KeyManager(const KeyManager&) = delete;
  KeyManager(const KeyManager&&) = delete;
  KeyManager& operator=(const KeyManager&) = delete;
  KeyManager& operator=(const KeyManager&&) = delete;
};
