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

namespace bftEngine::impl {
class KeyManager {
 public:
  static KeyManager& get(InternalBFTClient* cl = nullptr,
                         const int id = 0,
                         const uint32_t clusterSize = 0,
                         IReservedPages* reservedPages = nullptr,
                         const uint32_t sizeOfReservedPage = 0) {
    static KeyManager km{cl, id, clusterSize, reservedPages, sizeOfReservedPage};
    return km;
  }

  void sendKeyExchange();
  std::string onKeyExchange(KeyExchangeMsg& kemsg, const uint64_t& sn);
  void onCheckpoint(const int& num);
  void registerForNotification(IKeyExchanger* ke);
  KeyExchangeMsg getReplicaKey(const uint16_t& repID) const;
  void loadKeysFromReservedPages();

  std::atomic_bool keysExchanged{false};

 private:
  KeyManager(InternalBFTClient* cl,
             const int& id,
             const uint32_t& clusterSize,
             IReservedPages* reservedPages,
             const uint32_t sizeOfReservedPage);

  uint16_t repID_{};
  uint32_t clusterSize_{};
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

}  // namespace bftEngine::impl