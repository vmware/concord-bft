// Concord
//
// Copyright (c) 2020-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.
#include "ValidationOnlyIdentityManager.hpp"

namespace bftEngine::impl {

ValidationOnlyIdentityManager::ValidationOnlyIdentityManager(
    PrincipalId myId,
    const std::vector<std::pair<Key, concord::crypto::KeyFormat>>& publickeys,
    const std::map<PrincipalId, KeyIndex>& publicKeysMapping,
    const ReplicasInfo& replicasInfo)
    : SigManager(myId,
                 {"", concord::crypto::KeyFormat::HexaDecimalStrippedFormat},
                 publickeys,
                 publicKeysMapping,
                 false,
                 {},
                 replicasInfo) {}

bool ValidationOnlyIdentityManager::verifySig(
    PrincipalId pid, const concord::Byte* data, size_t dataLength, const concord::Byte* sig, uint16_t sigLength) const {
  return verifySigUsingInternalMap(pid, data, dataLength, sig, sigLength);
}

// This method is assumed to be called by a single thread
std::shared_ptr<SigManager> ValidationOnlyIdentityManager::init(
    PrincipalId myId,
    const std::set<std::pair<PrincipalId, const std::string>>& publicKeysOfReplicas,
    const ReplicasInfo& replicasInfo) {
  std::vector<std::pair<Key, concord::crypto::KeyFormat>> publicKeysWithFormat(publicKeysOfReplicas.size());
  std::map<PrincipalId, KeyIndex> publicKeysMapping;
  for (auto& [id, key] : publicKeysOfReplicas) {
    publicKeysWithFormat[id] = {key, concord::crypto::KeyFormat::HexaDecimalStrippedFormat};
    publicKeysMapping.emplace(id, id);
  }

  auto manager =
      std::make_shared<ValidationOnlyIdentityManager>(myId, publicKeysWithFormat, publicKeysMapping, replicasInfo);
  SigManager::reset(manager);
  return manager;
}

}  // namespace bftEngine::impl
