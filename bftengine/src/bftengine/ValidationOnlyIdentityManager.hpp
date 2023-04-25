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
#include "SigManager.hpp"
#pragma once

namespace bftEngine::impl {
/* This class is a hack to enable the validation of replica signatures
   using fixed keys without initializing a CryptoManager object.
   Messages such as CheckpointMsg use a global singleton to expose their validation logic,
   thus an instance of this class can be registered as a global SigManager to succeed in performing CheckpointMsg
   validations.
*/
class ValidationOnlyIdentityManager : public SigManager {
 public:
  ValidationOnlyIdentityManager(PrincipalId myId,
                                const std::vector<std::pair<Key, concord::crypto::KeyFormat>>& publickeys,
                                const std::map<PrincipalId, KeyIndex>& publicKeysMapping,
                                const ReplicasInfo& replicasInfo);

  bool verifySig(PrincipalId pid,
                 const concord::Byte* data,
                 size_t dataLength,
                 const concord::Byte* sig,
                 uint16_t sigLength) const override;

  static std::shared_ptr<SigManager> init(
      PrincipalId myId,
      const std::set<std::pair<PrincipalId, const std::string>>& publicKeysOfReplicas,
      const ReplicasInfo& replicasInfo);
};
}  // namespace bftEngine::impl
