// UTT Common API
//
// Copyright (c) 2020-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include <map>
#include <string>
#include <vector>

namespace utt {

// This is the configuration of a UTT instance
// [TODO-UTT] What should be the types for the different keys?

struct Configuration {
  bool useBudget = true;
  std::vector<std::vector<uint8_t>> encryptedCommitSecrets;
  std::vector<std::vector<uint8_t>> encryptedRegistrationSecrets;
  std::vector<std::vector<uint8_t>>
      committerVerificationKeyShares;  // [TODO-UTT] Check: I think we need this for partial sig verification
  std::vector<std::vector<uint8_t>>
      registrationVerificationKeyShares;  // [TODO-UTT] Check: I think we need this for partial sig verification
  std::vector<uint8_t> commitVerificationKey;
  std::vector<uint8_t> registrationVerificationKey;
  std::vector<uint8_t> publicParams;
};

}  // namespace utt