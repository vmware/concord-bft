// UTT Client API
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

#include <vector>
#include <string>
#include <variant>
#include <optional>

#include "utt-common-api/CommonApi.hpp"
#include "utt-client-api/User.hpp"

namespace utt::client {

// [TODO-UTT] All types are tentative

// Provides the means to store the user's wallet data
struct IWalletStorage {};

// Provides the means to generate a public/private key pair when creating a user
struct IPKInfrastructure {};

struct ConfigInputParams {
  bool useBudget = true;  // Disable/enable usage of a budget token
  std::vector<std::string> multipartyPublicKeys;
  uint16_t t = 0;  // Corruption threshold
};

/// @brief Initialize the UTT Client API. Must be called before any other API function.
void Initialize();

/// @brief Generates a UTT Instance Configuration given the input parameters
/// @param inputParams
/// @return unique UTT instance configuration
Configuration generateConfig(const ConfigInputParams& inputParams);

// Creates and initialize a new user
User createUser(const std::string& userId,
                const std::vector<uint8_t>& params,
                IPKInfrastructure& pki,
                IWalletStorage& storage);

// Load an existing user from storage
User loadUserFromStorage(IWalletStorage& storage);

}  // namespace utt::client