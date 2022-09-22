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
#include <memory>

#include "utt-common-api/CommonApi.hpp"
#include "utt-client-api/User.hpp"

namespace utt::client {

// [TODO-UTT] All types are tentative

// Provides the means to store the user's wallet data
struct IUserStorage {};

// Provides the means to generate a public/private key pair when creating a user
struct IUserPKInfrastructure {};

struct ConfigInputParams {
  bool useBudget = true;  // Disable/enable usage of a budget token
  std::vector<std::string> participantsPublicKeys;
  uint16_t threshold = 0;  // The number of participant shares required to reconstruct a signature
};

/// @brief Initialize the UTT Client API. Must be called before any other API function.
void Initialize();

/// @brief Generates a UTT Instance Configuration given the input parameters
/// @param inputParams
/// @return unique UTT instance configuration
Configuration generateConfig(const ConfigInputParams& inputParams);

/// @brief Creates and initializes a new UTT user
/// @param userId A unique string identifying the user in the UTT instance
/// @param params The public parameters of the UTT instance
/// @param pki A public key infrastructure object that can be used to generate a public/private key pair for the user
/// @param storage A storage interface for the user's data
/// @return Newly created user object
std::unique_ptr<User> createUser(const std::string& userId,
                                 const PublicParams& params,
                                 IUserPKInfrastructure& pki,
                                 IUserStorage& storage);

// Load an existing user from storage
std::unique_ptr<User> loadUserFromStorage(IUserStorage& storage);

}  // namespace utt::client