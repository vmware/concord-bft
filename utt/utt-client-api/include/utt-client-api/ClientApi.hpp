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

struct ConfigInputParams {
  bool useBudget = true;  // Disable/enable usage of a budget token
  std::vector<std::string> validatorPublicKeys;
  uint16_t threshold = 0;  // The number of validator shares required to reconstruct a signature
};

/// @brief Initialize the UTT Client API. Must be called before any other API function.
void Initialize();

/// @brief Generates a UTT Instance Configuration given the input parameters
/// @param inputParams
/// @return unique UTT instance configuration
Configuration generateConfig(const ConfigInputParams& inputParams);

/// @brief Convenience method to obtain the public part from an already existing config
/// @param config The input configuration
/// @return The public part of the configuration
PublicConfig getPublicConfig(const Configuration& config);

/// @brief Creates and initializes a new UTT user
/// @param userId A unique string identifying the user in the UTT instance
/// @param config The public config of the UTT instance
/// @param pki A public key infrastructure object that can be used to generate a public/private key pair for the user
/// @param storage A storage interface for the user's data
/// @return Newly created user object
std::unique_ptr<User> createUser(const std::string& userId,
                                 const PublicConfig& config,
                                 const std::string& private_key,
                                 const std::string& public_key,
                                 std::shared_ptr<IStorage> storage);

// Load an existing user from storage
std::unique_ptr<User> loadUserFromStorage(IStorage& storage);

}  // namespace utt::client