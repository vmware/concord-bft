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

#include "utt-client-api/ClientApi.hpp"

// libutt
#include <utt/Params.h>
#include <utt/RandSigDKG.h>
#include <utt/RegAuth.h>
#include <utt/Serialization.h>

// libutt new api
#include "config.hpp"
#include <UTTParams.hpp>
#include <serialization.hpp>

namespace utt::client {

bool s_initialized = false;

// Matches the expected data structure by UTTParams::create since the method hides the actual type by using a void*
struct CommitmentKeys {
  libutt::CommKey cck;
  libutt::CommKey rck;
};

void Initialize() {
  if (!s_initialized) {
    // Initialize underlying libraries
    libutt::api::UTTParams::BaseLibsInitData base_libs_init_data;
    libutt::api::UTTParams::initLibs(base_libs_init_data);

    s_initialized = true;
  }
}

Configuration generateConfig(const ConfigInputParams& inputParams) {
  if (!s_initialized) throw std::runtime_error("Privacy Client API not initialized!");

  // We derive the size of the validators from the size of the provided input public keys
  if (inputParams.validatorPublicKeys.empty())
    throw std::runtime_error("Generating UTT Instance Config with empty validator public keys!");

  if (inputParams.threshold == 0) throw std::runtime_error("Generating UTT Instance Config with zero threshold!");

  if (inputParams.threshold > inputParams.validatorPublicKeys.size())
    throw std::runtime_error("Generating UTT Instance Config with threshold greater than the number of validators!");

  const uint16_t n = (uint16_t)inputParams.validatorPublicKeys.size();
  const uint16_t t = inputParams.threshold;

  auto config = libutt::api::Configuration(n, t);

  // [TODO-UTT] Use the validator's public keys to encrypt the configuration secrets for each validator

  return libutt::api::serialize<libutt::api::Configuration>(config);
}

PublicConfig getPublicConfig(const Configuration& config) {
  if (!s_initialized) throw std::runtime_error("Privacy Client API not initialized!");
  auto temp = libutt::api::deserialize<libutt::api::Configuration>(config);
  return libutt::api::serialize<libutt::api::PublicConfig>(temp.getPublicConfig());
}

std::unique_ptr<User> createUser(const std::string& userId,
                                 const PublicConfig& config,
                                 IUserPKInfrastructure& pki,
                                 IUserStorage& storage) {
  if (!s_initialized) throw std::runtime_error("Privacy Client API not initialized!");
  return User::createInitial(userId, config, pki, storage);
}

}  // namespace utt::client