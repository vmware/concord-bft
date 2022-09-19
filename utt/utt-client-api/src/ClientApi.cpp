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

// libutt new api
#include <UTTParams.hpp>
#include <serialization.hpp>

namespace utt::client {

// Matches the expected data structure by UTTParams::create since the method hides the actual type by using a void*
struct CommitmentKeys {
  libutt::CommKey cck;
  libutt::CommKey rck;
};

void Initialize() {
  static bool s_initialized = false;

  if (!s_initialized) {
    // Initialize underlying libraries
    libutt::api::UTTParams::BaseLibsInitData base_libs_init_data;
    libutt::api::UTTParams::initLibs(base_libs_init_data);

    s_initialized = true;
  }
}

Configuration generateConfig(const ConfigInputParams& inputParams) {
  // We derive the size of the multiparty entities from the size of the provided input public keys
  if (inputParams.multipartyPublicKeys.empty())
    throw std::runtime_error("Generating UTT Instance Config with empty multiparty entries!");

  if (inputParams.t == 0) throw std::runtime_error("Generating UTT Instance Config with zero corruption threshold!");

  if (inputParams.t >= inputParams.multipartyPublicKeys.size())
    throw std::runtime_error(
        "Generating UTT Instance Config with threshold not less than the number of multiparty entities!");

  const size_t n = inputParams.multipartyPublicKeys.size();
  const size_t t = inputParams.t;

  // Generate public parameters and committer (bank) authority secret keys
  auto dkg = libutt::RandSigDKG(t, n, libutt::Params::NumMessages);

  // Generate registration authority secret keys
  auto rsk = libutt::RegAuthSK::generateKeyAndShares(t, n);

  // Pass in the commitment keys to UTTParams
  CommitmentKeys commitmentKeys{dkg.getCK(), rsk.ck_reg};
  auto publicParams = libutt::api::UTTParams::create((void*)(&commitmentKeys));

  // For some reason we need to go back and set the IBE parameters although we might not be using IBE.
  rsk.setIBEParams(publicParams.getParams().ibe);

  Configuration config;
  config.useBudget = inputParams.useBudget;
  config.publicParams = libutt::api::serialize<libutt::api::UTTParams>(publicParams);
  config.commitVerificationKey = libutt::api::serialize<libutt::RandSigPK>(dkg.sk.toPK());
  config.registrationVerificationKey = libutt::api::serialize<libutt::RegAuthPK>(rsk.toPK());

  // Add public verification keys
  config.commitVerificationKey = libutt::api::serialize<libutt::RandSigPK>(dkg.sk.toPK());
  config.registrationVerificationKey = libutt::api::serialize<libutt::RegAuthPK>(rsk.toPK());

  // Add keys per multiparty entity
  for (size_t i = 0; i < inputParams.multipartyPublicKeys.size(); ++i) {
    // Add encrypted secret key shares
    // [TODO-UTT] Encrypt each secret share with the entities's public key
    config.encryptedCommitSecrets.emplace_back(libutt::api::serialize<libutt::RandSigShareSK>(dkg.skShares[i]));
    config.encryptedRegistrationSecrets.emplace_back(libutt::api::serialize<libutt::RegAuthShareSK>(rsk.shares[i]));

    // Add public key shares
    config.committerVerificationKeyShares.emplace_back(
        libutt::api::serialize<libutt::RandSigSharePK>(dkg.skShares[i].toPK()));
    config.registrationVerificationKeyShares.emplace_back(
        libutt::api::serialize<libutt::RegAuthSharePK>(rsk.shares[i].toPK()));
  }

  return config;
}

}  // namespace utt::client