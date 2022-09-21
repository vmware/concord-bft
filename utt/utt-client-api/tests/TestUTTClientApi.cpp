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

#include <utt-client-api/ClientApi.hpp>

#include <xassert/XAssert.h>

#include <memory>

// libutt
#include <utt/RegAuth.h>
#include <utt/RandSig.h>

// libutt new interface
#include <registrator.hpp>
#include <coinsSigner.hpp>
#include <common.hpp>
#include <config.hpp>
#include <serialization.hpp>

using namespace libutt;

struct ServerMock {
  static ServerMock createFromConfig(const utt::Configuration& config) {
    // assertTrue(config.committerVerificationKeyShares.size() > 0);
    // assertTrue(config.committerVerificationKeyShares.size() == config.registrationVerificationKeyShares.size());

    ServerMock mock;
    mock.config_ =
        std::make_unique<libutt::api::Configuration>(libutt::api::deserialize<libutt::api::Configuration>(config));

    // Create registrars
    // for (size_t i = 0; i < config.encryptedRegistrationSecrets.size(); ++i) {
    //   mock.registrars.emplace_back(
    //       std::to_string(i), mock.config_.getRegistrationSecret(i), mock.config_.getRegistrationVerificationKey());
    // }

    // // Create coins signers
    // for (size_t i = 0; i < config.encryptedCommitSecrets.size(); ++i) {
    //   mock.coinsSigners.emplace_back(std::to_string(i),
    //                                  config.encryptedCommitSecrets[i],
    //                                  config.commitVerificationKey,
    //                                  config.registrationVerificationKey);
    // }

    return mock;
  }

  std::unique_ptr<libutt::api::Configuration> config_;
  std::vector<libutt::api::Registrator> registrars_;
  std::vector<libutt::api::CoinsSigner> coinsSigners_;
};

int main(int argc, char* argv[]) {
  (void)argc;
  (void)argv;

  // Note that this test assumes the client and server-side parts of the code work under the same initialization of
  // libutt.
  utt::client::Initialize();

  utt::client::ConfigInputParams cfgInputParams;

  // Create a UTT system tolerating F faulty participants
  const uint16_t F = 1;
  cfgInputParams.participantsPublicKeys = std::vector<std::string>(3 * F + 1, "placeholderForPublicKey");
  cfgInputParams.corruptionThreshold = F + 1;

  // Create a new UTT instance config
  auto config = utt::client::generateConfig(cfgInputParams);

  // Create a valid server-side mock based on the config
  auto serverMock = ServerMock::createFromConfig(config);
  (void)serverMock;

  // Create new users
  const int C = 5;
  std::vector<std::unique_ptr<utt::client::User>> users;
  users.reserve(C);

  // utt::client::IUserStorage storage;
  // utt::client::IUserPKInfrastructure pki;

  // for (int i = 0; i < C; ++i) {
  //   users.emplace_back(utt::client::createUser("user-" + std::to_string(i), config.publicParams, pki, storage));
  // }

  for (const auto& user : users) {
    std::cout << user->getUserId() << " created!\n";
  }

  return 0;
}