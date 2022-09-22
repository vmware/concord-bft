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
    ServerMock mock;
    mock.config_ =
        std::make_unique<libutt::api::Configuration>(libutt::api::deserialize<libutt::api::Configuration>(config));
    assertTrue(mock.config_->isValid());

    auto registrationVerificationKey = mock.config_->getPublicConfig().getRegistrationVerificationKey();
    auto commitVerificationKey = mock.config_->getPublicConfig().getCommitVerificationKey();

    // Create registrars and coins signers
    for (size_t i = 0; i < mock.config_->getNumParticipants(); ++i) {
      mock.registrars_.emplace_back(
          std::to_string(i), mock.config_->getRegistrationSecret(i), registrationVerificationKey);
      mock.coinsSigners_.emplace_back(
          std::to_string(i), mock.config_->getCommitSecret(i), commitVerificationKey, registrationVerificationKey);
    }

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
  cfgInputParams.threshold = F + 1;

  // Create a new UTT instance config
  auto config = utt::client::generateConfig(cfgInputParams);

  // Create a valid server-side mock based on the config
  auto serverMock = ServerMock::createFromConfig(config);
  (void)serverMock;

  // Create new users by using the public config
  const int C = 5;
  std::vector<std::unique_ptr<utt::client::User>> users;
  users.reserve(C);

  auto publicConfig = libutt::api::serialize<libutt::api::PublicConfig>(serverMock.config_->getPublicConfig());

  // Returns a copy of the same keys for every user - just for testing
  struct DummyUserPKInfrastructure : public utt::client::IUserPKInfrastructure {
    utt::client::IUserPKInfrastructure::KeyPair generateKeys(const std::string& userId) override {
      (void)userId;
      static const std::string s_secretKey =
          "-----BEGIN RSA PRIVATE KEY-----\n"
          "MIICXQIBAAKBgQDAyUwjK+m4EXyFKEha2NfQUY8eIqurRujNMqI1z7B3bF/8Bdg0\n"
          "wffIYJUjiXo13hB6yOq+gD62BGAPRjmgReBniT3d2rLU0Z72zQ64Gof66jCGQt0W\n"
          "0sfwDUv0XsfXXW9p1EGBYwIkgW6UGiABnkZcIUW4dzP2URoRCN/VIypkRwIDAQAB\n"
          "AoGAa3VIvSoTAoisscQ8YHcSBIoRjiihK71AsnAQvpHfuRFthxry4qVjqgs71i0h\n"
          "M7lt0iL/xePSEL7rlFf+cvnAFL4/j1R04ImBjRzWGnaNE8I7nNGGzJo9rL5I1oi3\n"
          "zN2yUucTSGm7qR0MCNVy26zNmCuS/FdBPsfdZ017OTsHtPECQQDlWXAJG6nHyw2o\n"
          "2cLYHzlyrrYgnWJkgFSKzr7VFNlHxfQSWXJ4zuDwhqkm3d176bVm4eHhDDv6f413\n"
          "iQGraKvTAkEA1zAzpxfI7LAqd3sObWYstQb03IXE7yddMgbhoMDCT3gXhNaHKfjT\n"
          "Z/GIk49jh8kyitN2FeYXXi9TiwrXStfhPQJBAMNea6ymjvstwoYKcgsOli5WG7ku\n"
          "uEkqdFoGAdObvfeA7gfPgE7e1AiwfVkpd+l9TVTFqFe/xzv8+fJQmEZ+lJcCQQDN\n"
          "5I7nh7h1zzEy1Qk+345TP262OT/u26kuHqtv1j+VLgDC10jIfg443D+jgITo/Tdg\n"
          "4WeRGHCva3TyCtNoBxq5AkA9KZpKof4ripad4oIuCJpR/ZhQATgQwR9f+FlAxgP0\n"
          "ABmBPCoxy4uGMtSBMqiiGpImbDuivYkhlBl7D8u8vn26\n"
          "-----END RSA PRIVATE KEY-----\n";

      static const std::string s_publicKey =
          "-----BEGIN PUBLIC KEY-----\n"
          "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDAyUwjK+m4EXyFKEha2NfQUY8e\n"
          "IqurRujNMqI1z7B3bF/8Bdg0wffIYJUjiXo13hB6yOq+gD62BGAPRjmgReBniT3d\n"
          "2rLU0Z72zQ64Gof66jCGQt0W0sfwDUv0XsfXXW9p1EGBYwIkgW6UGiABnkZcIUW4\n"
          "dzP2URoRCN/VIypkRwIDAQAB\n"
          "-----END PUBLIC KEY-----\n";
      return utt::client::IUserPKInfrastructure::KeyPair{s_secretKey, s_publicKey};
    }
  };

  utt::client::IUserStorage storage;
  DummyUserPKInfrastructure pki;

  for (int i = 0; i < C; ++i) {
    users.emplace_back(utt::client::createUser("user-" + std::to_string(i), publicConfig, pki, storage));
  }

  for (const auto& user : users) {
    std::cout << user->getUserId() << " created!\n";
  }

  return 0;
}