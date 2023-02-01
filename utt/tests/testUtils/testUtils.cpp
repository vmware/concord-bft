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

#include "testUtils.hpp"
#include "testKeys.hpp"

namespace libutt::api::testing {
void test_utt_instance::setUp(bool ibe, bool register_clients, bool init) {
  pr_keys = {k_TestKeys.at("user-1").first, k_TestKeys.at("user-2").first, k_TestKeys.at("user-3").first};
  pkeys = {k_TestKeys.at("user-1").second, k_TestKeys.at("user-2").second, k_TestKeys.at("user-3").second};
  if (init) {
    libutt::api::UTTParams::BaseLibsInitData base_libs_init_data;
    libutt::api::UTTParams::initLibs(base_libs_init_data);
  }
  config = std::make_unique<libutt::api::Configuration>(n, thresh);
  d = config->getPublicConfig().getParams();

  std::map<uint16_t, std::string> validation_keys;
  for (size_t i = 0; i < n; i++) {
    validation_keys[(uint16_t)i] = config->getRegistrationVerificationKeyShare((uint16_t)i);
  }
  for (size_t i = 0; i < n; i++) {
    registrators.push_back(std::make_shared<Registrator>((uint16_t)i,
                                                         config->getRegistrationSecret((uint16_t)i),
                                                         validation_keys,
                                                         config->getPublicConfig().getRegistrationVerificationKey()));
  }

  std::map<uint16_t, std::string> share_verification_keys_;
  for (size_t i = 0; i < n; i++) {
    share_verification_keys_[(uint16_t)i] = config->getCommitVerificationKeyShare((uint16_t)i);
  }
  for (size_t i = 0; i < n; i++) {
    banks.push_back(std::make_shared<CoinsSigner>((uint16_t)i,
                                                  config->getCommitSecret((uint16_t)i),
                                                  config->getPublicConfig().getCommitVerificationKey(),
                                                  share_verification_keys_,
                                                  config->getPublicConfig().getRegistrationVerificationKey()));
  }

  if (ibe) {
    GenerateIbeClients();
  } else {
    GenerateRsaClients(pr_keys);
  }

  if (register_clients) {
    for (auto& c : clients) {
      registerClient(c);
    }
  }
}
std::vector<uint32_t> test_utt_instance::getSubset(uint32_t n, uint32_t size) {
  std::srand((unsigned int)std::chrono::system_clock::now().time_since_epoch().count());
  std::map<uint32_t, uint32_t> ret;
  for (uint32_t i = 0; i < n; i++) ret[i] = i;
  for (uint32_t i = 0; i < n - size; i++) {
    uint32_t index = (uint32_t)(std::rand()) % (uint32_t)(ret.size());
    ret.erase(index);
  }
  std::vector<uint32_t> rret;
  for (const auto& [k, v] : ret) {
    (void)k;
    rret.push_back(v);
  }
  return rret;
}

void test_utt_instance::GenerateRsaClients(const std::vector<std::string>& pk) {
  c = pk.size();
  for (size_t i = 0; i < c; i++) {
    std::string pid = "client_" + std::to_string(i);
    clients.push_back(Client(pid,
                             config->getPublicConfig().getCommitVerificationKey(),
                             config->getPublicConfig().getRegistrationVerificationKey(),
                             pk.at(i)));
  }
}

void test_utt_instance::GenerateIbeClients() {
  for (size_t i = 0; i < c; i++) {
    std::string pid = "client_" + std::to_string(i);
    libutt::IBE::MSK msk = libutt::deserialize<libutt::IBE::MSK>(config->getIbeMsk());
    auto sk = msk.deriveEncSK(config->getPublicConfig().getParams().getImpl()->p.ibe, pid);
    auto mpk = msk.toMPK(config->getPublicConfig().getParams().getImpl()->p.ibe);
    std::string csk = libutt::serialize<libutt::IBE::EncSK>(sk);
    std::string cmpk = libutt::serialize<libutt::IBE::MPK>(mpk);
    clients.push_back(Client(pid,
                             config->getPublicConfig().getCommitVerificationKey(),
                             config->getPublicConfig().getRegistrationVerificationKey(),
                             csk,
                             cmpk));
  }
}

void test_utt_instance::registerClient(Client& c) {
  size_t n = registrators.size();
  std::vector<std::vector<uint8_t>> shares;
  std::vector<uint16_t> shares_signers;
  auto prf = c.getPRFSecretKey();
  Fr fr_s2 = Fr::random_element();
  types::CurvePoint s2;
  auto rcm1 = c.generateInputRCM();
  for (auto& r : registrators) {
    auto [ret_s2, sig] = r->signRCM(c.getPidHash(), fr_s2.to_words(), rcm1);
    shares.push_back(sig);
    shares_signers.push_back(r->getId());
    if (s2.empty()) {
      s2 = ret_s2;
    }
  }
  for (auto& r : registrators) {
    for (size_t i = 0; i < shares.size(); i++) {
      auto& sig = shares[i];
      auto& signer = shares_signers[i];
      assertTrue(r->validatePartialRCMSig(signer, c.getPidHash(), s2, rcm1, sig));
    }
  }
  auto sids = getSubset((uint32_t)n, (uint32_t)thresh);
  std::map<uint32_t, std::vector<uint8_t>> rsigs;
  for (auto i : sids) {
    rsigs[i] = shares[i];
  }
  auto sig = Utils::aggregateSigShares((uint32_t)n, rsigs);
  sig = Utils::unblindSignature(d, Commitment::Type::REGISTRATION, {Fr::zero().to_words(), Fr::zero().to_words()}, sig);
  c.setRCMSig(d, s2, sig);
}
}  // namespace libutt::api::testing