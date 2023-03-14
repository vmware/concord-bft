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
#include "UTTParams.hpp"
#include "coinsSigner.hpp"
#include "client.hpp"
#include "registrator.hpp"
#include "common.hpp"
#include "coin.hpp"
#include "config.hpp"
#include "testKeys.hpp"
#include <utt/RegAuth.h>
#include <utt/RandSigDKG.h>
#include <utt/Serialization.h>
#include <utt/IBE.h>
#include <utt/Serialization.h>
#include "../../libutt/src/api/include/params.impl.hpp"

#include <vector>
#include <memory>
#include <cstdlib>
#include <iostream>
#include <ctime>
#include <unordered_map>
#include <chrono>

#include "gtest/gtest.h"

using namespace libutt;
using namespace libutt::api;
namespace libutt::api::testing {

class test_utt_instance : public ::testing::Test {
 public:
 protected:
  void setUp(bool ibe, bool register_clients, bool init = true);
  std::vector<uint32_t> getSubset(uint32_t n, uint32_t size);

  void GenerateRsaClients(const std::vector<std::string>& pk);

  void GenerateIbeClients();

  void registerClient(Client& c);

 public:
  size_t thresh = 2;
  size_t n = 4;
  size_t c = 10;
  libutt::api::UTTParams d;
  std::unique_ptr<libutt::api::Configuration> config;
  std::vector<std::shared_ptr<CoinsSigner>> banks;
  std::vector<std::shared_ptr<Registrator>> registrators;
  std::vector<Client> clients;
  std::vector<std::string> pr_keys{
      k_TestKeys.at("user-1").first, k_TestKeys.at("user-2").first, k_TestKeys.at("user-3").first};
  std::vector<std::string> pkeys{
      k_TestKeys.at("user-1").second, k_TestKeys.at("user-2").second, k_TestKeys.at("user-3").second};
};
}  // namespace libutt::api::testing