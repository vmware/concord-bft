// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <chrono>

#include "example_config.hpp"

#include "client/clientservice/configuration.hpp"
#include "client/concordclient/concord_client.hpp"
#include "gtest/gtest.h"

using concord::client::clientservice::parseConfigFile;
using concord::client::concordclient::ConcordClientConfig;
using concord::client::concordclient::TransportConfig;

using namespace std::chrono_literals;

TEST(yaml_parsing, example_config) {
  const auto& yaml = YAML::Load(kExampleConf);
  ConcordClientConfig config;
  parseConfigFile(config, yaml);

  // Topology
  ASSERT_EQ(config.topology.f_val, 1);
  ASSERT_EQ(config.topology.c_val, 0);

  ASSERT_EQ(config.topology.replicas.size(), 4);
  for (auto i : {0, 1, 2, 3}) {
    ASSERT_EQ(config.topology.replicas[i].id.val, i);
    ASSERT_EQ(config.topology.replicas[i].host, "concord" + std::to_string(i + 1));
    ASSERT_EQ(config.topology.replicas[i].bft_port, 3501);
  }

  const auto& retry = config.topology.client_retry_config;
  ASSERT_EQ(retry.initial_retry_timeout, 500ms);
  ASSERT_EQ(retry.min_retry_timeout, 500ms);
  ASSERT_EQ(retry.max_retry_timeout, 3000ms);
  ASSERT_EQ(retry.number_of_standard_deviations_to_tolerate, 2);
  ASSERT_EQ(retry.samples_per_evaluation, 32);
  ASSERT_EQ(retry.samples_until_reset, 1000);

  // Transport
  ASSERT_EQ(config.transport.comm_type, TransportConfig::CommunicationType::TlsTcp);
  ASSERT_EQ(config.transport.buffer_length, 16777216);
  ASSERT_EQ(config.transport.tls_cert_root_path, "/concord/tls_certs");
  ASSERT_EQ(config.transport.tls_cipher_suite, "ECDHE-ECDSA-AES256-GCM-SHA384");

  // BFT Clients
  for (auto i : {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}) {
    ASSERT_EQ(config.bft_clients[i - 1].id.val, 20 + i);
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  int res = RUN_ALL_TESTS();
  return res;
}
