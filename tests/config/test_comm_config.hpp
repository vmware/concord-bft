// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.
//
// This file includes functionality that both the client and the replica use,
// to set up communications and signatures.

#ifndef TEST_COMM_CONFIG_HPP
#define TEST_COMM_CONFIG_HPP

#include "itest_comm_config.hpp"

class TestCommConfig : public ITestCommConfig {
 public:
  explicit TestCommConfig(concordlogger::Logger& logger) : ITestCommConfig(logger) {}

  void GetReplicaConfig(uint16_t replica_id, std::string keyFilePrefix, bftEngine::ReplicaConfig* out_config) override;

  bftEngine::PlainUdpConfig GetUDPConfig(bool is_replica,
                                         uint16_t node_id,
                                         uint16_t& num_of_clients,
                                         uint16_t& num_of_replicas,
                                         const std::string& config_file_name) override;

  bftEngine::PlainTcpConfig GetTCPConfig(bool is_replica,
                                         uint16_t node_id,
                                         uint16_t& num_of_clients,
                                         uint16_t& num_of_replicas,
                                         const std::string& config_file_name) override;

  bftEngine::TlsTcpConfig GetTlsTCPConfig(bool is_replica,
                                          uint16_t id,
                                          uint16_t& num_of_clients,
                                          uint16_t& num_of_replicas,
                                          const std::string& config_file_name) override;

 private:
  std::unordered_map<NodeNum, NodeInfo> SetUpConfiguredNodes(bool is_replica,
                                                             const std::string& config_file_name,
                                                             uint16_t node_id,
                                                             std::string& ip,
                                                             uint16_t& port,
                                                             uint16_t& num_of_clients,
                                                             uint16_t& num_of_replicas);

  std::unordered_map<NodeNum, NodeInfo> SetUpDefaultNodes(
      uint16_t node_id, std::string& ip, uint16_t& port, uint16_t num_of_clients, uint16_t num_of_replicas);

  std::unordered_map<NodeNum, NodeInfo> SetUpNodes(bool is_replica,
                                                   uint16_t node_id,
                                                   std::string& ip,
                                                   uint16_t& port,
                                                   uint16_t& num_of_clients,
                                                   uint16_t& num_of_replicas,
                                                   const std::string& config_file_name);

 private:
  // Network port of the first replica. Other replicas use ports
  // basePort + (2 * index).
  static const uint16_t base_port_ = 3710;
  static const uint32_t buf_length_ = 128 * 1024;  // 128 kB
  static const std::string default_ip_;
  static const std::string default_listen_ip_;
  static const char* ip_port_delimiter_;
};

#endif
