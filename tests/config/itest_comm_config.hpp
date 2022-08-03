// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <string>

#include "Logger.hpp"
#include "communication/CommDefs.hpp"
#include "ReplicaConfig.hpp"

class ITestCommConfig {
 public:
  explicit ITestCommConfig(logging::Logger& logger) : logger_(logger) {}
  virtual ~ITestCommConfig() = default;

  // Create a replica config for the replica with index `replicaId`.
  // inputReplicaKeyfile is used to read the keys for this replica, and
  // default values are loaded for non-cryptographic configuration parameters.
  virtual void GetReplicaConfig(uint16_t replica_id,
                                std::string keyFilePrefix,
                                bftEngine::ReplicaConfig* out_config) = 0;

  // Create a UDP communication configuration for the node (replica or client)
  // with index `id`.
  virtual bft::communication::PlainUdpConfig GetUDPConfig(bool is_replica,
                                                          uint16_t id,
                                                          uint16_t& num_of_clients,
                                                          uint16_t& num_of_replicas,
                                                          const std::string& config_file_name) = 0;

  // Create a UDP communication configuration for the node (replica or client)
  // with index `id`.
  virtual bft::communication::PlainTcpConfig GetTCPConfig(bool is_replica,
                                                          uint16_t id,
                                                          uint16_t& num_of_clients,
                                                          uint16_t& num_of_replicas,
                                                          const std::string& config_file_name) = 0;

  virtual bft::communication::TlsTcpConfig GetTlsTCPConfig(bool is_replica,
                                                           uint16_t id,
                                                           uint16_t& num_of_clients,
                                                           uint16_t& num_of_replicas,
                                                           const std::string& config_file_name,
                                                           bool use_unified_certs,
                                                           const std::string& cert_root_path) = 0;

 protected:
  logging::Logger& logger_;
};
