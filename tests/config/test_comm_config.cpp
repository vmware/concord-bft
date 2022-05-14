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

// This file includes functionality that both the client and the replica use,
// to set up communications and signatures.

#include "test_comm_config.hpp"

#include <unordered_map>
#include <vector>
#include <fstream>

#include "communication/CommFactory.hpp"
#include "KeyfileIOUtils.hpp"
#include <config_file_parser.hpp>
#include "CryptoManager.hpp"

using bft::communication::PlainUdpConfig;
using bft::communication::PlainTcpConfig;
using bft::communication::TlsTcpConfig;
using bft::communication::NodeNum;
using bft::communication::NodeInfo;
using bftEngine::ReplicaConfig;
using std::string;
using std::vector;

#define CLIENTS_CONFIG "clients_config"
#define REPLICAS_CONFIG "replicas_config"

const char* TestCommConfig::ip_port_delimiter_ = ":";
const std::string TestCommConfig::default_ip_ = "127.0.0.1";
// the default listen IP is a patch to be used on the machines where external
// IP is not available for listening (e.g. AWS). The patch is to listen on
// all interfaces, however, the clean solution will be to add listen IP to
// the config file - each replica and client should have "connect IP" for
// connecting to each other and "listen IP" - to listen to incoming connections
const std::string TestCommConfig::default_listen_ip_ = "0.0.0.0";

//////////////////////////////////////////////////////////////////////////////
// Create a replica config for the replica with index `replicaId`.
// inputReplicaKeyfile is used to read the keys for this replica, and default
// values are loaded for non-cryptographic configuration parameters.
void TestCommConfig::GetReplicaConfig(uint16_t replica_id,
                                      std::string keyFilePrefix,
                                      bftEngine::ReplicaConfig* out_config) {
  std::string key_file_name = keyFilePrefix + std::to_string(replica_id);
  auto sys = inputReplicaKeyfileMultisig(key_file_name, *out_config);
  if (sys) {
    std::unique_ptr<Cryptosystem> up(sys);
    bftEngine::CryptoManager::instance(std::move(up));
  }
}

std::unordered_map<NodeNum, NodeInfo> TestCommConfig::SetUpConfiguredNodes(bool is_replica,
                                                                           const std::string& config_file_name,
                                                                           uint16_t node_id,
                                                                           std::string& ip,
                                                                           uint16_t& port,
                                                                           uint16_t& num_of_clients,
                                                                           uint16_t& num_of_replicas) {
  concord::util::ConfigFileParser config_file_parser(logger_, config_file_name);
  try {
    config_file_parser.parse();
  } catch (const std::exception& e) {
    LOG_FATAL(logger_, e.what());
    exit(-1);
  }
  num_of_clients = static_cast<uint16_t>(config_file_parser.count(CLIENTS_CONFIG));
  num_of_replicas = static_cast<uint16_t>(config_file_parser.count(REPLICAS_CONFIG));
  if ((is_replica && (node_id + 1 > num_of_replicas)) ||
      (!is_replica && (node_id + 1 > num_of_clients + num_of_replicas))) {
    LOG_FATAL(logger_,
              "Wrong number of clients/replicas configured: "
                  << "numOfClients=" << num_of_clients << ", numOfReplicas=" << num_of_replicas);
    exit(-1);
  }
  auto replicas = config_file_parser.get_values<std::string>(REPLICAS_CONFIG);
  auto clients = config_file_parser.get_values<std::string>(CLIENTS_CONFIG);
  std::unordered_map<NodeNum, NodeInfo> nodes;
  int k = 0;
  for (int i = 0; i < (num_of_replicas + num_of_clients); i++) {
    vector<string>& current_vector = replicas;
    if ((k == i) && (i >= num_of_replicas)) {
      // All replicas were handled, now switch to clients.
      current_vector = clients;
      k = 0;
    }
    vector<string> ip_port_pair = config_file_parser.splitValue(current_vector[k++], ip_port_delimiter_);
    LOG_INFO(logger_,
             "setUpConfiguredNodes() node_id: " << node_id << ", k: " << k - 1
                                                << ", port:" << (uint16_t)(std::stoi(ip_port_pair[1])));
    if (ip_port_pair.size() != 2) {
      LOG_FATAL(logger_,
                "Wrong number of parameters configured for "
                "replica/client ip/port pair: "
                    << ip_port_pair.size());
      exit(-1);
    }
    if (i == node_id) {
      ip = ip_port_pair[0];
      port = static_cast<uint16_t>(std::stoi(ip_port_pair[1]));
    }
    nodes.insert(
        {i, NodeInfo{ip_port_pair[0], static_cast<uint16_t>(std::stoi(ip_port_pair[1])), i < num_of_replicas}});
  }
  return nodes;
}

std::unordered_map<NodeNum, NodeInfo> TestCommConfig::SetUpDefaultNodes(
    uint16_t node_id, std::string& ip, uint16_t& port, uint16_t num_of_clients, uint16_t num_of_replicas) {
  ip = default_ip_;
  port = static_cast<uint16_t>(base_port_ + node_id * 2);
  // Create a map of where the port for each node is.
  std::unordered_map<NodeNum, NodeInfo> nodes;
  for (int i = 0; i < (num_of_replicas + num_of_clients); i++)
    nodes.insert({i, NodeInfo{ip, static_cast<uint16_t>(base_port_ + i * 2), i < num_of_replicas}});
  return nodes;
}

std::unordered_map<NodeNum, NodeInfo> TestCommConfig::SetUpNodes(bool is_replica,
                                                                 uint16_t node_id,
                                                                 std::string& ip,
                                                                 uint16_t& port,
                                                                 uint16_t& num_of_clients,
                                                                 uint16_t& num_of_replicas,
                                                                 const std::string& config_file_name) {
  std::unordered_map<NodeNum, NodeInfo> nodes;
  if (config_file_name.empty())
    return SetUpDefaultNodes(node_id, ip, port, num_of_clients, num_of_replicas);
  else
    return SetUpConfiguredNodes(is_replica, config_file_name, node_id, ip, port, num_of_clients, num_of_replicas);
}

// Create a UDP communication configuration for the node (replica or client)
// with index `id`.
PlainUdpConfig TestCommConfig::GetUDPConfig(bool is_replica,
                                            uint16_t node_id,
                                            uint16_t& num_of_clients,
                                            uint16_t& num_of_replicas,
                                            const std::string& config_file_name) {
  string ip;
  uint16_t port;
  std::unordered_map<NodeNum, NodeInfo> nodes =
      SetUpNodes(is_replica, node_id, ip, port, num_of_clients, num_of_replicas, config_file_name);

  PlainUdpConfig ret_val(default_listen_ip_, port, buf_length_, nodes, node_id);
  return ret_val;
}

// Create a UDP communication configuration for the node (replica or client)
// with index `id`.
PlainTcpConfig TestCommConfig::GetTCPConfig(bool is_replica,
                                            uint16_t node_id,
                                            uint16_t& num_of_clients,
                                            uint16_t& num_of_replicas,
                                            const std::string& config_file_name) {
  string ip;
  uint16_t port;
  std::unordered_map<NodeNum, NodeInfo> nodes =
      SetUpNodes(is_replica, node_id, ip, port, num_of_clients, num_of_replicas, config_file_name);

  PlainTcpConfig ret_val(default_listen_ip_, port, buf_length_, nodes, num_of_replicas - 1, node_id);
  return ret_val;
}

TlsTcpConfig TestCommConfig::GetTlsTCPConfig(bool is_replica,
                                             uint16_t id,
                                             uint16_t& num_of_clients,
                                             uint16_t& num_of_replicas,
                                             const std::string& config_file_name,
                                             const std::string& cert_root_path) {
  string ip;
  uint16_t port;

  std::unordered_map<NodeNum, NodeInfo> nodes =
      SetUpNodes(is_replica, id, ip, port, num_of_clients, num_of_replicas, config_file_name);

  // private key decryption configuration for tests
  concord::secretsmanager::SecretData secretData;
  secretData.algo = "AES/CBC/PKCS5Padding";
  secretData.key = "15ec11a047f630ca00f65c25f0b3bfd89a7054a5b9e2e3cdb6a772a58251b4c2";
  secretData.iv = "38106509f6528ff859c366747aa04f21";

  // need to move the default cipher suite to the config file
  TlsTcpConfig retVal(default_listen_ip_,
                      port,
                      buf_length_,
                      nodes,
                      num_of_replicas - 1,
                      id,
                      cert_root_path,
                      "TLS_AES_256_GCM_SHA384",
                      false,
                      nullptr,
                      secretData);
  return retVal;
}
