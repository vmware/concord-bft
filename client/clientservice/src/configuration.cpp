// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "configuration.hpp"

#include <chrono>
#include <string>
#include <boost/program_options.hpp>
#include <yaml-cpp/yaml.h>

#include "assertUtils.hpp"
#include "client_service.hpp"
#include "client/concordclient/concord_client.hpp"
#include "Logger.hpp"

using concord::client::clientservice::ClientService;
using concord::client::concordclient::ConcordClient;
using concord::client::concordclient::ConcordClientConfig;

namespace po = boost::program_options;

namespace concord::client::clientservice {

void configureConcordClient(ConcordClientConfig& config, po::variables_map& opts, logging::Logger& l) {
  auto file_path = opts["config"].as<std::string>();
  LOG_INFO(l, "config file name " << file_path);

  auto yaml = YAML::LoadFile(file_path);
  config.topology.f_val = yaml["f_val"].as<uint16_t>();
  config.topology.c_val = yaml["c_val"].as<uint16_t>();

  ConcordAssert(yaml["node"].IsSequence());
  for (const auto& node : yaml["node"]) {
    ConcordAssert(node.IsMap());
    ConcordAssert(node["replica"].IsSequence());
    ConcordAssert(node["replica"][0].IsMap());
    auto replica = node["replica"][0];

    concord::client::concordclient::ReplicaInfo ri;
    ri.id.val = replica["principal_id"].as<uint16_t>();
    ri.host = replica["replica_host"].as<std::string>();
    ri.bft_port = replica["replica_port"].as<uint16_t>();
    // TODO: Should come from the configuration as well
    ri.event_port = 50051;

    config.topology.replicas.push_back(ri);
  }

  config.topology.client_retry_config.initial_retry_timeout =
      std::chrono::milliseconds(yaml["client_initial_retry_timeout_milli"].as<unsigned>());
  config.topology.client_retry_config.min_retry_timeout =
      std::chrono::milliseconds(yaml["client_min_retry_timeout_milli"].as<unsigned>());
  config.topology.client_retry_config.max_retry_timeout =
      std::chrono::milliseconds(yaml["client_max_retry_timeout_milli"].as<unsigned>());
  config.topology.client_retry_config.number_of_standard_deviations_to_tolerate =
      yaml["client_number_of_standard_deviations_to_tolerate"].as<uint16_t>();
  config.topology.client_retry_config.samples_per_evaluation = yaml["client_samples_per_evaluation"].as<uint16_t>();
  config.topology.client_retry_config.samples_until_reset = yaml["client_samples_until_reset"].as<int16_t>();

  config.transport.buffer_length = yaml["concord-bft_communication_buffer_length"].as<uint32_t>();
  concord::client::concordclient::TransportConfig::CommunicationType comm_type;
  auto comm = yaml["comm_to_use"].as<std::string>();
  if (comm == "tls") {
    comm_type = concord::client::concordclient::TransportConfig::TlsTcp;
    config.transport.tls_cert_root_path = yaml["tls_certificates_folder_path"].as<std::string>();
    config.transport.tls_cipher_suite = yaml["tls_cipher_suite_list"].as<std::string>();
  } else if (comm == "udp") {
    comm_type = concord::client::concordclient::TransportConfig::PlainUdp;
  } else {
    comm_type = concord::client::concordclient::TransportConfig::Invalid;
  }
  config.transport.comm_type = comm_type;

  auto node = yaml["participant_nodes"][0];
  ConcordAssert(node.IsMap());
  ConcordAssert(node["participant_node"].IsSequence());
  ConcordAssert(node["participant_node"][0].IsMap());
  ConcordAssert(node["participant_node"][0]["external_clients"].IsSequence());
  for (const auto& item : node["participant_node"][0]["external_clients"]) {
    ConcordAssert(item.IsMap());
    ConcordAssert(item["client"].IsSequence());
    ConcordAssert(item["client"][0].IsMap());
    auto client = item["client"][0];

    concord::client::concordclient::BftClientInfo ci;
    ci.id.val = client["principal_id"].as<uint16_t>();
    // TODO: client_port
    config.bft_clients.push_back(ci);
  }

  // Event service
  config.subscribe_config.id = opts["event-service-id"].as<std::string>();

  // TODO: Read TLS certs and fill config struct

  // TODO: Configure TRS endpoints
}

}  // namespace concord::client::clientservice
