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

#include "client/clientservice/configuration.hpp"

#include <chrono>
#include <string>
#include <sstream>

#include "assertUtils.hpp"
#include "client/clientservice/client_service.hpp"

using concord::client::concordclient::ConcordClientConfig;

static auto logger = logging::getLogger("concord.client.clientservice.configuration");

namespace concord::client::clientservice {

// Copy a value from the YAML node to `out`.
// Throws and exception if no value could be read but the value is required.
template <typename T>
static void readYamlField(const YAML::Node& yaml, const std::string& index, T& out, bool value_required = true) {
  try {
    out = yaml[index].as<T>();
  } catch (const std::exception& e) {
    if (value_required) {
      // We ignore the YAML excpetions because they aren't useful
      std::ostringstream msg;
      msg << "Failed to read \"" << index << "\"";
      throw std::runtime_error(msg.str().data());
    } else {
      LOG_INFO(logger, "No value found for \"" << index << "\"");
    }
  }
}
static void readYamlField(const YAML::Node& yaml,
                          const std::string& index,
                          std::chrono::milliseconds& out,
                          bool value_required = true) {
  try {
    out = std::chrono::milliseconds(yaml[index].as<uint64_t>());
  } catch (const std::exception& e) {
    if (value_required) {
      // We ignore the YAML excpetions because they aren't useful
      std::ostringstream msg;
      msg << "Failed to read milliseconds \"" << index << "\"";
      throw std::runtime_error(msg.str().data());
    } else {
      LOG_INFO(logger, "No value found for \"" << index << "\"");
    }
  }
}

void parseConfigFile(ConcordClientConfig& config, const YAML::Node& yaml) {
  readYamlField(yaml, "f_val", config.topology.f_val);
  readYamlField(yaml, "c_val", config.topology.c_val);
  readYamlField(yaml,
                "client_sends_request_to_all_replicas_first_thresh",
                config.topology.client_sends_request_to_all_replicas_first_thresh);
  readYamlField(yaml,
                "client_sends_request_to_all_replicas_period_thresh",
                config.topology.client_sends_request_to_all_replicas_period_thresh);
  readYamlField(yaml, "signing_key_path", config.topology.signing_key_path);
  readYamlField(yaml, "encrypted_config_enabled", config.topology.encrypted_config_enabled);
  readYamlField(yaml, "transaction_signing_enabled", config.topology.transaction_signing_enabled);
  readYamlField(yaml, "client_batching_enabled", config.topology.client_batching_enabled);
  readYamlField(yaml, "client_batching_max_messages_nbr", config.topology.client_batching_max_messages_nbr);
  readYamlField(yaml, "client_batching_flush_timeout_ms", config.topology.client_batching_flush_timeout_ms);

  ConcordAssert(yaml["node"].IsSequence());
  for (const auto& node : yaml["node"]) {
    ConcordAssert(node.IsMap());
    ConcordAssert(node["replica"].IsSequence());
    ConcordAssert(node["replica"][0].IsMap());
    auto replica = node["replica"][0];

    concord::client::concordclient::ReplicaInfo ri;
    readYamlField(replica, "principal_id", ri.id.val);
    readYamlField(replica, "replica_host", ri.host);
    readYamlField(replica, "replica_port", ri.bft_port);
    readYamlField(replica, "event_port", ri.event_port);

    config.topology.replicas.push_back(ri);
  }

  readYamlField(yaml, "client_initial_retry_timeout_milli", config.topology.client_retry_config.initial_retry_timeout);
  readYamlField(yaml, "client_min_retry_timeout_milli", config.topology.client_retry_config.min_retry_timeout);
  readYamlField(yaml, "client_max_retry_timeout_milli", config.topology.client_retry_config.max_retry_timeout);
  readYamlField(yaml, "client_samples_per_evaluation", config.topology.client_retry_config.samples_per_evaluation);
  readYamlField(yaml,
                "client_number_of_standard_deviations_to_tolerate",
                config.topology.client_retry_config.number_of_standard_deviations_to_tolerate);
  readYamlField(yaml, "client_samples_until_reset", config.topology.client_retry_config.samples_until_reset);
  readYamlField(yaml, "enable_mock_comm", config.transport.enable_mock_comm);

  readYamlField(yaml, "concord-bft_communication_buffer_length", config.transport.buffer_length);

  concord::client::concordclient::TransportConfig::CommunicationType comm_type;
  std::string comm;
  readYamlField(yaml, "comm_to_use", comm);
  if (comm == "tls") {
    comm_type = concord::client::concordclient::TransportConfig::TlsTcp;
    readYamlField(yaml, "tls_certificates_folder_path", config.transport.tls_cert_root_path);
    readYamlField(yaml, "tls_cipher_suite_list", config.transport.tls_cipher_suite);
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
    readYamlField(client, "principal_id", ci.id.val);
    readYamlField(client, "client_port", ci.port);
    readYamlField(node["participant_node"][0], "participant_node_host", ci.host);
    config.bft_clients.push_back(ci);
  }

  config.num_of_used_bft_clients = yaml["clients_per_participant_node"].as<int16_t>();
}

void configureSubscription(concord::client::concordclient::ConcordClientConfig& config,
                           const std::string& tr_id,
                           bool is_insecure,
                           const std::string& tls_path) {
  config.subscribe_config.id = tr_id;
  config.subscribe_config.use_tls = not is_insecure;

  // TODO: Read TLS certs for this TRC instance
  config.subscribe_config.pem_cert_chain = "";
  config.subscribe_config.pem_private_key = "";
  config.transport.event_pem_certs = "";
}

}  // namespace concord::client::clientservice
