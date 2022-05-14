// Concord
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

#include <bftclient/bft_client.h>
#include <string>
#include "communication/CommDefs.hpp"
#include "communication/CommFactory.hpp"
#include "communication/ICommunication.hpp"

namespace concord {

namespace config {
class ConcordConfiguration;
}  // namespace config

namespace config_pool {
typedef struct ExternalClient {
  std::uint16_t client_port;
  std::uint16_t principal_id;
} ExternalClient;

typedef struct ParticipantNode {
  std::string participant_node_host;
  std::uint16_t principal_id;
  std::unordered_map<uint16_t, ExternalClient> externalClients;
} ParticipantNode;

typedef struct ConcordClientPoolConfig {
  std::uint16_t c_val = 0;
  std::uint16_t client_min_retry_timeout_milli = 5000;
  std::uint16_t client_periodic_reset_thresh = 30;
  std::uint16_t client_initial_retry_timeout_milli = 5000;
  std::uint16_t client_max_retry_timeout_milli = 10000;
  std::uint16_t client_sends_request_to_all_replicas_first_thresh = 2;
  std::uint16_t client_sends_request_to_all_replicas_period_thresh = 2;
  std::uint16_t client_number_of_standard_deviations_to_tolerate = 2;
  std::uint16_t client_samples_per_evaluation = 32;
  std::uint16_t client_samples_until_reset = 1000;
  std::uint16_t clients_per_participant_node = 1;
  std::uint16_t active_clients_in_pool = 0;
  bool enable_mock_comm = false;
  std::string comm_to_use = "tls";
  std::string concord_bft_communication_buffer_length = "64000";
  std::uint16_t f_val = 1;
  std::uint32_t num_replicas = 4;
  std::uint32_t client_proxies_per_replica = 4;
  std::string prometheus_port = "9891";
  std::string tls_certificates_folder_path = "/concord/tls_certs";
  std::string tls_cipher_suite_list = "ECDHE-ECDSA-AES256-GCM-SHA384";
  std::string signing_key_path = "resources/signing_keys";
  std::uint32_t trace_sampling_rate = 0;
  std::string file_name;
  bool client_batching_enabled = false;
  bool enable_multiplex_channel = false;
  bool use_unified_certificates = false;
  size_t client_batching_max_messages_nbr = 20;
  std::uint64_t client_batching_flush_timeout_ms = 100;
  bool encrypted_config_enabled = false;
  bool transaction_signing_enabled = false;
  bool with_cre = false;
  std::string secrets_url;
  bft::communication::NodeMap replicas;
  std::deque<ParticipantNode> participant_nodes;
  secretsmanager::SecretData secret_data;
  std::string path_to_replicas_master_key = std::string();
} ConcordClientPoolConfig;

class ClientPoolConfig {
 public:
  const std::string FILE_NAME = "concord_external_client";
  const std::string F_VAL = "f_val";
  const std::string C_VAL = "c_val";
  const std::string NUM_REPLICAS = "num_replicas";
  const std::string PARTICIPANT_NODES = "participant_nodes";
  const std::string PARTICIPANT_NODE = "participant_node";
  const std::string NUM_EXTERNAL_CLIENTS = "clients_per_participant_node";
  const int MAX_EXTERNAL_CLIENTS = 4096;
  const std::string COMM_PROTOCOL = "comm_to_use";
  const std::string CERT_FOLDER = "tls_certificates_folder_path";
  const std::string CIPHER_SUITE = "tls_cipher_suite_list";
  const std::string COMM_BUFF_LEN = "concord-bft_communication_buffer_length";
  const std::string INITIAL_RETRY_TIMEOUT = "client_initial_retry_timeout_milli";
  const std::string MIN_RETRY_TIMEOUT = "client_min_retry_timeout_milli";
  const std::string MAX_RETRY_TIMEOUT = "client_max_retry_timeout_milli";
  const std::string STANDARD_DEVIATIONS_TO_TOLERATE = "client_number_of_standard_deviations_to_tolerate";
  const std::string SAMPLES_PER_EVALUATION = "client_samples_per_evaluation";
  const std::string SAMPLES_UNTIL_RESET = "client_samples_until_reset";
  const std::string FIRST_THRESH = "client_sends_request_to_all_replicas_first_thresh";
  const std::string PERIODIC_THRESH = "client_sends_request_to_all_replicas_period_thresh";
  const std::string RESET_THRESH = "client_periodic_reset_thresh";
  const std::string NODE_VAR = "node";
  const std::string REPLICA_VAR = "replica";
  const std::string CLIENT_ID = "principal_id";
  const std::string REPLICA_HOST = "replica_host";
  const std::string REPLICA_PORT = "replica_port";
  const std::string EXTERNAL_CLIENTS = "external_clients";
  const std::string CLIENT = "client";
  const std::string CLIENT_PORT = "client_port";
  const std::string PROMETHEUS_PORT = "prometheus_port";
  const std::string PROMETHEUS_HOST = "participant_node_host";
  const std::string CLIENT_PROXIES_PER_REPLICA = "client_proxies_per_replica";
  const std::string ENABLE_MOCK_COMM = "enable_mock_comm";
  const std::string TRANSACTION_SIGNING_ENABLED = "transaction_signing_enabled";
  const std::string TRANSACTION_SIGNING_KEY_PATH = "signing_key_path";
  const std::string CLIENT_BATCHING_ENABLED = "client_batching_enabled";
  const std::string MULTIPLEX_CHANNEL_ENABLED = "enable_multiplex_channel";
  const std::string CLIENT_BATCHING_MAX_MSG_NUM = "client_batching_max_messages_nbr";
  const std::string CLIENT_BATCHING_TIMEOUT_MILLI = "client_batching_flush_timeout_ms";
  const std::string TRACE_SAMPLING_RATE = "trace_sampling_rate";
  ClientPoolConfig();

 private:
  // Logger
  logging::Logger logger_;
};

}  // namespace config_pool
}  // namespace concord
