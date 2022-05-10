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

#pragma once

#include <string>

// clang-format off
const auto kExampleConf = std::string(\
R"(client_batching_enabled: true
c_val: 0
client_batching_flush_timeout_ms: 100
client_batching_max_messages_nbr: 20
client_initial_retry_timeout_milli: 500
client_max_retry_timeout_milli: 3000
client_min_retry_timeout_milli: 500
client_number_of_standard_deviations_to_tolerate: 2
client_periodic_reset_thresh: 30
client_proxies_per_replica: 4
client_samples_per_evaluation: 32
client_samples_until_reset: 1000
client_sends_request_to_all_replicas_first_thresh: 2
client_sends_request_to_all_replicas_period_thresh: 2
clients_per_participant_node: 15
comm_to_use: tls
concord-bft_communication_buffer_length: 16777216
enable_mock_comm: false
encrypted_config_enabled: false
f_val: 1
num_replicas: 4
num_ro_replicas: 1
prometheus_port: 9873
secrets_url: ""
signing_key_path: /clientservice/bft_signing_keys
tls_1_3_comm_enabled: true
tls_certificates_folder_path: /clientservice/bft_certs
tls_cipher_suite_list: ECDHE-ECDSA-AES256-GCM-SHA384
tls_1_3_cipher_suite_list: TLS_AES_256_GCM_SHA384
transaction_signing_enabled: true
enable_multiplex_channel: false
use_unified_certificates: false
with_cre: false
node:
  - replica:
      - principal_id: 0
        replica_host: concord1
        replica_port: 3501
        event_port: 50051
        replica_host_uuid: concord1
  - replica:
      - principal_id: 1
        replica_host: concord2
        replica_port: 3501
        event_port: 50051
        replica_host_uuid: concord2
  - replica:
      - principal_id: 2
        replica_host: concord3
        replica_port: 3501
        event_port: 50051
        replica_host_uuid: concord3
  - replica:
      - principal_id: 3
        replica_host: concord4
        replica_port: 3501
        event_port: 50051
        replica_host_uuid: concord4
participant_nodes:
  - participant_node:
      - participant_node_host: 0.0.0.0
        clientservice_host_uuid: clientservice1
        principal_id: 55
        external_clients:
          - client:
              - client_port: 3502
                principal_id: 21
          - client:
              - client_port: 3503
                principal_id: 22
          - client:
              - client_port: 3504
                principal_id: 23
          - client:
              - client_port: 3505
                principal_id: 24
          - client:
              - client_port: 3506
                principal_id: 25
          - client:
              - client_port: 3507
                principal_id: 26
          - client:
              - client_port: 3508
                principal_id: 27
          - client:
              - client_port: 3509
                principal_id: 28
          - client:
              - client_port: 3510
                principal_id: 29
          - client:
              - client_port: 3511
                principal_id: 30
          - client:
              - client_port: 3512
                principal_id: 31
          - client:
              - client_port: 3513
                principal_id: 32
          - client:
              - client_port: 3514
                principal_id: 33
          - client:
              - client_port: 3515
                principal_id: 34
          - client:
              - client_port: 3516
                principal_id: 35)");
// clang-format on
