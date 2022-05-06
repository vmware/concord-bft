// Concord
//
// Copyright (c) 2021-2022 VMware, Inc. All Rights Reserved.
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

#include <chrono>
#include <functional>
#include <opentracing/span.h>
#include <memory>
#include <string>
#include <thread>
#include <variant>
#include <vector>

#include "bftclient/base_types.h"
#include "bftclient/bft_client.h"
#include "client/thin-replica-client/thin_replica_client.hpp"
#include "client/client_pool/concord_client_pool.hpp"
#include "client/concordclient/event_update.hpp"
#include "client/concordclient/snapshot_update.hpp"
#include "client/concordclient/concord_client_exceptions.hpp"
#include "Metrics.hpp"
#include "client/thin-replica-client/replica_state_snapshot_client.hpp"

namespace concord::client::concordclient {

struct SendError {
  std::string msg;
  enum ErrorType {
    ClientsBusy  // All clients are busy and cannot accept new requests
  };
  ErrorType type;
};

typedef std::variant<uint32_t, bft::client::Reply> SendResult;

struct ReplicaInfo {
  bft::client::ReplicaId id;
  std::string host;
  uint16_t bft_port;
  uint16_t event_port;
};

struct BftTopology {
  uint16_t f_val;
  uint16_t c_val;
  std::vector<ReplicaInfo> replicas;
  bft::client::RetryTimeoutConfig client_retry_config;
  std::uint16_t client_sends_request_to_all_replicas_first_thresh;
  std::uint16_t client_sends_request_to_all_replicas_period_thresh;
  std::uint32_t client_proxies_per_replica;
  std::string signing_key_path;
  bool encrypted_config_enabled;
  bool transaction_signing_enabled;
  bool with_cre;
  bool client_batching_enabled;
  size_t client_batching_max_messages_nbr;
  std::uint64_t client_batching_flush_timeout_ms;
  std::string path_to_replicas_master_key = std::string();
};

struct ClientServiceInfo {
  bft::client::ClientId id;
  std::string host;
  std::string host_uuid;
};

struct BftClientInfo {
  // ID needs to match the values set in Concord's configuration
  bft::client::ClientId id;
  uint16_t port;
};

struct TransportConfig {
  enum CommunicationType { Invalid, TlsTcp, PlainUdp };
  CommunicationType comm_type;
  bool enable_multiplex_channel;
  // for testing purposes
  bool enable_mock_comm;
  // Communication buffer length
  uint32_t buffer_length;
  // TLS settings ignored if comm_type is not TlsTcp
  std::string tls_cert_root_path;
  std::string tls_cipher_suite;
  bool use_unified_certs;
  std::optional<concord::secretsmanager::SecretData> secret_data;
  // Buffer with the servers' PEM encoded certificates for the event port
  std::string event_pem_certs;
};

struct SubscribeConfig {
  // Subscription ID
  std::string id;
  // If set to false then all certificates related to subscription will be ignored
  bool use_tls;
  // Buffer with the client's PEM encoded certififacte chain
  std::string pem_cert_chain;
  // Buffer with the client's PEM encoded private key
  std::string pem_private_key;
};

struct StateSnapshotConfig {
  // Number of threads available for state snapshot
  uint32_t num_threads;
  // Timeout till grpc connection with replicas will wait.
  uint16_t timeout_in_sec;
};

struct ConcordClientConfig {
  // Description of the replica network
  BftTopology topology;
  // Communication layer configuration
  TransportConfig transport;
  // BFT client descriptors
  ClientServiceInfo client_service;
  std::vector<BftClientInfo> bft_clients;
  std::uint16_t clients_per_participant_node;
  std::uint16_t active_clients_in_pool;
  // Configuration for subscribe requests
  SubscribeConfig subscribe_config;
  StateSnapshotConfig state_snapshot_config;
};

struct StateSnapshotRequest {
  uint64_t snapshot_id;
  std::optional<std::string> last_received_key;
};

struct EventGroupRequest {
  uint64_t event_group_id;
};
struct LegacyEventRequest {
  uint64_t block_id;
};
struct SubscribeRequest {
  // Depending on the type, the subscription will either return Events or EventGroups
  // Use EventGroups, BlockIds are deprecated.
  std::variant<EventGroupRequest, LegacyEventRequest> request;
};

// ConcordClient combines two different client functionalities into one interface.
// On one side, the bft client to send/recieve request/response and on the other, the subscription API to
// observe events.
class ConcordClient {
 public:
  ConcordClient(const ConcordClientConfig&, std::shared_ptr<concordMetrics::Aggregator>);
  ~ConcordClient() { unsubscribe(); }

  // Register a callback that gets invoked once the handling BFT client returns.
  void send(const bft::client::WriteConfig& config,
            bft::client::Msg&& msg,
            const std::function<void(SendResult&&)>& callback);
  void send(const bft::client::ReadConfig& config,
            bft::client::Msg&& msg,
            const std::function<void(SendResult&&)>& callback);

  // Subscribe to events which are pushed into the given update queue.
  void subscribe(const SubscribeRequest& request,
                 std::shared_ptr<EventUpdateQueue>& queue,
                 const std::unique_ptr<opentracing::Span>& parent_span);

  // Note, if the caller doesn't unsubscribe and no runtime error occurs then resources
  // will be occupied forever.
  void unsubscribe();

  // Stream a specific state snapshot in a resumable fashion as a finite stream of key-values.
  // Key-values are streamed with lexicographic order on keys.
  void getSnapshot(const StateSnapshotRequest& request, std::shared_ptr<SnapshotQueue>& remote_queue);

  // Get subscription id.
  std::string getSubscriptionId() const { return config_.subscribe_config.id; }

 private:
  config_pool::ConcordClientPoolConfig createClientPoolStruct(const ConcordClientConfig& config);
  void createGrpcConnections();
  void checkAndReConnectGrpcConnections();

  logging::Logger logger_;
  const ConcordClientConfig& config_;
  std::shared_ptr<concordMetrics::Aggregator> metrics_;

  // TODO: Allow multiple subscriptions
  std::atomic_bool active_subscription_{false};

  std::vector<std::shared_ptr<::client::concordclient::GrpcConnection>> grpc_connections_;
  std::unique_ptr<::client::thin_replica_client::ThinReplicaClient> trc_;
  std::unique_ptr<::client::replica_state_snapshot_client::ReplicaStateSnapshotClient> rss_;
  std::unique_ptr<concord::concord_client_pool::ConcordClientPool> client_pool_;
};

}  // namespace concord::client::concordclient
