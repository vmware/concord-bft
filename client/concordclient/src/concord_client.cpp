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

#include <chrono>
#include <thread>

#include "assertUtils.hpp"
#include "client/concordclient/concord_client.hpp"
#include "client/thin-replica-client/thin_replica_client.hpp"
#include "client/thin-replica-client/replica_state_snapshot_client.hpp"

using ::client::thin_replica_client::ThinReplicaClient;
using ::client::thin_replica_client::ThinReplicaClientConfig;
using ::client::replica_state_snapshot_client::ReplicaStateSnapshotClient;
using ::client::replica_state_snapshot_client::ReplicaStateSnapshotClientConfig;
using ::client::concordclient::GrpcConnection;
using concord::config_pool::ConcordClientPoolConfig;
using concord::config_pool::ExternalClient;
using concord::config_pool::ParticipantNode;
using ::client::concordclient::GrpcConnectionConfig;
using bft::communication::NodeMap;
using bft::communication::NodeInfo;

namespace concord::client::concordclient {

ConcordClient::ConcordClient(const ConcordClientConfig& config, std::shared_ptr<concordMetrics::Aggregator> aggregator)
    : logger_(logging::getLogger("concord.client.concordclient")), config_(config), metrics_(aggregator) {
  ConcordClientPoolConfig client_pool_config = createClientPoolStruct(config);
  client_pool_ = std::make_unique<concord::concord_client_pool::ConcordClientPool>(client_pool_config, metrics_);
  while (client_pool_->HealthStatus() == concord::concord_client_pool::PoolStatus::NotServing) {
    LOG_INFO(logger_, "Waiting for client pool to connect");
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }
  createGrpcConnections();
}

ConcordClientPoolConfig ConcordClient::createClientPoolStruct(const ConcordClientConfig& config) {
  ConcordClientPoolConfig client_pool_config;
  client_pool_config.path_to_replicas_master_key = config.topology.path_to_replicas_master_key;
  NodeMap& replicas = client_pool_config.replicas;
  for (const auto& replica : config_.topology.replicas) {
    NodeInfo replica_info;
    replica_info.host = replica.host;
    replica_info.port = replica.bft_port;
    replica_info.isReplica = true;
    replicas[replica.id.val] = replica_info;
  }

  ParticipantNode client_pool_pn;
  client_pool_pn.participant_node_host = config_.client_service.host;
  client_pool_pn.principal_id = config_.client_service.id.val;
  int id = 0;
  for (const auto& bft_client : config_.bft_clients) {
    ExternalClient client_pool_ec;
    auto external_client_id = bft_client.id.val;
    client_pool_ec.principal_id = external_client_id;
    client_pool_ec.client_port = bft_client.port;
    client_pool_pn.externalClients[id] = client_pool_ec;
    id++;
  }

  client_pool_config.participant_nodes.push_back(client_pool_pn);
  client_pool_config.clients_per_participant_node = config_.clients_per_participant_node;
  client_pool_config.active_clients_in_pool = config_.active_clients_in_pool;

  client_pool_config.f_val = config.topology.f_val;
  client_pool_config.c_val = config.topology.c_val;
  client_pool_config.client_initial_retry_timeout_milli =
      config.topology.client_retry_config.initial_retry_timeout.count();
  client_pool_config.client_min_retry_timeout_milli = config.topology.client_retry_config.min_retry_timeout.count();
  client_pool_config.client_max_retry_timeout_milli = config.topology.client_retry_config.max_retry_timeout.count();
  client_pool_config.client_number_of_standard_deviations_to_tolerate =
      config.topology.client_retry_config.number_of_standard_deviations_to_tolerate;
  client_pool_config.client_samples_per_evaluation = config.topology.client_retry_config.samples_per_evaluation;
  client_pool_config.client_samples_until_reset = config.topology.client_retry_config.samples_until_reset;
  client_pool_config.client_sends_request_to_all_replicas_first_thresh =
      config.topology.client_sends_request_to_all_replicas_first_thresh;
  client_pool_config.client_sends_request_to_all_replicas_period_thresh =
      config.topology.client_sends_request_to_all_replicas_period_thresh;
  client_pool_config.num_replicas = config.topology.replicas.size();
  client_pool_config.client_proxies_per_replica = config.topology.client_proxies_per_replica;
  client_pool_config.signing_key_path = config.topology.signing_key_path;
  client_pool_config.encrypted_config_enabled = config.topology.encrypted_config_enabled;
  client_pool_config.transaction_signing_enabled = config.topology.transaction_signing_enabled;
  client_pool_config.with_cre = config.topology.with_cre;
  client_pool_config.client_batching_enabled = config.topology.client_batching_enabled;
  client_pool_config.client_batching_max_messages_nbr = config.topology.client_batching_max_messages_nbr;
  client_pool_config.client_batching_flush_timeout_ms = config.topology.client_batching_flush_timeout_ms;

  client_pool_config.comm_to_use = config.transport.comm_type == TransportConfig::Invalid
                                       ? "Invalid"
                                       : config.transport.comm_type == TransportConfig::TlsTcp ? "tls" : "udp";
  client_pool_config.enable_multiplex_channel = config.transport.enable_multiplex_channel;
  client_pool_config.use_unified_certificates = config.transport.use_unified_certs;
  client_pool_config.concord_bft_communication_buffer_length = std::to_string(config.transport.buffer_length);
  client_pool_config.tls_certificates_folder_path = config.transport.tls_cert_root_path;
  client_pool_config.tls_cipher_suite_list = config.transport.tls_cipher_suite;
  client_pool_config.enable_mock_comm = config.transport.enable_mock_comm;
  if (config.transport.secret_data.has_value()) {
    client_pool_config.secret_data = config.transport.secret_data.value();
  }

  return client_pool_config;
}

void ConcordClient::send(const bft::client::ReadConfig& config,
                         bft::client::Msg&& msg,
                         const std::function<void(SendResult&&)>& callback) {
  LOG_INFO(logger_, "Log message until config is used f=" << config_.topology.f_val);
  client_pool_->SendRequest(config, std::forward<bft::client::Msg>(msg), callback);
}

void ConcordClient::send(const bft::client::WriteConfig& config,
                         bft::client::Msg&& msg,
                         const std::function<void(SendResult&&)>& callback) {
  client_pool_->SendRequest(config, std::forward<bft::client::Msg>(msg), callback);
}

void ConcordClient::createGrpcConnections() {
  for (const auto& replica : config_.topology.replicas) {
    auto addr = replica.host + ":" + std::to_string(replica.event_port);
    auto grpc_conn = std::make_shared<GrpcConnection>(
        addr, config_.subscribe_config.id, /* TODO */ 3, /* TODO */ 3, config_.state_snapshot_config.timeout_in_sec);

    LOG_INFO(logger_,
             "Create Grpc Connection" << KVLOG(config_.subscribe_config.use_tls, addr, config_.subscribe_config.id));
    // TODO: Adapt TRC API to support PEM buffers
    auto trsc_config = std::make_unique<GrpcConnectionConfig>(config_.subscribe_config.use_tls,
                                                              config_.subscribe_config.pem_private_key,
                                                              config_.subscribe_config.pem_cert_chain,
                                                              config_.transport.event_pem_certs);

    grpc_conn->connect(trsc_config);
    grpc_connections_.push_back(std::move(grpc_conn));
  }
}

void ConcordClient::checkAndReConnectGrpcConnections() {
  bool need_reconnection = false;
  for (size_t con_offset = 0; con_offset < config_.topology.replicas.size(); ++con_offset) {
    if (!(grpc_connections_[con_offset])->isConnected()) {
      need_reconnection = true;
      break;
    }
  }
  if (need_reconnection) {
    for (size_t con_offset = 0; con_offset < config_.topology.replicas.size(); ++con_offset) {
      auto trsc_config = std::make_unique<GrpcConnectionConfig>(config_.subscribe_config.use_tls,
                                                                config_.subscribe_config.pem_private_key,
                                                                config_.subscribe_config.pem_cert_chain,
                                                                config_.transport.event_pem_certs);
      (grpc_connections_[con_offset])->checkAndReConnect(trsc_config);
    }
  }
}

void ConcordClient::subscribe(const SubscribeRequest& sub_req,
                              std::shared_ptr<EventUpdateQueue>& queue,
                              const std::unique_ptr<opentracing::Span>& parent_span) {
  bool expected = false;
  if (!active_subscription_.compare_exchange_weak(expected, true)) {
    LOG_ERROR(logger_, "subscription already in progress - unsubscribe first");
    throw SubscriptionExists();
  }

  checkAndReConnectGrpcConnections();

  auto trc_config = std::make_unique<ThinReplicaClientConfig>(
      config_.subscribe_config.id, queue, config_.topology.f_val, grpc_connections_);
  trc_ = std::make_unique<ThinReplicaClient>(std::move(trc_config), metrics_);

  if (std::holds_alternative<EventGroupRequest>(sub_req.request)) {
    ::client::thin_replica_client::SubscribeRequest trc_request;
    trc_request.event_group_id = std::get<EventGroupRequest>(sub_req.request).event_group_id;
    trc_->Subscribe(trc_request);
  } else if (std::holds_alternative<LegacyEventRequest>(sub_req.request)) {
    trc_->Subscribe(std::get<LegacyEventRequest>(sub_req.request).block_id);
  } else {
    ConcordAssert(false);
  }
}

void ConcordClient::unsubscribe() {
  if (active_subscription_) {
    LOG_INFO(logger_, "Closing subscription. Waiting for subscriber to finish.");
    trc_->Unsubscribe();
    trc_.reset();
    active_subscription_ = false;
    LOG_INFO(logger_, "Subscriber finished.");
  }
}

void ConcordClient::getSnapshot(const StateSnapshotRequest& request, std::shared_ptr<SnapshotQueue>& remote_queue) {
  LOG_INFO(logger_, "getSnapshot called.");
  checkAndReConnectGrpcConnections();
  if (!rss_) {
    // Lazy initialization, when required for the first time.
    auto rss_config = std::make_unique<ReplicaStateSnapshotClientConfig>(grpc_connections_,
                                                                         config_.state_snapshot_config.num_threads);
    rss_ = std::make_unique<ReplicaStateSnapshotClient>(std::move(rss_config));
  }

  ::client::replica_state_snapshot_client::SnapshotRequest rss_request;
  rss_request.snapshot_id = request.snapshot_id;
  if (request.last_received_key.has_value()) {
    rss_request.last_received_key = request.last_received_key.value();
  }
  rss_->readSnapshotStream(rss_request, remote_queue);
}

}  // namespace concord::client::concordclient
