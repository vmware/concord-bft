// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
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
#include "client/concordclient/event_update_queue.hpp"
#include "Metrics.hpp"

namespace concord::client::concordclient {

struct SendError {
  std::string msg;
  enum ErrorType {
    ClientsBusy  // All clients are busy and cannot accept new requests
  };
  ErrorType type;
};
typedef std::variant<int, bft::client::Reply> SendResult;

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
  std::uint32_t external_requests_queue_size;
  bool encrypted_config_enabled;
  bool transaction_signing_enabled;
  bool with_cre;
  bool client_batching_enabled;
  size_t client_batching_max_messages_nbr;
  std::uint64_t client_batching_flush_timeout_ms;
};

struct BftClientInfo {
  // ID needs to match the values set in Concord's configuration
  bft::client::ClientId id;
  uint16_t port;
  std::string host;
};

struct TransportConfig {
  enum CommunicationType { Invalid, TlsTcp, PlainUdp };
  CommunicationType comm_type;
  // for testing purposes
  bool enable_mock_comm;
  // Communication buffer length
  uint32_t buffer_length;
  // TLS settings ignored if comm_type is not TlsTcp
  std::string tls_cert_root_path;
  std::string tls_cipher_suite;
  // Buffer with the servers' PEM encoded certififactes for the event port
  std::string event_pem_certs;
};

struct SubscribeConfig {
  // Subscription ID
  std::string id;
  // If set to false then all certificates related to subscription will be ignored
  bool use_tls;
  // TRC/S certificate path
  std::string trsc_tls_cert_path;
  // Buffer with the client's PEM encoded certififacte chain
  std::string pem_cert_chain;
  // Buffer with the client's PEM encoded private key
  std::string pem_private_key;
};

struct ConcordClientConfig {
  // Description of the replica network
  BftTopology topology;
  // Communication layer configuration
  TransportConfig transport;
  // BFT client descriptors
  std::vector<BftClientInfo> bft_clients;
  std::uint16_t num_of_used_bft_clients;
  // Configuration for subscribe requests
  SubscribeConfig subscribe_config;
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
            const std::unique_ptr<opentracing::Span>& parent_span,
            const std::function<void(SendResult&&)>& callback);
  void send(const bft::client::ReadConfig& config,
            bft::client::Msg&& msg,
            const std::unique_ptr<opentracing::Span>& parent_span,
            const std::function<void(SendResult&&)>& callback);

  // Subscribe to events which are pushed into the given update queue.
  void subscribe(const SubscribeRequest& request,
                 std::shared_ptr<UpdateQueue>& queue,
                 const std::unique_ptr<opentracing::Span>& parent_span);

  // Note, if the caller doesn't unsubscribe and no runtime error occurs then resources
  // will be occupied forever.
  void unsubscribe();

  // At the moment, we only allow one subscriber at a time. This exception is thrown if the caller subscribes while an
  // active subscription is in progress already.
  class SubscriptionExists : public std::runtime_error {
   public:
    SubscriptionExists() : std::runtime_error("subscription exists already"){};
  };

 private:
  config_pool::ConcordClientPoolConfig createClientPoolStruct(const ConcordClientConfig& config);

  // This method reads certificates from file if TLS is enabled
  void readCert(const std::string& input_filename, std::string& out_data);

  // This method gets the client_id from the OU field in the client certificate
  std::string getClientIdFromClientCert(const std::string& client_cert_path);

  // This method is used by getClientIdFromClientCert to get the client_id from
  // the subject in the client certificate
  std::string parseClientIdFromSubject(const std::string& subject_str);

  logging::Logger logger_;
  const ConcordClientConfig& config_;
  std::shared_ptr<concordMetrics::Aggregator> metrics_;

  // TODO: Allow multiple subscriptions
  std::atomic_bool active_subscription_{false};

  std::shared_ptr<BasicUpdateQueue> trc_queue_;
  std::unique_ptr<::client::thin_replica_client::ThinReplicaClient> trc_;
  std::unique_ptr<concord::concord_client_pool::ConcordClientPool> client_pool_;
};

}  // namespace concord::client::concordclient
