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
#include "Metrics.hpp"

namespace concord::client::concordclient {

struct SendError {
  std::string msg;
  enum ErrorType {
    ClientsBusy  // All clients are busy and cannot accept new requests
  };
  ErrorType type;
};
typedef std::variant<SendError, bft::client::Reply> SendResult;

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
};

struct BftClientInfo {
  // ID needs to match the values set in Concord's configuration
  bft::client::ClientId id;
};

struct TransportConfig {
  enum CommunicationType { Invalid, TlsTcp, PlainUdp };
  CommunicationType comm_type;

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

// TODO: With the execution engine interface in place, describe Events.
typedef std::vector<uint8_t> Event;
struct EventGroup {
  uint64_t id;
  std::vector<Event> events;
  std::chrono::microseconds record_time;
  // This map follows the W3C specification for trace context.
  // https://www.w3.org/TR/trace-context/#trace-context-http-headers-format
  std::map<std::string, std::string> trace_context;
};

// Legacy format of event groups visible within a single block.
struct LegacyEvent {
  uint64_t block_id;
  std::vector<std::pair<std::string, std::string>> events;
  std::string correlation_id;
  std::unordered_map<std::string, std::string> trace_context;
};

// TODO
struct SubscribeError {};
typedef std::variant<SubscribeError, EventGroup, LegacyEvent> SubscribeResult;

// ConcordClient combines two different client functionalities into one interface.
// On one side, the bft client to send/recieve request/response and on the other, the subscription API to
// observe events.
class ConcordClient {
 public:
  ConcordClient(const ConcordClientConfig& config);
  ~ConcordClient() { unsubscribe(); }

  void setMetricsAggregator(std::shared_ptr<concordMetrics::Aggregator> m) { metrics_ = m; }

  // Register a callback that gets invoked once the handling BFT client returns.
  void send(const bft::client::WriteConfig& config,
            bft::client::Msg&& msg,
            const std::unique_ptr<opentracing::Span>& parent_span,
            const std::function<void(SendResult&&)>& callback);
  void send(const bft::client::ReadConfig& config,
            bft::client::Msg&& msg,
            const std::unique_ptr<opentracing::Span>& parent_span,
            const std::function<void(SendResult&&)>& callback);

  // Register a callback that gets invoked for every validated event received.
  // void callback(SubscribeResult);
  // Return subscriber ID used to unsubscribe.
  void subscribe(const SubscribeRequest& request,
                 const std::unique_ptr<opentracing::Span>& parent_span,
                 const std::function<void(SubscribeResult&&)>& callback);

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
  logging::Logger logger_;
  const ConcordClientConfig& config_;
  std::shared_ptr<concordMetrics::Aggregator> metrics_;

  // TODO: Allow multiple subscriptions
  std::atomic_bool stop_subscriber_{true};
  std::unique_ptr<std::thread> subscriber_;

  std::shared_ptr<::client::thin_replica_client::BasicUpdateQueue> trc_queue_;
  std::unique_ptr<::client::thin_replica_client::ThinReplicaClient> trc_;
};

}  // namespace concord::client::concordclient
