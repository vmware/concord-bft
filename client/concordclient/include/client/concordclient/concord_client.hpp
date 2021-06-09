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

#include <string>
#include <variant>
#include <vector>

#include "bftclient/base_types.h"
#include "bftclient/bft_client.h"

#include "client/thin-replica-client/thin_replica_client.hpp"

namespace client::concordclient {

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
  uint16_t trs_port;
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
};

struct SubscribeServer {
  // If set to false then the fields below won't be evaluated
  const bool use_tls;
  // Buffer with the server's PEM encoded certififactes
  const std::string pem_certs;
};

struct SubscribeConfig {
  // Subscription ID
  const std::string id;
  // Buffer with the client's PEM encoded certififacte chain
  const std::string pem_cert_chain;
  // Buffer with the client's PEM encoded private key
  const std::string pem_private_key;
  // List of SubscribeServer endpoints
  std::vector<SubscribeServer> servers;
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

// ConcordClient combines two different client functionalities into one interface.
// On one side, the bft client to send/recieve request/response and on the other, the thin replica subscription to
// observe events.
class ConcordClient {
 public:
  ConcordClient(const ConcordClientConfig& config) : config_(config) {}

  // Register a callback that gets invoked once the handling BFT client returns
  // void callback(SendResult result);
  template <class CallbackT>
  void send(const bft::client::WriteConfig& config,
            bft::client::Msg&& msg,
            const std::unique_ptr<opentracing::Span>& parent_span,
            CallbackT callback);
  template <class CallbackT>
  void send(const bft::client::ReadConfig& config,
            bft::client::Msg&& msg,
            const std::unique_ptr<opentracing::Span>& parent_span,
            CallbackT callback);

  // Register a callback that gets invoked for every validated event received.
  // void callback(client::thin_replica_client::SubscribeResult result);
  template <class CallbackT>
  void subscribe(const client::thin_replica_client::SubscribeRequest& request,
                 const std::unique_ptr<opentracing::Span>& parent_span,
                 CallbackT callback);
  void unsubscribe();

 private:
  ConcordClientConfig config_;
};

}  // namespace client::concordclient
