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

#pragma once

#include <bftengine/SimpleClient.hpp>
#include <chrono>
#include <cstdint>
#include <utility>
#include <vector>
#include "client_pool_config.hpp"
#include "communication/StatusInfo.h"
#include "external_client_exception.hpp"

namespace concord {

namespace config {
class ConcordConfiguration;
}

namespace external_client {
// Represents a Concord BFT client. The purpose of this class is to be easy to
// use for external users. This is achieved by:
//  * providing a simple public interface
//  * providing a generic public interface that allows for various use cases
//  * configuration via a file - users don't need to know what the structure of
//  the file is and changes to the file will not affect the interface of the
//  client
class ConcordClient {
 public:
  using PendingReplies = std::deque<bftEngine::ClientReply>;
  // Constructs the client by passing concord configuration
  // object and a client_id to get the specific values for this client.
  // Construction executes all needed steps to provide a ready-to-use
  // object (including starting internal threads, if needed).
  ConcordClient(int client_id, std::shared_ptr<concordMetrics::Aggregator> aggregator);

  // Destructs the client. This includes stopping any internal threads, if needed.
  ~ConcordClient() noexcept;

  bft::client::Reply SendRequest(const bft::client::WriteConfig& config, bft::client::Msg&& request);

  bft::client::Reply SendRequest(const bft::client::ReadConfig& config, bft::client::Msg&& request);

  void AddPendingRequest(std::vector<uint8_t>&& request,
                         bftEngine::ClientMsgFlag flags,
                         char* reply_buffer,
                         std::chrono::milliseconds timeout_ms,
                         std::uint32_t reply_size,
                         uint64_t seq_num,
                         const std::string& correlation_id = {},
                         const std::string& span_context = {},
                         bftEngine::RequestCallBack callback = {});

  size_t PendingRequestsCount() const { return pending_requests_.size(); }

  std::pair<int32_t, PendingReplies> SendPendingRequests();

  int getClientId() const;

  uint64_t generateClientSeqNum();

  void setStartRequestTime();

  std::chrono::steady_clock::time_point getStartRequestTime() const;

  std::chrono::steady_clock::time_point getAndDeleteCidBeforeSendTime(const std::string& cid);

  std::chrono::steady_clock::time_point getAndDeleteCidResponseTime(const std::string& cid);

  void setStartWaitingTime();

  std::chrono::steady_clock::time_point getWaitingTime() const;

  bool isServing() const;

  void stopClientComm();

  bftEngine::OperationResult getRequestExecutionResult();

  ConcordClient(ConcordClient&& t) = delete;

  static bft::client::SharedCommPtr ToCommunication(const bft::communication::BaseCommConfig& comm_config);

  static void setStatics(uint16_t required_num_of_replicas,
                         uint16_t num_of_replicas,
                         uint32_t max_reply_size,
                         size_t batch_size,
                         config_pool::ConcordClientPoolConfig& pool_config,
                         bftEngine::SimpleClientParams& client_params,
                         bft::communication::BaseCommConfig* multiplexConfig);

  static void setDelayFlagForTest(bool delay);

  std::string messageSignature(bft::client::Msg&);

 private:
  void CreateClient(std::shared_ptr<concordMetrics::Aggregator> aggregator);

  bft::communication::BaseCommConfig* CreateCommConfig() const;

  void CreateClientConfig(bft::communication::BaseCommConfig* comm_config, bft::client::ClientConfig& cfg);

  std::tuple<bft::communication::BaseCommConfig*, bft::client::SharedCommPtr> CreateCommConfigAndCommChannel();

 private:
  static uint16_t num_of_replicas_;
  static uint16_t required_num_of_replicas_;
  static size_t max_reply_size_;
  // A shared memory for all clients to return reply because for now the reply is not important
  static std::shared_ptr<std::vector<char>> reply_;
  static bool delayed_behaviour_;
  static std::set<bft::client::ReplicaId> all_replicas_;
  static config_pool::ConcordClientPoolConfig pool_config_;
  static bftEngine::SimpleClientParams client_params_;
  static bft::communication::BaseCommConfig* multiplexConfig_;
  static bft::client::SharedCommPtr multiplex_comm_channel_;

  std::unique_ptr<bftEngine::SeqNumberGeneratorForClientRequests> seqGen_;
  std::chrono::steady_clock::time_point start_job_time_ = std::chrono::steady_clock::now();
  std::chrono::steady_clock::time_point waiting_job_time_ = std::chrono::steady_clock::now();
  std::unique_ptr<bft::client::Client> new_client_;
  logging::Logger logger_;
  int client_id_;
  bool enable_mock_comm_ = false;
  using PendingRequests = std::deque<bftEngine::ClientRequest>;
  PendingRequests pending_requests_;
  PendingReplies pending_replies_;
  size_t batching_buffer_reply_offset_ = 0UL;
  bftEngine::OperationResult clientRequestExecutionResult_;
  std::unordered_map<std::string, std::chrono::steady_clock::time_point> cid_before_send_map_;
  std::unordered_map<std::string, std::chrono::steady_clock::time_point> cid_response_map_;
};

}  // namespace external_client
}  // namespace concord
