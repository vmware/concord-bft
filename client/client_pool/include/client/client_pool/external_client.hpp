// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
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
// Call back for request - at this point we know for sure that a client is handling the request so we can assure that we
// will have reply. This callback will be attached to the client reply struct and whenever we get the reply fro, the bft
// client we will activate the callback.
typedef std::function<void(bft::client::Reply&&)> RequestCallBack;

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
  ConcordClient(int client_id,
                config_pool::ConcordClientPoolConfig& struct_config,
                const bftEngine::SimpleClientParams& client_params);

  // Destructs the client. This includes stopping any internal threads, if
  // needed.
  ~ConcordClient() noexcept;

  bft::client::Reply SendRequest(const bft::client::WriteConfig& config, bft::client::Msg&& request);

  bft::client::Reply SendRequest(const bft::client::ReadConfig& config, bft::client::Msg&& request);

  void AddPendingRequest(std::vector<uint8_t>&& request,
                         bftEngine::ClientMsgFlag flags,
                         char* reply_buffer,
                         std::chrono::milliseconds timeout_ms,
                         std::uint32_t reply_size,
                         uint64_t seq_num,
                         const RequestCallBack& callback,
                         const std::string& correlation_id = {},
                         const std::string& span_context = {});

  size_t PendingRequestsCount() const { return pending_requests_.size(); }

  std::pair<int8_t, PendingReplies> SendPendingRequests();

  int getClientId() const;

  uint64_t generateClientSeqNum();

  void setStartRequestTime();

  std::chrono::steady_clock::time_point getStartRequestTime() const;

  void setStartWaitingTime();

  std::chrono::steady_clock::time_point getWaitingTime() const;

  bool isServing() const;

  void stopClientComm();

  static std::unique_ptr<bft::communication::ICommunication> ToCommunication(
      const bft::communication::BaseCommConfig& comm_config);

  static void setStatics(uint16_t required_num_of_replicas,
                         uint16_t num_of_replicas,
                         uint32_t max_reply_size,
                         size_t batch_size);
  static void setDelayFlagForTest(bool delay);
  static bftEngine::OperationResult getClientRequestError();
  ConcordClient(ConcordClient&& t) = delete;

  // this buffer is fully owned by external application who may set it
  // via the setExternalReplyBuffer() function.
  // this memory is used when the callback is called and probably
  // will be used ONLY in conjunction with external callback.
  // the better solution is to pass it via the Send function but due to the time
  // constraints we are not changing the interface now.
  void setReplyBuffer(char* buf, uint32_t size);
  void unsetReplyBuffer();

 private:
  void CreateClient(concord::config_pool::ConcordClientPoolConfig&, const bftEngine::SimpleClientParams& client_params);

  bft::communication::BaseCommConfig* CreateCommConfig(int num_replicas,
                                                       const config_pool::ConcordClientPoolConfig&) const;

  std::unique_ptr<bft::communication::ICommunication> comm_;
  std::unique_ptr<bft::client::Client> new_client_;

  // Logger
  logging::Logger logger_;
  int client_id_;
  bool enable_mock_comm_ = false;
  std::unique_ptr<bftEngine::SeqNumberGeneratorForClientRequests> seqGen_;
  std::chrono::steady_clock::time_point start_job_time_ = std::chrono::steady_clock::now();
  std::chrono::steady_clock::time_point waiting_job_time_ = std::chrono::steady_clock::now();
  static uint16_t num_of_replicas_;
  static uint16_t required_num_of_replicas_;
  static size_t max_reply_size_;
  // A shared memory for all clients to return reply because for now the reply
  // is not important
  static std::shared_ptr<std::vector<char>> reply_;

  // this buffer is fully owned by external application who may set it
  // via the setExternalReplyBuffer() function.
  // this memory is used when the callback is called and probably
  // will be used ONLY in conjunction with external callback.
  // the better solution is to pass it via the Send function but due to the time
  // constraints we are not changing the interface now.
  char* externalReplyBuffer = nullptr;
  uint32_t externalReplyBufferSize = 0;
  using PendingRequests = std::deque<bftEngine::ClientRequest>;
  PendingRequests pending_requests_;
  PendingReplies pending_replies_;
  size_t batching_buffer_reply_offset_ = 0UL;
  static bool delayed_behaviour_;
  static bftEngine::OperationResult clientRequestError_;
};

}  // namespace external_client
}  // namespace concord
