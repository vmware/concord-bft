// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <queue>
#include <chrono>
#include <memory>

#include "Metrics.hpp"
#include "communication/ICommunication.hpp"
#include "PerformanceManager.hpp"
#include "SharedTypes.hpp"
#include "../../../bftclient/include/bftclient/base_types.h"

namespace bftEngine {
struct SimpleClientParams {
  uint16_t clientInitialRetryTimeoutMilli = 150;
  uint16_t clientMinRetryTimeoutMilli = 50;
  uint16_t clientMaxRetryTimeoutMilli = 1000;
  uint16_t numberOfStandardDeviationsToTolerate = 2;
  uint16_t samplesPerEvaluation = 32;
  uint16_t samplesUntilReset = 1000;
  uint16_t clientSendsRequestToAllReplicasFirstThresh = 2;
  uint16_t clientSendsRequestToAllReplicasPeriodThresh = 2;
  uint16_t clientPeriodicResetThresh = 30;
};

// Possible values for 'flags' parameter
enum ClientMsgFlag : uint8_t {
  EMPTY_FLAGS_REQ = 0x0,
  READ_ONLY_REQ = 0x1,
  PRE_PROCESS_REQ = 0x2,
  HAS_PRE_PROCESSED_REQ = 0x4,
  KEY_EXCHANGE_REQ = 0x8,
  EMPTY_CLIENT_REQ = 0x10,
};

struct ClientRequest {
  uint8_t flags = 0;
  std::vector<uint8_t> request;
  uint32_t lengthOfRequest = 0;
  uint64_t reqSeqNum = 0;
  uint64_t timeoutMilli = 0;
  std::string cid;
  std::string span_context;
};

struct ClientReply {
  uint32_t lengthOfReplyBuffer = 0;
  char* replyBuffer = nullptr;
  uint32_t actualReplyLength = 0;
  bftEngine::OperationResult opResult = bftEngine::OperationResult::SUCCESS;
  std::string cid;
  std::string span_context;
};

class SimpleClient {
 public:
  explicit SimpleClient(uint16_t clientId,
                        const std::shared_ptr<concord::performance::PerformanceManager>& pm =
                            std::make_shared<concord::performance::PerformanceManager>())
      : metrics_{"clientMetrics_" + std::to_string(clientId), std::make_shared<concordMetrics::Aggregator>()},
        client_metrics_{{metrics_.RegisterCounter("retransmissions")},
                        {metrics_.RegisterGauge("retransmissionTimer", 0)}},
        pm_{pm} {
    metrics_.Register();
  }
  static const uint64_t INFINITE_TIMEOUT = UINT64_MAX;

  static SimpleClient* createSimpleClient(bft::communication::ICommunication* communication,
                                          uint16_t clientId,
                                          uint16_t fVal,
                                          uint16_t cVal,
                                          const std::shared_ptr<concord::performance::PerformanceManager>& pm =
                                              std::make_shared<concord::performance::PerformanceManager>());

  static SimpleClient* createSimpleClient(bft::communication::ICommunication* communication,
                                          uint16_t clientId,
                                          uint16_t fVal,
                                          uint16_t cVal,
                                          SimpleClientParams p,
                                          const std::shared_ptr<concord::performance::PerformanceManager>& pm =
                                              std::make_shared<concord::performance::PerformanceManager>());

  virtual ~SimpleClient();

  virtual bftEngine::OperationResult sendRequest(uint8_t flags,
                                                 const char* request,
                                                 uint32_t lengthOfRequest,
                                                 uint64_t reqSeqNum,
                                                 uint64_t timeoutMilli,
                                                 uint32_t lengthOfReplyBuffer,
                                                 char* replyBuffer,
                                                 uint32_t& actualReplyLength,
                                                 const std::string& cid = "",
                                                 const std::string& spanContext = "") = 0;

  // To be used only for write requests
  virtual bftEngine::OperationResult sendBatch(const std::deque<ClientRequest>& clientRequests,
                                               std::deque<ClientReply>& clientReplies,
                                               const std::string& batchCid) = 0;

  void setAggregator(const std::shared_ptr<concordMetrics::Aggregator>& aggregator) {
    if (aggregator) metrics_.SetAggregator(aggregator);
  }

 protected:
  /* Client Metrics */
  concordMetrics::Component metrics_;
  struct ClientMetrics {
    concordMetrics::CounterHandle retransmissions;
    concordMetrics::GaugeHandle retransmissionTimer;
  } client_metrics_;
  std::shared_ptr<concord::performance::PerformanceManager> pm_ = nullptr;
};

// This class is mainly for testing and SimpleClient applications.
// Users are allowed to generate their own sequence numbers (they do not
// have to use this class). Other examples of ways to do this include:
// (1) A simple counter + store the last counter in a persistent storage
// (2) An approach that utilizes the functions
//     SimpleClient::sendRequestToResetSeqNum() or
//     SimpleClient::sendRequestToReadLatestSeqNum(..)
//     [These functions are not yet supported, but if necessary, we will
//     support them.]
class SeqNumberGeneratorForClientRequests {
 public:
  static std::unique_ptr<SeqNumberGeneratorForClientRequests> createSeqNumberGeneratorForClientRequests();

  virtual uint64_t generateUniqueSequenceNumberForRequest() = 0;

  virtual uint64_t generateUniqueSequenceNumberForRequest(std::chrono::time_point<std::chrono::system_clock> now) = 0;

  virtual ~SeqNumberGeneratorForClientRequests() = default;
};

}  // namespace bftEngine
