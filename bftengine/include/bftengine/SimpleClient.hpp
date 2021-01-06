// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
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
#include <set>
#include <chrono>
#include <memory>

#include "Metrics.hpp"
#include "communication/ICommunication.hpp"

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
  EMPTY_CLIENT_REQ = 0x10
};

enum OperationResult : int8_t { SUCCESS, NOT_READY, TIMEOUT, BUFFER_TOO_SMALL };

class SimpleClient {
 public:
  explicit SimpleClient(uint16_t clientId)
      : metrics_{"clientMetrics_" + std::to_string(clientId), std::make_shared<concordMetrics::Aggregator>()},
        client_metrics_{{metrics_.RegisterCounter("retransmissions")},
                        {metrics_.RegisterGauge("retransmissionTimer", 0)}} {
    metrics_.Register();
  }
  static const uint64_t INFINITE_TIMEOUT = UINT64_MAX;

  static SimpleClient* createSimpleClient(bft::communication::ICommunication* communication,
                                          uint16_t clientId,
                                          uint16_t fVal,
                                          uint16_t cVal);

  static SimpleClient* createSimpleClient(bft::communication::ICommunication* communication,
                                          uint16_t clientId,
                                          uint16_t fVal,
                                          uint16_t cVal,
                                          SimpleClientParams p);

  virtual ~SimpleClient();

  virtual OperationResult sendRequest(uint8_t flags,
                                      const char* request,
                                      uint32_t lengthOfRequest,
                                      uint64_t reqSeqNum,
                                      uint64_t timeoutMilli,
                                      uint32_t lengthOfReplyBuffer,
                                      char* replyBuffer,
                                      uint32_t& actualReplyLength,
                                      const std::string& cid = "",
                                      const std::string& span_context = "") = 0;

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
};

// This class is mainly for testing and SimpleClient applications.
// Users are allowed to generate their own sequence numbers (they do notSUCCESS
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
