// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <cstddef>
#include <memory>
#include <cstdint>
#include <string>

#include "IStateTransfer.hpp"
#include "OpenTracing.hpp"
#include "communication/ICommunication.hpp"
#include "MetadataStorage.hpp"
#include "Metrics.hpp"
#include "ReplicaConfig.hpp"
#include "PerformanceManager.hpp"

namespace concord::reconfiguration {
class IReconfigurationHandler;
class IPruningHandler;
}  // namespace concord::reconfiguration

namespace bftEngine {
// Possible values for 'flags' parameter
enum MsgFlag : uint8_t {
  EMPTY_FLAGS = 0x0,
  READ_ONLY_FLAG = 0x1,
  PRE_PROCESS_FLAG = 0x2,
  HAS_PRE_PROCESSED_FLAG = 0x4,
  KEY_EXCHANGE_FLAG = 0x8,  // TODO [TK] use reconfig_flag
  EMPTY_CLIENT_FLAG = 0x10,
  RECONFIG_FLAG = 0x20
};

// The IControlHandler is a group of methods that enables the userRequestHandler to perform infrastructure
// changes in the system.
// For example, assuming we want to upgrade the system to new software version, then:
// 1. We need to bring the system to a stable state (bft responsibility)
// 2. We need to perform the actual upgrade process (the platform responsibility)
// Thus, once the bft brings the system to the desired stable state, it needs to invoke the a callback of the user to
// perform the actual upgrade.
// More possible scenarios would be:
// 1. Adding/removing node
// 2. Key exchange
// 3. Change DB scheme
// and basically any management action that is handled by the layer that uses concord-bft.
class IControlHandler {
 public:
  static const std::shared_ptr<IControlHandler> instance(IControlHandler *ch = nullptr) {
    static const std::shared_ptr<IControlHandler> ch_(ch);
    return ch_;
  }
  virtual void onSuperStableCheckpoint() = 0;
  virtual void onStableCheckpoint() = 0;
  virtual bool onPruningProcess() = 0;
  virtual bool isOnNOutOfNCheckpoint() const = 0;
  virtual bool isOnStableCheckpoint() const = 0;
  virtual void setOnPruningProcess(bool inProcess) = 0;
  virtual ~IControlHandler() = default;
};

class IRequestsHandler {
 public:
  struct ExecutionRequest {
    uint16_t clientId = 0;
    uint64_t executionSequenceNum = 0;
    uint8_t flags = 0;  // copy of ClientRequestMsg flags
    uint32_t requestSize = 0;
    const char *request;
    uint32_t maxReplySize = 0;
    char *outReply;
    uint64_t requestSequenceNum = executionSequenceNum;
    uint32_t outActualReplySize = 0;
    uint32_t outReplicaSpecificInfoSize = 0;
    int outExecutionStatus = 1;
  };

  typedef std::deque<ExecutionRequest> ExecutionRequestsQueue;

  virtual void execute(ExecutionRequestsQueue &requests,
                       const std::string &batchCid,
                       concordUtils::SpanWrapper &parent_span) = 0;

  virtual void onFinishExecutingReadWriteRequests() {}

  std::shared_ptr<concord::reconfiguration::IReconfigurationHandler> getReconfigurationHandler() const {
    return reconfig_handler_;
  }
  void setReconfigurationHandler(std::shared_ptr<concord::reconfiguration::IReconfigurationHandler> rh) {
    reconfig_handler_ = rh;
  }
  std::shared_ptr<concord::reconfiguration::IPruningHandler> getPruningHandler() const { return pruning_handler_; }
  void setPruningHandler(std::shared_ptr<concord::reconfiguration::IPruningHandler> ph) { pruning_handler_ = ph; }
  virtual ~IRequestsHandler() = default;

 protected:
  std::shared_ptr<concord::reconfiguration::IReconfigurationHandler> reconfig_handler_;
  std::shared_ptr<concord::reconfiguration::IPruningHandler> pruning_handler_;
};

class IReplica {
 public:
  using IReplicaPtr = std::unique_ptr<IReplica>;
  static IReplicaPtr createNewReplica(const ReplicaConfig &,
                                      IRequestsHandler *,
                                      IStateTransfer *,
                                      bft::communication::ICommunication *,
                                      MetadataStorage *,
                                      std::shared_ptr<concord::performance::PerformanceManager> sdm =
                                          std::make_shared<concord::performance::PerformanceManager>());
  static IReplicaPtr createNewReplica(const ReplicaConfig &,
                                      IRequestsHandler *,
                                      IStateTransfer *,
                                      bft::communication::ICommunication *,
                                      MetadataStorage *,
                                      bool &erasedMetadata,
                                      std::shared_ptr<concord::performance::PerformanceManager> sdm);

  static IReplicaPtr createNewRoReplica(const ReplicaConfig &, IStateTransfer *, bft::communication::ICommunication *);

  virtual ~IReplica() = default;

  virtual bool isRunning() const = 0;

  virtual int64_t getLastExecutedSequenceNum() const = 0;

  virtual void start() = 0;

  virtual void stop() = 0;

  // TODO(GG) : move the following methods to an "advanced interface"
  virtual void SetAggregator(std::shared_ptr<concordMetrics::Aggregator> a) = 0;
  virtual void restartForDebug(uint32_t delayMillis) = 0;  // for debug only.
};

}  // namespace bftEngine
