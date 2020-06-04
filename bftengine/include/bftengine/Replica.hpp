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

namespace bftEngine {

// Possible values for 'flags' parameter
enum MsgFlag : uint8_t {
  EMPTY_FLAGS = 0x0,
  READ_ONLY_FLAG = 0x1,
  PRE_PROCESS_FLAG = 0x2,
  HAS_PRE_PROCESSED_FLAG = 0x4
};

class IRequestsHandler {
 public:
  virtual int execute(uint16_t clientId,
                      uint64_t sequenceNum,
                      uint8_t flags,
                      uint32_t requestSize,
                      const char *request,
                      uint32_t maxReplySize,
                      char *outReply,
                      uint32_t &outActualReplySize,
                      concordUtils::SpanWrapper &parent_span) = 0;

  virtual void onFinishExecutingReadWriteRequests() {}
  virtual ~IRequestsHandler() {}
};

class IReplica {
 public:
  using IReplicaPtr = std::unique_ptr<IReplica>;
  static IReplicaPtr createNewReplica(
      ReplicaConfig *, IRequestsHandler *, IStateTransfer *, bft::communication::ICommunication *, MetadataStorage *);

  static IReplicaPtr createNewRoReplica(ReplicaConfig *,
                                        IStateTransfer *,
                                        bft::communication::ICommunication *,
                                        MetadataStorage *);

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
