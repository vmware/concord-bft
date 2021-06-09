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
#include <functional>

#include "IStateTransfer.hpp"
#include "OpenTracing.hpp"
#include "communication/ICommunication.hpp"
#include "MetadataStorage.hpp"
#include "Metrics.hpp"
#include "ReplicaConfig.hpp"
#include "PerformanceManager.hpp"
#include "IRequestHandler.hpp"

namespace concord::secretsmanager {
class ISecretsManagerImpl;
}

namespace bftEngine {
// Possible values for 'flags' parameter
enum MsgFlag : uint8_t {
  EMPTY_FLAGS = 0x0,
  READ_ONLY_FLAG = 0x1,
  PRE_PROCESS_FLAG = 0x2,
  HAS_PRE_PROCESSED_FLAG = 0x4,
  KEY_EXCHANGE_FLAG = 0x8,  // TODO [TK] use reconfig_flag
  TICK_FLAG = 0x10,
  RECONFIG_FLAG = 0x20,
  TIME_SERVICE_FLAG = 0x40,
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
  virtual void addOnSuperStableCheckpointCallBack(const std::function<void()> &cb) = 0;
  virtual void addOnStableCheckpointCallBack(const std::function<void()> &cb) = 0;
  virtual ~IControlHandler() = default;
};

class IReplica {
 public:
  using IReplicaPtr = std::unique_ptr<IReplica>;
  static IReplicaPtr createNewReplica(const ReplicaConfig &,
                                      std::shared_ptr<IRequestsHandler>,
                                      IStateTransfer *,
                                      bft::communication::ICommunication *,
                                      MetadataStorage *,
                                      std::shared_ptr<concord::performance::PerformanceManager> pm,
                                      const std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> &sm);
  static IReplicaPtr createNewReplica(const ReplicaConfig &,
                                      std::shared_ptr<IRequestsHandler>,
                                      IStateTransfer *,
                                      bft::communication::ICommunication *,
                                      MetadataStorage *,
                                      bool &erasedMetadata,
                                      std::shared_ptr<concord::performance::PerformanceManager> pm,
                                      const std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> &sm);

  static IReplicaPtr createNewRoReplica(const ReplicaConfig &,
                                        std::shared_ptr<IRequestsHandler>,
                                        IStateTransfer *,
                                        bft::communication::ICommunication *);

  virtual ~IReplica() = default;

  virtual bool isRunning() const = 0;

  virtual int64_t getLastExecutedSequenceNum() const = 0;

  virtual void start() = 0;

  virtual void stop() = 0;

  // TODO(GG) : move the following methods to an "advanced interface"
  virtual void SetAggregator(std::shared_ptr<concordMetrics::Aggregator>) = 0;
  virtual void restartForDebug(uint32_t delayMillis) = 0;  // for debug only.
};

}  // namespace bftEngine
