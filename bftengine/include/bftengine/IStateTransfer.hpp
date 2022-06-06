// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <memory>
#include "IReservedPages.hpp"
#include "Timers.hpp"
#include "messages/MessageBase.hpp"

using std::shared_ptr;

namespace concord::client::reconfiguration {
class ClientReconfigurationEngine;
}
namespace bftEngine {
class IReplicaForStateTransfer;  // forward definition

// May become larger over time based on all types of msgs we intercept
// For now we intercept only preprepare
struct ConsensusMsg {
  ConsensusMsg() = delete;
  ConsensusMsg(MsgType type, NodeIdType sender_id) : type_(type), sender_id_(sender_id) {}
  const MsgType type_;
  const NodeIdType sender_id_;
};

class IStateTransfer : public IReservedPages {
 public:
  enum StateTransferCallBacksPriorities { HIGH = 0, DEFAULT = 20, LOW = 40 };
  virtual ~IStateTransfer() {}

  // The methods of this interface will always be called by the same thread of
  // the BFT engine. They will never be called when operations are executing
  // (i.e., the state is not allowed to changed concurrently by operations).

  // management
  virtual void init(uint64_t maxNumOfRequiredStoredCheckpoints,
                    uint32_t numberOfRequiredReservedPages,
                    uint32_t sizeOfReservedPage) = 0;
  virtual void startRunning(IReplicaForStateTransfer *r) = 0;
  virtual void stopRunning() = 0;
  virtual bool isRunning() const = 0;

  // checkpoints

  virtual void createCheckpointOfCurrentState(uint64_t checkpointNumber) = 0;

  virtual void getDigestOfCheckpoint(uint64_t checkpointNumber,
                                     uint16_t sizeOfDigestBuffer,
                                     uint64_t &outBlockId,
                                     char *outStateDigest,
                                     char *outResPagesDigest,
                                     char *outRVBDataDigest) = 0;

  // state
  virtual void startCollectingState() = 0;

  virtual bool isCollectingState() const = 0;

  // timer (for simple implementation, a state transfer module can use its own
  // timers and threads)
  virtual void onTimer() = 0;

  // messsage that was send via the BFT engine
  // (a state transfer module may directly send messages).
  // Message msg should be released by using
  // IReplicaForStateTransfer::freeStateTransferMsg
  virtual void handleStateTransferMessage(char *msg, uint32_t msgLen, uint16_t senderId) = 0;

  // Return the internal state (member variables, etc...) of the state transfer module as a string. This is used by the
  // diagnostics subsystem.
  virtual std::string getStatus() { return ""; };

  // Registers a function that is called every time State Transfer completes.
  // Accepts the checkpoint number as a parameter.
  // Callbacks must not throw.
  // Multiple callbacks can be added.
  virtual void addOnTransferringCompleteCallback(
      const std::function<void(uint64_t)> &cb,
      StateTransferCallBacksPriorities priority = StateTransferCallBacksPriorities::DEFAULT) = 0;

  // Registers a function that is called every time fetching state changes
  virtual void addOnFetchingStateChangeCallback(const std::function<void(uint64_t)> &) = 0;

  virtual void setEraseMetadataFlag() = 0;

  virtual void setReconfigurationEngine(
      std::shared_ptr<concord::client::reconfiguration::ClientReconfigurationEngine>) = 0;
  virtual void handleIncomingConsensusMessage(const ConsensusMsg msg) = 0;
  // Reports State Transfer whenever a prune command is triggered into storage, after a consensus on the last prunable
  // block. lastAgreedPrunableBlockId is the maximal block to be deleted from local storage.
  virtual void reportLastAgreedPrunableBlockId(uint64_t lastAgreedPrunableBlockId) = 0;
};

// This interface may only be used when the state transfer module is runnning
// (methods can be invoked by any thread)
class IReplicaForStateTransfer {
 public:
  virtual void onTransferringComplete(uint64_t checkpointNumberOfNewState) = 0;

  // The following methods can be used to simplify the state transfer
  // implementations. (A state transfer module may not need to use them).

  virtual void freeStateTransferMsg(char *m) = 0;

  virtual void sendStateTransferMessage(char *m, uint32_t size, uint16_t replicaId) = 0;

  // the timer is disabled when timerPeriodMilli==0
  // (notice that the state transfer module can use its own timers and threads)
  virtual void changeStateTransferTimerPeriod(uint32_t timerPeriodMilli) = 0;

  // Invoke State Transfer timer callback once
  virtual concordUtil::Timers::Handle addOneShotTimer(uint32_t timeoutMilli) = 0;

  virtual ~IReplicaForStateTransfer() = default;
};
}  // namespace bftEngine
