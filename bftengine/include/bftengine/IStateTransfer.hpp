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
#include <string>

namespace bftEngine {
class IReplicaForStateTransfer;  // forward definition

class IStateTransfer {
 public:
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

  virtual void markCheckpointAsStable(uint64_t checkpointNumber) = 0;

  virtual void getDigestOfCheckpoint(uint64_t checkpointNumber, uint16_t sizeOfDigestBuffer, char *outDigestBuffer) = 0;

  // state
  virtual void startCollectingState() = 0;

  virtual bool isCollectingState() const = 0;

  // working with reserved pages
  virtual uint32_t numberOfReservedPages() const = 0;
  virtual uint32_t sizeOfReservedPage() const = 0;
  virtual bool loadReservedPage(uint32_t reservedPageId, uint32_t copyLength, char *outReservedPage) const = 0;
  virtual void saveReservedPage(uint32_t reservedPageId, uint32_t copyLength, const char *inReservedPage) = 0;
  virtual void zeroReservedPage(uint32_t reservedPageId) = 0;

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
};

// This interface may only be used when the state transfer module is runnning
// (methods can be invoked by any thread)
class IReplicaForStateTransfer {
 public:
  virtual void onTransferringComplete(int64_t checkpointNumberOfNewState) = 0;

  // The following methods can be used to simplify the state transfer
  // implementations. (A state transfer module may not need to use them).

  virtual void freeStateTransferMsg(char *m) = 0;

  virtual void sendStateTransferMessage(char *m, uint32_t size, uint16_t replicaId) = 0;

  // the timer is disabled when timerPeriodMilli==0
  // (notice that the state transfer module can use its own timers and threads)
  virtual void changeStateTransferTimerPeriod(uint32_t timerPeriodMilli) = 0;

  virtual ~IReplicaForStateTransfer() = default;
};
}  // namespace bftEngine
