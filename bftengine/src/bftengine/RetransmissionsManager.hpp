// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <stdint.h>

#include "PrimitiveTypes.hpp"
#include "TimeUtils.hpp"

namespace util {
class SimpleThreadPool;
}
namespace bftEngine {
namespace impl {
class IncomingMsgsStorage;

// TODO(GG): use types from PrimitiveTypes.hpp

class RetransmissionsParams  // Parameters // TODO(GG): some of the parameters should be taken from the replica
                             // configuration (TBD)
{
 public:
  static const uint16_t maxNumberOfConcurrentManagedTransmissions = 2000;

  static const uint16_t maxTransmissionsPerMsg = 4;

  static const uint16_t maxNumberOfMsgsInThreadLocalQueue = 500;
  static const uint16_t sufficientNumberOfMsgsToStartBkProcess = 50;

  static const uint16_t maxTimeBetweenRetranMilli = 5000;
  static const uint16_t minTimeBetweenRetranMilli = 20;
  static const uint16_t defaultTimeBetweenRetranMilli = 100;

  static const uint16_t evalPeriod = 30;
  static const uint16_t resetPoint = 10000;
  static const uint16_t maxIncreasingFactor = 4;
  static const uint16_t maxDecreasingFactor = 2;

  static_assert(maxTransmissionsPerMsg >= 2, "This functionality is not needed when maxTransmissionsPerMsg < 2");
  // TODO(GG): more static asserts may be needed
};

class RetransmissionsManager {
 public:
  RetransmissionsManager();  // retransmissions logic is disabled

  RetransmissionsManager(util::SimpleThreadPool* threadPool,
                         IncomingMsgsStorage* const incomingMsgsStorage,
                         uint16_t maxOutNumOfSeqNumbers,
                         SeqNum lastStableSeqNum);

  ~RetransmissionsManager();

  void onSend(uint16_t replicaId, SeqNum msgSeqNum, uint16_t msgType, bool ignorePreviousAcks = false);

  void onAck(uint16_t replicaId, SeqNum msgSeqNum, uint16_t msgType);

  bool tryToStartProcessing();

  void setLastStable(SeqNum newLastStableSeqNum);

  void setLastView(ViewNum newView);

  void OnProcessingComplete();  // TODO(GG): should only be called by internal code ...

 protected:
  enum class EType { ERROR = 0, SENT, ACK, SENT_AND_IGNORE_PREV };

  struct Event {
    EType etype;
    Time time;
    uint16_t replicaId;
    SeqNum msgSeqNum;
    uint16_t msgType;
  };

  void add(const Event& e);

  util::SimpleThreadPool* const pool;
  IncomingMsgsStorage* const incomingMsgs;
  const uint16_t maxOutSeqNumbers;
  void* const internalLogicInfo;

  SeqNum lastStable = 0;
  SeqNum lastView = 0;
  bool bkProcessing = false;
  std::vector<Event>* setOfEvents = nullptr;
  bool needToClearPendingRetransmissions = false;
};

}  // namespace impl
}  // namespace bftEngine
