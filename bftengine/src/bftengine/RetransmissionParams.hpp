// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include "PrimitiveTypes.hpp"
#include "TimeUtils.hpp"

namespace bftEngine::impl {

enum class RetransmissionEventType { ERROR = 0, SENT, ACK, SENT_AND_IGNORE_PREV };

struct RetransmissionEvent {
  RetransmissionEventType eventType = RetransmissionEventType::ERROR;
  Time time;
  uint16_t replicaId = 0;
  SeqNum msgSeqNum = 0;
  uint16_t msgType = 0;
};

// TODO(GG): some of the parameters should be taken from the replica configuration (TBD)
struct RetransmissionParams {
  static const uint16_t maxNumberOfConcurrentManagedTransmissions = 2000;
  static const uint16_t maxTransmissionsPerMsg = 4;
  static const uint16_t maxNumberOfMsgsInThreadLocalQueue = 500;
  static const uint16_t sufficientNumberOfMsgsToStartBkProcess = 50;

  static const uint16_t maxTimeBetweenRetransmissionsMilli = 5000;
  static const uint16_t minTimeBetweenRetransmissionsMilli = 20;
  static const uint16_t defaultTimeBetweenRetransmissionsMilli = 100;

  static const uint16_t evalPeriod = 30;
  static const uint16_t resetPoint = 10000;
  static const uint16_t maxIncreasingFactor = 4;
  static const uint16_t maxDecreasingFactor = 2;

  static_assert(maxTransmissionsPerMsg >= 2, "This functionality is not needed when maxTransmissionsPerMsg < 2");
};

#define PARM RetransmissionParams

}  // namespace bftEngine::impl
