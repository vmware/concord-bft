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

#include "ReplicaRetransmissionInfo.hpp"
#include <assertUtils.hpp>

#include <math.h>

namespace bftEngine::impl {

void ReplicaRetransmissionInfo::add(uint64_t responseTimeMilli) {
  // TODO(GG): ?? we only wanted safe conversion from uint64_t to double
  const uint64_t hours24 = 24 * 60 * 60 * 1000;
  if (responseTimeMilli > hours24) responseTimeMilli = hours24;

  const auto v = (double)responseTimeMilli;
  avgAndVarOfAckTime_.add(v);
  const int numOfElements = avgAndVarOfAckTime_.numOfElements();

  if (numOfElements % PARM::evalPeriod == 0) {
    const uint64_t maxVal = std::min((uint64_t)PARM::maxTimeBetweenRetransmissionsMilli,
                                     retransmissionTimeMilli_ * PARM::maxIncreasingFactor);
    const uint64_t minVal = std::max((uint64_t)PARM::minTimeBetweenRetransmissionsMilli,
                                     retransmissionTimeMilli_ / PARM::maxDecreasingFactor);
    Assert(minVal <= maxVal);

    const double avg = avgAndVarOfAckTime_.avg();
    const double var = avgAndVarOfAckTime_.var();
    const double sd = ((var > 0) ? sqrt(var) : 0);
    const uint64_t newRetransmission = (uint64_t)avg + 2 * ((uint64_t)sd);

    if (newRetransmission > maxVal)
      retransmissionTimeMilli_ = maxVal;
    else if (newRetransmission < minVal)
      retransmissionTimeMilli_ = minVal;
    else
      retransmissionTimeMilli_ = newRetransmission;

    if (numOfElements >= PARM::resetPoint) avgAndVarOfAckTime_.reset();
  }
}

uint64_t ReplicaRetransmissionInfo::getRetransmissionTimeMilli() {
  if (retransmissionTimeMilli_ == PARM::maxTimeBetweenRetransmissionsMilli)
    return UINT64_MAX;
  else
    return retransmissionTimeMilli_;
}

}  // namespace bftEngine::impl