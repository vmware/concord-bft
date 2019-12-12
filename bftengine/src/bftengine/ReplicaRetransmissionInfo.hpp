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

#include "RollingAvgAndVar.hpp"
#include "RetransmissionParams.hpp"

namespace bftEngine::impl {

class ReplicaRetransmissionInfo {
 public:
  ReplicaRetransmissionInfo() : retransmissionTimeMilli_{PARM::defaultTimeBetweenRetransmissionsMilli} {}

  void add(uint64_t responseTimeMilli);
  uint64_t getRetransmissionTimeMilli();

 private:
  RollingAvgAndVar avgAndVarOfAckTime_;
  uint64_t retransmissionTimeMilli_ = 0;
};

}  // namespace bftEngine::impl