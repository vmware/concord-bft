// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use
// this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license
// terms. Your use of these subcomponents is subject to the terms and conditions of the
// subcomponent's license, as noted in the LICENSE file.

#pragma once

#include <chrono>
#include <cstdint>
#include <atomic>

#include "assertUtils.hpp"
#include "base_types.h"

namespace bft::client {

// This is a basic implementation of a monotonic clock based sequence number.
//
// Using this class is safe, but not necessarily live. It is safe because messages from a client
// with an old (or identical) sequence number are simply ignored. It may not be live because if your machine
// reboots, its clock may be set "backwards". Within a few seconds however, given NTP,
// things should be back to normal and new requests will be handled. This is actually a very
// unlikely scenario, as reboots will generally take longer than any possible clock drift. Therefore
// this is a good default sequence number generator.
//
// Users are allowed to generate their own sequence numbers (they do not
// have to use this class). Other examples of ways to do this include:
// (1) A simple counter + store the last counter in a persistent storage
// (2) A mechanism to retrieve the last used sequence number from the replicas on restart. No such
//     mechanism is cucrrently implemented, buf if necessary we will add it.
class SeqNumberGenerator {
 public:
  SeqNumberGenerator(ClientId client_id) : client_id_(client_id) {}

  uint64_t unique() {
    std::scoped_lock sl(mtx_);
    std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
    return unique(now);
  }

  uint64_t unique(std::chrono::time_point<std::chrono::system_clock> now) {
    uint64_t milli = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

    if (milli > lastMilliOfUniqueFetchID_) {
      lastMilliOfUniqueFetchID_ = milli;
      lastCountOfUniqueFetchID_ = 0;
    } else {
      if (lastCountOfUniqueFetchID_ == LAST_COUNT_LIMIT) {
        LOG_WARN(logger_, "Client SeqNum Counter reached max value. " << KVLOG(client_id_.val));
        lastMilliOfUniqueFetchID_++;
        lastCountOfUniqueFetchID_ = 0;
      } else {
        // increase last count to preserve uniqueness.
        lastCountOfUniqueFetchID_++;
      }
    }
    // shift lastMilli by 22 (0x3FFFFF) in order to 'bitwise or' with lastCount
    // and preserve uniqueness and monotonicity.
    uint64_t r = (lastMilliOfUniqueFetchID_ << (64 - 42));
    ConcordAssert(lastCountOfUniqueFetchID_ <= LAST_COUNT_LIMIT);
    r = r | ((uint64_t)lastCountOfUniqueFetchID_);

    return r;
  }

 private:
  // limited to the size lastMilli shifted.
  const uint64_t LAST_COUNT_LIMIT = 0x3FFFFF;
  // lastMilliOfUniqueFetchID_ holds the last SN generated,
  uint64_t lastMilliOfUniqueFetchID_ = 0;

  // lastCount used to preserve uniqueness.
  uint32_t lastCountOfUniqueFetchID_ = 0;

  ClientId client_id_;

  logging::Logger logger_ = logging::getLogger("bftclient.seqnum");
  std::mutex mtx_;
};

}  // namespace bft::client
