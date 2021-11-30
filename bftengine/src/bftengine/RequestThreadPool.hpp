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
#include <array>
#include <stdexcept>

#include "thread_pool.hpp"

namespace bftEngine {
namespace impl {

// This class is used to create a thread bag which will assimilate a lot of tasks at once.
// Then the tasks in the bag will be scheduled and executed.
// Currently the user of this thread bag will have to do the book keeping of all the tasks
// and will have to wait for them.
// TODO (achaudhuri) : Add the functionality of book keeping and wait for finish of tasks
//  for each thread
// This abstraction will evolve further to provide the functionality of a bag of thread
// using a theadpool.
class RequestThreadPool final {
 public:
  enum PoolLevel : uint16_t { STARTING = 0, FIRSTLEVEL, MAXLEVEL };
  static auto& getThreadPool(uint16_t level) {
    // Currently we need 2 level thread pools.
    static std::array<concord::util::ThreadPool, PoolLevel::MAXLEVEL> threadBag = {
        ReplicaConfig::instance().threadbagConcurrencyLevel1, ReplicaConfig::instance().threadbagConcurrencyLevel2};
    return threadBag.at(level);
  }

 private:
  RequestThreadPool() = default;
  ~RequestThreadPool() = default;

  RequestThreadPool(const RequestThreadPool&) = delete;
  RequestThreadPool& operator=(const RequestThreadPool&) = delete;
  RequestThreadPool(RequestThreadPool&&) = delete;
  RequestThreadPool& operator=(RequestThreadPool&&) = delete;
};

}  // namespace impl
}  // namespace bftEngine