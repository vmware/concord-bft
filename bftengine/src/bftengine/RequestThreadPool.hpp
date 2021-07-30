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

#include "thread_pool.hpp"

namespace bftEngine {
namespace impl {

class RequestThreadPool final {
 public:
  static auto& getThreadPool() {
    static thread_local concord::util::ThreadPool threadBag{
        ReplicaConfig::instance().get("concord.bft.message.preprepareDigestCalculationConcurrency", 16u)};
    return threadBag;
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