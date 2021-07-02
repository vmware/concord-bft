// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "queue.h"

#include "orrery_msgs.cmf.hpp"

namespace concord::orrery {
class Mailbox {
 public:
  void put(Envelope&& envelope);

  const std::string& executorName() const { return name_; }

 public:
  // This is public solely for use by std::vector::resize()
  Mailbox(){};

 private:
  friend class Executor;
  explicit Mailbox(const std::string& name, const std::shared_ptr<detail::Queue>& queue) : name_(name), queue_(queue) {}

  std::string name_;
  std::shared_ptr<detail::Queue> queue_;
};

}  // namespace concord::orrery
