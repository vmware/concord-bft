// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#pragma once
#include <functional>
#include "callback_registry.hpp"
namespace bft::communication {
class StateControl {
 public:
  static StateControl& instance() {
    static StateControl instance_;
    return instance_;
  }
  void setBlockNewConnectionsFlag(bool flag) { blockNewConnectionsFlag_ = flag; }
  bool getBlockNewConnectionsFlag() const { return blockNewConnectionsFlag_; }

  void setCommRestartCallBack(std::function<void(uint32_t)> cb) {
    if (cb != nullptr) comm_restart_cb_registry_.add(cb);
  }

  void restartComm(uint32_t id) { comm_restart_cb_registry_.invokeAll(id); }

 private:
  bool blockNewConnectionsFlag_ = false;
  concord::util::CallbackRegistry<uint32_t> comm_restart_cb_registry_;
};
}  // namespace bft::communication
