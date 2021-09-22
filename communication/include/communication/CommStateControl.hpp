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
class CommStateControl {
 public:
  static CommStateControl& instance() {
    static CommStateControl instance_;
    return instance_;
  }
  void setBlockNewConnectionsFlag(bool flag) { blockNewConnectionsFlag_ = flag; }
  bool getBlockNewConnectionsFlag() { return blockNewConnectionsFlag_; }

  void setCommRestartCallBack(std::function<void()> cb) {
    if (cb != nullptr) comm_restart_cb_registry_.add(cb);
  }

  void restartComm() { comm_restart_cb_registry_.invokeAll(); }

 private:
  bool blockNewConnectionsFlag_ = false;
  concord::util::CallbackRegistry<> comm_restart_cb_registry_;
};
}  // namespace bft::communication
