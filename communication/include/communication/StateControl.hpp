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
#include <utility>
#include "callback_registry.hpp"
namespace bft::communication {
class StateControl {
 public:
  static StateControl& instance() {
    static StateControl instance_;
    return instance_;
  }
  void lockComm() { lock_comm_.lock(); }
  bool tryLockComm() { return lock_comm_.try_lock(); }
  void unlockComm() { lock_comm_.unlock(); }

  void setCommRestartCallBack(std::function<void(uint32_t)> cb) {
    if (cb != nullptr) comm_restart_cb_registry_.add(cb);
  }

  void restartComm(uint32_t id) { comm_restart_cb_registry_.invokeAll(id); }
  void setGetPeerPubKeyMethod(std::function<std::string(uint32_t)> m) { get_peer_pub_key_ = std::move(m); }
  std::string getPeerPubKey(uint32_t id) {
    if (get_peer_pub_key_) return get_peer_pub_key_(id);
    return std::string();
  }

 private:
  std::mutex lock_comm_;
  concord::util::CallbackRegistry<uint32_t> comm_restart_cb_registry_;
  std::function<std::string(uint32_t)> get_peer_pub_key_;
};
}  // namespace bft::communication
