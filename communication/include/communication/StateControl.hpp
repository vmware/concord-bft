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
#include <unordered_map>
#include <variant>
#include <memory>
#include "Logger.hpp"

namespace bft::communication {
struct EnumHash {
  template <typename T>
  size_t operator()(T t) const {
    return static_cast<size_t>(t);
  }
};

class StateControl {
 public:
  using CallbackRegistry = concord::util::CallbackRegistry<uint32_t>;
  enum class EventType { TLS_COMM, THIN_REPLICA_SERVER };

  static StateControl& instance() {
    static StateControl instance_;
    return instance_;
  }

  void lockComm() { lock_comm_.lock(); }
  bool tryLockComm() { return lock_comm_.try_lock(); }
  void unlockComm() { lock_comm_.unlock(); }

  // Added/retained these methods to act as facade
  void setCommRestartCallBack(std::function<void(uint32_t)> cb, const EventType& et = EventType::TLS_COMM) {
    registerCallback(et, cb);
  }

  void restartComm(uint32_t id, const EventType& et = EventType::TLS_COMM) { invokeCallback(et, id); }

  void setTlsRestartCallBack(std::function<void(uint32_t)> cb, const EventType& et = EventType::THIN_REPLICA_SERVER) {
    registerCallback(et, cb);
  }

  void restartThinReplicaServer(uint32_t id, const EventType& et = EventType::THIN_REPLICA_SERVER) {
    invokeCallback(et, id);
  }

  const std::string getEventTypeAsString(const EventType& et) {
    switch (et) {
      case EventType::TLS_COMM:
        return "TLS_COMM";
      case EventType::THIN_REPLICA_SERVER:
        return "THIN_REPLICA_SERVER";
      default:
        return "Invalid Type";
    }
  }
  void setGetPeerPubKeyMethod(std::function<std::string(uint32_t)> m) { get_peer_pub_key_ = std::move(m); }

  std::string getPeerPubKey(uint32_t id) {
    if (get_peer_pub_key_) return get_peer_pub_key_(id);
    return std::string();
  }

 private:
  StateControl() : logger_(logging::getLogger("concord-bft.comm.StetControl")) {}

  void registerCallback(const EventType& et, std::function<void(uint32_t)> cb) {
    if (cb != nullptr) {
      if (event_registry_.find(et) == event_registry_.end()) {
        event_registry_.insert({et, std::make_unique<CallbackRegistry>()});
      }
      event_registry_[et]->add(cb);
    }
  }

  void invokeCallback(const EventType& et, uint32_t id) {
    if (event_registry_.find(et) != event_registry_.end()) {
      event_registry_[et]->invokeAll(id);
    } else {
      LOG_WARN(logger_, "No callback(s) registered for eventType " << getEventTypeAsString(et));
    }
  }

  logging::Logger logger_;
  std::mutex lock_comm_;
  // keeping function template to be void(uint32_t) for uniformity
  std::unordered_map<const EventType, std::unique_ptr<CallbackRegistry>, EnumHash> event_registry_;
  std::function<std::string(uint32_t)> get_peer_pub_key_;
};
}  // namespace bft::communication
