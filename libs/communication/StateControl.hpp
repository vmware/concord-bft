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
#include "util/callback_registry.hpp"
#include <unordered_map>
#include <variant>
#include <memory>

#include "log/logger.hpp"
#include "util/assertUtils.hpp"

namespace bft::communication {
struct EnumHash {
  template <typename T>
  size_t operator()(T t) const {
    return static_cast<size_t>(t);
  }
};

class StateControl {
 private:
  static constexpr const size_t replicaIdentityHistoryCount = 2;

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
  void setCommRestartCallBack(std::function<void(uint32_t)>&& cb, const EventType& et = EventType::TLS_COMM) {
    registerCallback(et, std::move(cb));
  }

  void restartComm(uint32_t id, const EventType& et = EventType::TLS_COMM) { invokeCallback(et, id); }

  void setTrsRestartCallBack(std::function<void(uint32_t)>&& cb, const EventType& et = EventType::THIN_REPLICA_SERVER) {
    registerCallback(et, std::move(cb));
  }

  void restartThinReplicaServer(uint32_t id = 0, const EventType& et = EventType::THIN_REPLICA_SERVER) {
    invokeCallback(et, id);
  }

  const std::string getEventTypeAsString(const EventType& et) {
    std::string str;
    switch (et) {
      case EventType::TLS_COMM:
        str = "TLS_COMM";
        break;
      case EventType::THIN_REPLICA_SERVER:
        str = "THIN_REPLICA_SERVER";
        break;
    }
    return str;
  }
  void setGetPeerPubKeyMethod(std::function<std::array<std::string, replicaIdentityHistoryCount>(uint32_t)> m) {
    get_peer_pub_key_ = std::move(m);
  }

  std::array<std::string, replicaIdentityHistoryCount> getPeerPubKey(uint32_t id) {
    if (get_peer_pub_key_) return get_peer_pub_key_(id);
    return {};
  }

 private:
  StateControl() : logger_(logging::getLogger("concord-bft.comm.state_control")) {}

  void registerCallback(const EventType& et, std::function<void(uint32_t)>&& cb) {
    ConcordAssert(cb != nullptr);
    if (event_registry_.find(et) == event_registry_.end()) {
      event_registry_.insert({et, std::make_unique<CallbackRegistry>()});
    }
    event_registry_[et]->add(cb);
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
  std::function<std::array<std::string, replicaIdentityHistoryCount>(uint32_t)> get_peer_pub_key_;
};
}  // namespace bft::communication
