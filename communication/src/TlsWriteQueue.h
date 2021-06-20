// Concord
//
// Copyright (c) 2020-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#pragma once

#include <arpa/inet.h>
#include <bits/stdint-uintn.h>
#include <atomic>
#include <cstddef>
#include <cstring>
#include <deque>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "communication/CommDefs.hpp"
#include "Logger.hpp"
#include "TlsDiagnostics.h"

namespace bft::communication::tls {

// Any message attempted to be put on the queue that causes the total size of the queue to exceed
// this value will be dropped. This is to prevent indefinite backups and useless stale messages.
// The number is very large right now so as not to affect current setups. In the future we will
// have better admission control.
static constexpr size_t MAX_QUEUE_SIZE_IN_BYTES = 1024 * 1024 * 1024;  // 1 GB
static constexpr size_t MSG_HEADER_SIZE = 4;

struct OutgoingMsg {
  OutgoingMsg(std::vector<uint8_t>&& raw_msg)
      : msg(raw_msg.size() + MSG_HEADER_SIZE), send_time(std::chrono::steady_clock::now()) {
    uint32_t msg_size = htonl(static_cast<uint32_t>(raw_msg.size()));
    std::memcpy(msg.data(), &msg_size, MSG_HEADER_SIZE);
    std::memcpy(msg.data() + MSG_HEADER_SIZE, raw_msg.data(), raw_msg.size());
  }
  std::vector<uint8_t> msg;
  std::chrono::steady_clock::time_point send_time;

  size_t payload_size() { return msg.size() - MSG_HEADER_SIZE; }
};

class WriteQueue {
 public:
  WriteQueue(Recorders& recorders) : logger_(logging::getLogger("concord-bft.tls.conn")), recorders_(recorders) {}

  // Only add onto the queue if there is an active connection. Return the size of the queue after
  // the push completes or std::nullopt if the queue is full.
  std::optional<size_t> push(std::shared_ptr<OutgoingMsg>&& msg) {
    if (queued_size_in_bytes_ > MAX_QUEUE_SIZE_IN_BYTES) {
      std::string destination = destination_.has_value() ? std::to_string(*destination_) : "unknown";
      LOG_WARN(logger_, "Queue full. Dropping message." << KVLOG(destination, msg->payload_size()));
      return std::nullopt;
    }
    queued_size_in_bytes_ += msg->msg.size();
    msgs_.push_back(std::move(msg));
    return msgs_.size();
  }

  std::shared_ptr<OutgoingMsg> pop() {
    recorders_.write_queue_len->recordAtomic(msgs_.size());
    recorders_.write_queue_size_in_bytes->recordAtomic(queued_size_in_bytes_);
    if (msgs_.empty()) {
      return nullptr;
    }
    auto msg = std::move(msgs_.front());
    msgs_.pop_front();
    queued_size_in_bytes_ -= msg->msg.size();
    return msg;
  }

  void clear() {
    msgs_.clear();
    queued_size_in_bytes_ = 0;
  }

  void setDestination(NodeNum id) { destination_ = id; }
  size_t size() const { return msgs_.size(); }

  size_t sizeInBytes() const { return queued_size_in_bytes_; }

  WriteQueue(const WriteQueue&) = delete;
  WriteQueue& operator=(const WriteQueue&) = delete;

 private:
  std::deque<std::shared_ptr<OutgoingMsg>> msgs_;
  size_t queued_size_in_bytes_ = 0;

  std::optional<NodeNum> destination_ = std::nullopt;
  logging::Logger logger_;
  Recorders& recorders_;
};

}  // namespace bft::communication::tls
