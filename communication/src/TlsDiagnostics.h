// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#pragma once

#include <atomic>
#include <sstream>

#include "kvstream.h"
#include "diagnostics.h"

namespace bft::communication {

// TLS Status registered with and retrieved by the diagnostics status handler.

// We probably want some of these things to be metrics also. For now, we just make them available
// via diagnostics.
//
// Note that because these are all updated independently, it's possible that there can be a
// transient state, where the values are in flight and don't quite add up. For instance, a connection
// may have finished resolving, but not have updated num_connecting yet. While we could add an
// atomic<boolean> to keep track of updates in progress, this seems like overkill, and makes the
// code harder to read. it's also possible that we forget to reset it and cause more problems than
// we solve.
struct TlsStatus {
  TlsStatus()
      : total_accepted_connections(0),
        total_connect_attempts_completed(0),
        num_resolving(0),
        num_connecting(0),
        num_connected_waiting_for_handshake(0),
        num_accepted_waiting_for_handshake(0),
        num_connections(0),
        total_messages_sent(0),
        total_messages_dropped(0) {}

  void reset() {
    total_accepted_connections = 0;
    total_connect_attempts_completed = 0;
    num_resolving = 0;
    num_connecting = 0;
    num_connected_waiting_for_handshake = 0;
    num_accepted_waiting_for_handshake = 0;
    num_connections = 0;
    total_messages_sent = 0;
    total_messages_dropped = 0;
  }

  std::string status() {
    std::ostringstream oss;
    oss << KVLOG(total_accepted_connections) << std::endl;
    oss << KVLOG(total_connect_attempts_completed) << std::endl;
    oss << KVLOG(num_resolving) << std::endl;
    oss << KVLOG(num_connecting) << std::endl;
    oss << KVLOG(num_connected_waiting_for_handshake) << std::endl;
    oss << KVLOG(num_accepted_waiting_for_handshake) << std::endl;
    oss << KVLOG(num_connections) << std::endl;
    oss << KVLOG(total_messages_sent) << std::endl;
    oss << KVLOG(total_messages_dropped) << std::endl;
    return oss.str();
  }

  std::atomic<size_t> total_accepted_connections;
  std::atomic<size_t> total_connect_attempts_completed;
  std::atomic<size_t> num_resolving;
  std::atomic<size_t> num_connecting;
  std::atomic<size_t> num_connected_waiting_for_handshake;
  std::atomic<size_t> num_accepted_waiting_for_handshake;
  std::atomic<size_t> num_connections;
  std::atomic<size_t> total_messages_sent;
  std::atomic<size_t> total_messages_dropped;
};

// Histogram Recorders for use in the TLS code.
// These should only be written in the ASIO thread.
struct Recorders {
  static constexpr int64_t MAX_NS = 1000 * 1000 * 1000 * 60l;  // 60s
  static constexpr int64_t MAX_US = 1000 * 1000 * 60;          // 60s
  static constexpr int64_t MAX_QUEUE_LENGTH = 100000;

  using Recorder = concord::diagnostics::Recorder;
  using Unit = concord::diagnostics::Unit;

  Recorders(std::string port, int64_t max_msg_size, int64_t max_queue_size_in_bytes)
      : write_queue_len(std::make_shared<Recorder>(1, MAX_QUEUE_LENGTH, 3, Unit::COUNT)),
        write_queue_size_in_bytes(std::make_shared<Recorder>(1, max_queue_size_in_bytes, 3, Unit::BYTES)),
        sent_msg_size(std::make_shared<Recorder>(1, max_msg_size, 3, Unit::BYTES)),
        received_msg_size(std::make_shared<Recorder>(1, max_msg_size, 3, Unit::BYTES)),
        send_enqueue_time(std::make_shared<Recorder>(1, MAX_NS, 3, Unit::NANOSECONDS)),
        send_time_in_queue(std::make_shared<Recorder>(1, MAX_NS, 3, Unit::NANOSECONDS)),
        read_enqueue_time(std::make_shared<Recorder>(1, MAX_NS, 3, Unit::NANOSECONDS)),
        time_between_reads(std::make_shared<Recorder>(1, MAX_US, 3, Unit::MICROSECONDS)) {
    auto& registrar = concord::diagnostics::RegistrarSingleton::getInstance();
    registrar.perf.registerComponent("tls" + port,
                                     {
                                         {"write_queue_len", write_queue_len},
                                         {"write_queue_size_in_bytes", write_queue_size_in_bytes},
                                         {"sent_msg_size", sent_msg_size},
                                         {"received_msg_size", received_msg_size},
                                         {"send_enqueue_time", send_enqueue_time},
                                         {"send_time_in_queue", send_time_in_queue},
                                         {"read_enqueue_time", read_enqueue_time},

                                     });
  }

  std::shared_ptr<Recorder> write_queue_len;
  std::shared_ptr<Recorder> write_queue_size_in_bytes;
  std::shared_ptr<Recorder> sent_msg_size;
  std::shared_ptr<Recorder> received_msg_size;
  std::shared_ptr<Recorder> send_enqueue_time;
  std::shared_ptr<Recorder> send_time_in_queue;
  std::shared_ptr<Recorder> read_enqueue_time;
  std::shared_ptr<concord::diagnostics::Recorder> time_between_reads;
};

}  // namespace bft::communication
