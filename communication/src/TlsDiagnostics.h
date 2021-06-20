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
#include <chrono>
#include <sstream>

#include "kvstream.h"
#include "diagnostics.h"

namespace bft::communication {

inline int64_t durationInMicros(const std::chrono::steady_clock::time_point& start) {
  return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start).count();
}

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
  TlsStatus() { reset(); }

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
    msg_size_header_read_attempts = 0;
    msg_reads = 0;
    read_timer_started = 0;
    read_timer_stopped = 0;
    read_timer_expired = 0;
    write_timer_started = 0;
    write_timer_stopped = 0;
    write_timer_expired = 0;
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
    oss << KVLOG(msg_size_header_read_attempts) << std::endl;
    oss << KVLOG(msg_reads) << std::endl;
    oss << KVLOG(read_timer_started) << std::endl;
    oss << KVLOG(read_timer_stopped) << std::endl;
    oss << KVLOG(read_timer_expired) << std::endl;
    oss << KVLOG(write_timer_started) << std::endl;
    oss << KVLOG(write_timer_stopped) << std::endl;
    oss << KVLOG(write_timer_expired) << std::endl;
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
  std::atomic<size_t> msg_size_header_read_attempts;
  std::atomic<size_t> msg_reads;
  std::atomic<size_t> read_timer_started;
  std::atomic<size_t> read_timer_stopped;
  std::atomic<size_t> read_timer_expired;
  std::atomic<size_t> write_timer_started;
  std::atomic<size_t> write_timer_stopped;
  std::atomic<size_t> write_timer_expired;
};

// Histogram Recorders for use in the TLS code.
struct Recorders {
  static constexpr int64_t MAX_NS = 1000 * 1000 * 1000 * 60l;  // 60s
  static constexpr int64_t MAX_US = 1000 * 1000 * 60;          // 60s
  static constexpr int64_t MAX_QUEUE_LENGTH = 100000;

  using Recorder = concord::diagnostics::Recorder;
  using Unit = concord::diagnostics::Unit;

  Recorders(const std::string& selfId, int64_t max_msg_size, int64_t max_queue_size_in_bytes)
      : write_queue_size_in_bytes(
            MAKE_SHARED_RECORDER("write_queue_size_in_bytes", 1, max_queue_size_in_bytes, 3, Unit::BYTES)),
        sent_msg_size(MAKE_SHARED_RECORDER("sent_msg_size", 1, max_msg_size, 3, Unit::BYTES)),
        received_msg_size(MAKE_SHARED_RECORDER("received_msg_size", 1, max_msg_size, 3, Unit::BYTES)) {
    auto& registrar = concord::diagnostics::RegistrarSingleton::getInstance();
    registrar.perf.registerComponent("tls" + selfId,
                                     {write_queue_len,
                                      write_queue_size_in_bytes,
                                      sent_msg_size,
                                      received_msg_size,
                                      send_time_in_queue,
                                      read_enqueue_time,
                                      send_post_to_mgr,
                                      send_post_to_conn,
                                      async_write,
                                      async_read_header_partial,
                                      async_read_header_full,
                                      async_read_msg,
                                      on_connection_authenticated});
  }

  std::shared_ptr<Recorder> write_queue_size_in_bytes;
  std::shared_ptr<Recorder> sent_msg_size;
  std::shared_ptr<Recorder> received_msg_size;
  DEFINE_SHARED_RECORDER(write_queue_len, 1, MAX_QUEUE_LENGTH, 3, Unit::COUNT);
  DEFINE_SHARED_RECORDER(send_time_in_queue, 1, MAX_US, 3, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(read_enqueue_time, 1, MAX_US, 3, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(send_post_to_mgr, 1, MAX_US, 3, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(send_post_to_conn, 1, MAX_US, 3, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(async_write, 1, MAX_US, 3, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(async_read_header_full, 1, MAX_US, 3, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(async_read_header_partial, 1, MAX_US, 3, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(async_read_msg, 1, MAX_US, 3, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(on_connection_authenticated, 1, MAX_US, 3, Unit::MICROSECONDS);
};

}  // namespace bft::communication
