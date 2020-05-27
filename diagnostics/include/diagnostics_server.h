// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

// This file contains a tcp server listening on localhost that handles the messages in protocol.h.

#pragma once

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstring>
#include <future>
#include <sstream>
#include <utility>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <thread>
#include <unistd.h>

#include "Logger.hpp"
#include "errnoString.hpp"
#include "protocol.h"

using concordUtils::errnoString;

namespace concord::diagnostics {

static constexpr uint16_t PORT = 6888;
static constexpr int BACKLOG = 5;
static constexpr size_t MAX_INPUT_SIZE = 1024;

static concordlogger::Logger logger = concordlogger::Log::getLogger("concord.diagnostics");

// Returns a successfully read line as a string.
// Throws a std::runtime_error on error.
std::string readline(int sock) {
  std::array<char, MAX_INPUT_SIZE> buf;
  buf.fill(0);
  int count = 0;
  auto start = std::chrono::steady_clock::now();
  auto timeout = std::chrono::microseconds(999999);
  auto remaining = timeout;
  while (true) {
    fd_set read_fds;
    FD_ZERO(&read_fds);
    FD_SET(sock, &read_fds);
    timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = remaining.count();
    auto rv = select(sock + 1, &read_fds, NULL, NULL, &tv);
    if (rv == 0) {
      throw std::runtime_error("timeout");
    }
    if (rv < 0 && errno == EINTR) continue;
    if (rv < 0) {
      throw std::runtime_error("select failed: " + errnoString(rv));
    }

    if (count == MAX_INPUT_SIZE) {
      throw std::runtime_error("Request exceeded max size: " + std::to_string(MAX_INPUT_SIZE));
    }

    rv = read(sock, &buf + count, buf.size() - count);
    if (rv <= 0) {
      throw std::runtime_error("read failed: " + errnoString(rv));
    }
    count += rv;

    // Check to see if we have a complete command
    auto it = std::find(buf.begin(), buf.end(), '\n');
    if (it != buf.end()) {
      return std::string(buf.begin(), it);
    }

    // We may not have received all the data yet. Update the timeout.
    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    remaining = timeout - duration;
  }
}

void handleRequest(const Registrar& registrar, int sock) {
  try {
    std::stringstream ss(readline(sock));
    std::vector<std::string> tokens;
    std::string token;
    while (std::getline(ss, token, ' ')) {
      tokens.push_back(token);
    }
    std::string output = run(tokens, registrar);
    if (write(sock, output.data(), output.size()) < 0) {
      LOG_WARN(logger, "Failed to write to client socket: " << errnoString(errno));
    }
    close(sock);
  } catch (const std::exception& e) {
    std::string out = std::string("Error: ") + e.what() + "\n";
    if (write(sock, out.data(), out.size()) < 0) {
      LOG_WARN(logger, "Failed to write to client socket: " << errnoString(errno));
    }
    close(sock);
  }
}

// Each request creates a separate connection and spawns a thread.
//
// The purposes behind this decision are simplicity and expediency. If performance becomes a problem
// we can maintain persistent connections and/or create a thread pool. We can also switch to using
// async connections via boost ASIO if necessary, although this seems extremely heavy handed for the
// use case.
class Server {
 public:
  void start(const Registrar& registrar) {
    shutdown_.store(false);
    listen_thread_ = std::thread([this, &registrar]() {
      listen();

      while (!shutdown_.load()) {
        fd_set read_fds;
        FD_ZERO(&read_fds);
        FD_SET(listen_sock_, &read_fds);
        timeval tv;
        tv.tv_sec = 1;
        tv.tv_usec = 0;
        auto rv = select(listen_sock_ + 1, &read_fds, NULL, NULL, &tv);
        if (rv == 0) continue;  // timeout
        if (rv < 0 && errno == EINTR) continue;
        assert(rv > 0);
        int sock = accept(listen_sock_, NULL, NULL);
        // We must bind the result future or else this call blocks.
        auto _ = std::async(std::launch::async, [&]() { handleRequest(registrar, sock); });
        (void)_;  // unused variable hack
      }
    });
  }

  void stop() {
    shutdown_.store(true);
    listen_thread_.join();
  };

 private:
  void listen() {
    listen_sock_ = socket(AF_INET, SOCK_STREAM, 0);
    assert(listen_sock_ >= 0);
    bzero(&servaddr_, sizeof(servaddr_));
    servaddr_.sin_family = AF_INET;
    // LOCALHOST ONLY, FOR SECURITY PURPOSES. DO NOT CHANGE THIS!!!
    servaddr_.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    servaddr_.sin_port = htons(PORT);
    int enable = 1;
    if (setsockopt(listen_sock_, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable))) {
      LOG_FATAL(logger, "Failed to set listen socket options: " << errnoString(errno));
      std::exit(-1);
    }
    if (bind(listen_sock_, (sockaddr*)&servaddr_, sizeof(servaddr_))) {
      LOG_FATAL(logger, "Failed to bind listen socket: " << errnoString(errno));
      std::exit(-1);
    }
    if (::listen(listen_sock_, BACKLOG)) {
      LOG_FATAL(logger, "Failed to listen for connections: " << errnoString(errno));
      std::exit(-1);
    }
  }

  int listen_sock_;
  sockaddr_in servaddr_;
  std::thread listen_thread_;
  std::atomic<bool> shutdown_;
};

}  // namespace concord::diagnostics
