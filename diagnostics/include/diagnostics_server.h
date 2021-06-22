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
#include "kvstream.h"
#include "protocol.h"

using concordUtils::errnoString;

namespace concord::diagnostics {

static constexpr int BACKLOG = 5;
static constexpr size_t MAX_INPUT_SIZE = 1024;

static logging::Logger logger = logging::getLogger("concord.diagnostics");

// Returns a successfully read line as a string.
// Throws a std::runtime_error on error.
inline std::string readline(int sock) {
  static thread_local std::array<char, MAX_INPUT_SIZE> buf;
  buf.fill(0);
  size_t count = 0;
  while (true) {
    if (count == MAX_INPUT_SIZE) {
      throw std::runtime_error("Request exceeded max size: " + std::to_string(MAX_INPUT_SIZE));
    }

    const auto read_rv = read(sock, buf.data() + count, buf.size() - count);
    LOG_DEBUG(logger, KVLOG(read_rv, count));
    if (read_rv <= 0) {
      throw std::runtime_error("diagnostics server read failed: " + errnoString(errno));
    }
    count += read_rv;

    // Check to see if we have a complete command
    auto it = std::find(buf.begin(), buf.end(), '\n');
    if (it != buf.end()) {
      return std::string(buf.begin(), it);
    }
    LOG_DEBUG(logger, "More data to read. Got: " << std::string(buf.begin(), buf.begin() + count));
  }
}

inline void handleRequest(Registrar& registrar, int sock) {
  try {
    LOG_DEBUG(logger, "Handle Diagnostics Request");
    const auto cmd = readline(sock);
    LOG_DEBUG(logger, "Command: " << cmd);
    std::stringstream ss(cmd);
    std::vector<std::string> tokens;
    std::string token;
    while (std::getline(ss, token, ' ')) {
      tokens.push_back(token);
    }
    LOG_DEBUG(logger, "Running command");
    std::string output = run(tokens, registrar);
    if (write(sock, output.data(), output.size()) < 0) {
      LOG_WARN(logger, "Failed to write to client socket: " << errnoString(errno));
    }
    LOG_DEBUG(logger, "Command completed");
    close(sock);
  } catch (const std::exception& e) {
    std::string out = std::string("Error: ") + e.what() + "\n";
    LOG_WARN(logger, out);
    if (write(sock, out.data(), out.size()) < 0) {
      LOG_WARN(logger, "Failed to write to client socket: " << errnoString(errno));
    }
    close(sock);
  }
  LOG_DEBUG(logger, "Finished handling diagnostics request");
}

// Each request creates a separate connection and spawns a thread.
//
// The purposes behind this decision are simplicity and expediency. If performance becomes a problem
// we can maintain persistent connections and/or create a thread pool. We can also switch to using
// async connections via boost ASIO if necessary, although this seems extremely heavy handed for the
// use case.
class Server {
  bool setTimeOut(int fd, time_t seconds = 1) {
    struct timeval timeout;
    timeout.tv_sec = seconds;
    timeout.tv_usec = 0;
    if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (char*)&timeout, sizeof(timeout)) < 0) {
      LOG_ERROR(logger, "Diagnostics Server failed to set read timeout\n");
      return false;
    }

    if (setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (char*)&timeout, sizeof(timeout)) < 0) {
      LOG_ERROR(logger, "Diagnostics Server failed to set write timeout\n");
      return false;
    }
    return true;
  }

 public:
  ~Server() {
    LOG_INFO(logger, "Diagnostics Server being destroyed.");
    stop();
  }

  void start(Registrar& registrar, in_addr_t host, uint16_t port) {
    shutdown_.store(false);
    listen_thread_ = std::thread([this, &registrar, host, port]() {
      LOG_INFO(logger, "Running diagnostics server main thread");
      if (listen(host, port) == -1) {
        LOG_ERROR(logger, "Failed to listen to incoming requests");
        return;
      }

      if (!setTimeOut(listen_sock_)) {
        LOG_ERROR(logger, "Diagnostics Server will not start.");
        return;
      }

      while (!shutdown_.load()) {
        int sock = accept(listen_sock_, NULL, NULL);
        if (sock < 0) {
          if (errno == EAGAIN || errno == EINTR) {
            LOG_WARN(logger, "DEBUG: TIMEOUT: " << errnoString(errno));
            continue;
          } else {
            LOG_WARN(logger, "Failed to accept connection: " << errnoString(errno));
            continue;
          }
        }
        if (setTimeOut(sock)) {
          handleRequest(registrar, sock);
        }
      }
    });
  }

  void stop() {
    if (!shutdown_) {
      LOG_INFO(logger, "Shutting down diagnostics server main thread.");
      shutdown_.store(true);
      listen_thread_.join();
      LOG_INFO(logger, "Diagnostics server main thread exited");
    }
  };

 private:
  int listen(in_addr_t host, uint16_t port) {
    listen_sock_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_sock_ < 0) {
      LOG_ERROR(logger, "couldn't retrieve a socket FD, shutting down the server");
      return -1;
    }
    bzero(&servaddr_, sizeof(servaddr_));
    servaddr_.sin_family = AF_INET;
    servaddr_.sin_addr.s_addr = htonl(host);
    servaddr_.sin_port = htons(port);
    int enable = 1;
    if (setsockopt(listen_sock_, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable))) {
      LOG_ERROR(logger, "Failed to set listen socket options: " << errnoString(errno));
      return -1;
    }
    if (bind(listen_sock_, (sockaddr*)&servaddr_, sizeof(servaddr_))) {
      LOG_ERROR(logger, "Failed to bind listen socket: " << errnoString(errno));
      return -1;
    }
    if (::listen(listen_sock_, BACKLOG)) {
      LOG_ERROR(logger, "Failed to listen for connections: " << errnoString(errno));
      return -1;
    }
    LOG_INFO(logger, "Diagnostics server listening on port " << port);
    listening_ = true;
    return 0;
  }

  int listen_sock_;
  sockaddr_in servaddr_;
  std::thread listen_thread_;
  std::atomic<bool> shutdown_{false};

 public:
  // Used for testing
  std::atomic<bool> listening_{false};
};

}  // namespace concord::diagnostics
