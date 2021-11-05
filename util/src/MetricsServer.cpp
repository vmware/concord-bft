// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include <string.h>
#include <unistd.h>
#include <exception>
#include <iostream>
#include <arpa/inet.h>

#include "MetricsServer.hpp"
#include "errnoString.hpp"

namespace concordMetrics {

void Server::Start() {
  if ((sock_ = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    LOG_FATAL(logger_, "Error creating UDP socket");
    std::terminate();
  }

  struct sockaddr_in servaddr;
  memset(&servaddr, 0, sizeof(servaddr));
  servaddr.sin_family = AF_INET;
  servaddr.sin_addr.s_addr = INADDR_ANY;
  servaddr.sin_port = htons(listenPort_);

  if (bind(sock_, (const struct sockaddr*)&servaddr, sizeof(servaddr)) < 0) {
    char addr[INET_ADDRSTRLEN];
    if (inet_ntop(AF_INET, &servaddr.sin_addr.s_addr, addr, INET_ADDRSTRLEN)) {
      LOG_FATAL(logger_,
                "Error binding UDP socket: IP=" << addr << ", Port=" << listenPort_
                                                << ", errno=" << concordUtils::errnoString(errno));
    } else {
      LOG_FATAL(logger_,
                "Error binding UDP socket: IP="
                    << "unknown"
                    << ", Port=" << listenPort_ << ", errno=" << concordUtils::errnoString(errno));
    }
    std::terminate();
  }

  running_lock_.lock();
  running_ = true;
  running_lock_.unlock();

  auto recvThread = std::thread(&Server::RecvLoop, &*this);
  std::swap(thread_, recvThread);
}

void Server::Stop() {
  running_lock_.lock();
  running_ = false;
  running_lock_.unlock();

  // This will cause `recvfrom` to error in `RecvLoop` and therefore allow it
  // to check for running_ = false without requiring a timeout on the socket.
  shutdown(sock_, SHUT_RDWR);

  // Wait for the recvLoop thread to stop
  if (thread_.joinable()) thread_.join();
}

void Server::RecvLoop() {
  int len = 0;
  struct sockaddr_in cliaddr;
  memset(&cliaddr, 0, sizeof(cliaddr));

  while (1) {
    running_lock_.lock();
    if (!running_) {
      running_lock_.unlock();
      close(sock_);
      return;
    }
    running_lock_.unlock();

    struct timeval timeout;
    timeout.tv_sec = 1;
    timeout.tv_usec = 0;

    if (setsockopt(sock_, SOL_SOCKET, SO_RCVTIMEO, (char*)&timeout, sizeof(timeout)) < 0) {
      LOG_ERROR(logger_, "Failed to set socket timeout!" << concordUtils::errnoString(errno));
    }

    socklen_t addrlen = sizeof(cliaddr);
    len = recvfrom(sock_, buf_, MAX_MSG_SIZE, 0, (sockaddr*)&cliaddr, &addrlen);

    if (len < 0) {
      if (errno != EAGAIN) {
        LOG_ERROR(logger_, "Failed to recv msg: " << concordUtils::errnoString(errno));
      }

      continue;
    }

    if (addrlen == 0) {
      // This may happen after a shutdown on the socket which causes 0 bytes
      // to be returned with empty address (probably platform dependent).
      // In this case we simply continue and recheck if we're still running.
      //
      // Note: If zero bytes are received from a valid address (if at all possible)
      // we go to the next check and send an error as usual.
      LOG_WARN(logger_, "Empty peer address. Received " << len << " bytes.");
      continue;
    }

    if (buf_[0] != kRequest || len != sizeof(Header)) {
      LOG_WARN(logger_, "Received invalid request");
      sendError(&cliaddr, addrlen);
      continue;
    }

    std::string json = aggregator_->ToJson();

    if (json.size() > MAX_MSG_SIZE - sizeof(Header)) {
      LOG_FATAL(logger_, "Aggregator data too large to be transmitted!");
      std::terminate();
    }

    sendReply(json, &cliaddr, addrlen);
  }
}

void Server::sendReply(std::string data, sockaddr_in* cliaddr, socklen_t addrlen) {
  buf_[0] = kReply;
  memcpy(buf_ + sizeof(Header), data.data(), data.size());
  auto len = sendto(sock_, buf_, data.size() + sizeof(Header), 0, (const struct sockaddr*)cliaddr, addrlen);
  if (len < 0) {
    LOG_ERROR(logger_, "Failed to send reply msg: " << concordUtils::errnoString(errno));
  }
}

void Server::sendError(sockaddr_in* cliaddr, socklen_t addrlen) {
  const char* msg = "Invalid Request";
  auto msglen = strlen(msg);

  buf_[0] = kError;
  memcpy(buf_ + sizeof(Header), msg, msglen);
  auto len = sendto(sock_, buf_, msglen + sizeof(Header), 0, (const struct sockaddr*)cliaddr, addrlen);
  if (len < 0) {
    LOG_ERROR(logger_, "Failed to send error msg: " << concordUtils::errnoString(errno));
  }
}

}  // namespace concordMetrics
