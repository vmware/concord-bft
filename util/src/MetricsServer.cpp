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
#include <iostream>

#include "MetricsServer.hpp"

// The MetricServer only handles a single type of request, that requests a JSON
// metrics blob. The message format is just a single 8-bit 0 on the wire. We can
// always change the protocol later if needed.
const uint8_t kRequest = 0;

// The MetricsServer only returns a single type of response. The response is
// just an 8-bit 1 on the wire followed by a JSON string containing all metrics
// for all components. Since we are using UDP, the entire message will always be
// included, so no need to worry about framing. We can always change the
// protocol if we decide to enhance the Metric server later on or move to a
// different transport.
const uint8_t kReply = 1;
const uint8_t kError = 2;

namespace concordMetrics {

void Server::Start() {
  if ((sock_ = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    LOG_FATAL(logger_, "Error creating UDP socket");
    exit(1);
  }

  struct sockaddr_in servaddr;
  memset(&servaddr, 0, sizeof(servaddr));
  servaddr.sin_family = AF_INET;
  servaddr.sin_addr.s_addr = INADDR_ANY;
  servaddr.sin_port = htons(listenPort_);

  if (bind(sock_, (const struct sockaddr*)&servaddr, sizeof(servaddr)) < 0) {
    LOG_FATAL(logger_,
              "Error binding UDP socket: IP=" << servaddr.sin_addr.s_addr
                                              << ", Port=" << servaddr.sin_port
                                              << ", errno=" << strerror(errno));
    exit(1);
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
  close(sock_);

  // Wait for the recvLoop thread to stop
  thread_.join();
}

void Server::RecvLoop() {
  int len = 0;
  struct sockaddr_in cliaddr;
  memset(&cliaddr, 0, sizeof(cliaddr));

  while (1) {
    running_lock_.lock();
    if (!running_) {
      running_lock_.unlock();
      return;
    }
    running_lock_.unlock();

    socklen_t addrlen = sizeof(cliaddr);
    len = recvfrom(sock_, buf_, MAX_MSG_SIZE, 0, (sockaddr*)&cliaddr, &addrlen);

    if (len < 0) {
      LOG_ERROR(logger_, "Failed to recv msg: " << strerror(errno));
      continue;
    }

    if (buf_[0] != kRequest || len != 1) {
      LOG_WARN(logger_, "Received invalid request");
      sendError(&cliaddr, addrlen);
      continue;
    }

    std::string json = aggregator_->ToJson();

    if (json.size() > MAX_MSG_SIZE - 1) {
      LOG_FATAL(logger_, "Aggregator data too large to be transmitted!");
      exit(1);
    }

    sendReply(json, &cliaddr, addrlen);
  }
}

void Server::sendReply(std::string data,
                       sockaddr_in* cliaddr,
                       socklen_t addrlen) {
  buf_[0] = kReply;
  memcpy(buf_ + 1, data.data(), data.size());
  auto len = sendto(sock_,
                    buf_,
                    data.size() + 1,
                    0,
                    (const struct sockaddr*)cliaddr,
                    addrlen);
  if (len < 0) {
    LOG_ERROR(logger_, "Failed to send reply msg: " << strerror(errno));
  }
}

void Server::sendError(sockaddr_in* cliaddr, socklen_t addrlen) {
  const char* msg = "Invalid Request";
  auto msglen = strlen(msg);

  buf_[0] = kError;
  memcpy(buf_ + 1, msg, msglen);
  auto len = sendto(
      sock_, buf_, msglen + 1, 0, (const struct sockaddr*)cliaddr, addrlen);
  if (len < 0) {
    LOG_ERROR(logger_, "Failed to send error msg: " << strerror(errno));
  }
}

}  // namespace concordMetrics
