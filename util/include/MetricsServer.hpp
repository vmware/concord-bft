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

#pragma once

#include <stdint.h>
#include <memory>
#include <thread>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>

#include "Logger.hpp"
#include "Metrics.hpp"

#define MAX_MSG_SIZE (64 * 1024)  // 64k

namespace concordMetrics {

const uint8_t kRequest = 0;
const uint8_t kReply = 1;
const uint8_t kError = 2;

#pragma pack(push, 1)
// All requests are solely Headers with msg_type_ set to kRequest. Replies are
// JSON strings preceded by a Header with msg_type set to kReply or kError.
// Since we are using UDP, the entire message will always be included, so no
// need to worry about framing. We can always change the protocol if we decide
// to enhance the Metric server later on or move to a different transport.
struct Header {
  uint8_t msg_type_;
  uint64_t seq_num_;
};
#pragma pack(pop)

// A UDP server that returns aggregated metrics
class Server {
 public:
  Server(uint16_t listenPort)
      : listenPort_{listenPort},
        logger_{logging::getLogger("metrics-server")},
        running_{false},
        aggregator_{std::make_shared<Aggregator>()} {}

  void Start();
  void Stop();

  std::shared_ptr<Aggregator> GetAggregator() { return aggregator_; }

 private:
  uint16_t listenPort_;
  logging::Logger logger_;
  bool running_;
  std::mutex running_lock_;

  std::shared_ptr<Aggregator> aggregator_;
  std::thread thread_;

  int sock_;
  uint8_t buf_[MAX_MSG_SIZE];

  void RecvLoop();
  void sendReply(std::string data, sockaddr_in* cliaddr, socklen_t addrlen);
  void sendError(sockaddr_in* cliaddr, socklen_t addrlen);
};

}  // namespace concordMetrics
