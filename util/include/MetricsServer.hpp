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

#include <stdint.h>
#include <memory>
#include <thread>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>

#include "Logging.hpp"
#include "Metrics.hpp"

#ifndef CONCORD_BFT_METRICS_SERVER_HPP
#define CONCORD_BFT_METRICS_SERVER_HPP

#define MAX_MSG_SIZE 64 * 1024  // 64k

namespace concordMetrics {

// A UDP server that returns aggregated metrics
class Server {
 public:
  Server(uint16_t listenPort)
      : listenPort_{listenPort},
        logger_{concordlogger::Logger::getLogger("metrics-server")},
        running_{false},
        aggregator_{std::make_shared<Aggregator>()} {}

  void Start();
  void Stop();

  std::shared_ptr<Aggregator> GetAggregator() { return aggregator_; }

 private:
  uint16_t listenPort_;
  concordlogger::Logger logger_;
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

#endif  // CONCORD_BFT_METRICS_SERVER_HPP
