// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <string>
#include <utility>

#include "communication/CommFactory.hpp"
#include "Logger.hpp"

namespace bft::communication {

logging::Logger CommFactory::_logger = logging::getLogger("communication.factory");

ICommunication *CommFactory::create(const BaseCommConfig &config) {
  ICommunication *res = nullptr;
  switch (config.commType) {
    case CommType::PlainUdp:
      LOG_INFO(_logger,
               "Using PlainUDP: "
                   << "Host=" << config.listenHost << ", Port=" << config.listenPort);
      res = PlainUDPCommunication::create(dynamic_cast<const PlainUdpConfig &>(config));
      break;
    case CommType::SimpleAuthUdp:
      break;
    case CommType::PlainTcp:
#ifdef USE_COMM_PLAIN_TCP
      LOG_INFO(_logger,
               "Using PlainTCP: "
                   << "Host=" << config.listenHost << ", Port=" << config.listenPort);
      res = PlainTCPCommunication::create(dynamic_cast<const PlainTcpConfig &>(config));
#endif
      break;
    case CommType::SimpleAuthTcp:
      break;
    case CommType::TlsTcp:
#ifdef USE_COMM_TLS_TCP
      LOG_INFO(_logger,
               "Using TlsTCP: "
                   << "Host=" << config.listenHost << ", Port=" << config.listenPort);
      res = TlsTCPCommunication::create(dynamic_cast<const TlsTcpConfig &>(config));
#endif
      break;
  }

  return res;
}

}  // namespace bft::communication
