// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
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

#include "CommFactory.hpp"
#include "Logging.hpp"

using bftEngine::CommFactory;
using bftEngine::CommType;
using bftEngine::ICommunication;
using bftEngine::PlainTcpConfig;
using bftEngine::PlainUdpConfig;
using bftEngine::TlsTcpConfig;

concordlogger::Logger CommFactory::_logger =
   concordlogger::Logger::getLogger("comm-factory");

ICommunication*
CommFactory::create(const BaseCommConfig &config) {
  ICommunication *res = nullptr;
  switch (config.commType) {
  case CommType::PlainUdp:
    LOG_INFO(_logger, "Using PlainUDP: " << "IP=" << config.listenIp <<
                      ", Port=" << config.listenPort);
    res = PlainUDPCommunication::create(
      dynamic_cast<const PlainUdpConfig&>(config));
    break;
  case CommType::SimpleAuthUdp:
    break;
  case CommType::PlainTcp:
#ifdef USE_COMM_PLAIN_TCP
    LOG_INFO(_logger, "Using PlainTCP: " << "IP=" << config.listenIp <<
                      ", Port=" << config.listenPort);
    res = PlainTCPCommunication::create(
      dynamic_cast<const PlainTcpConfig&>(config));
#endif
    break;
  case CommType::SimpleAuthTcp:
    break;
  case CommType::TlsTcp:
#ifdef USE_COMM_TLS_TCP
    LOG_INFO(_logger, "Using TlsTCP: " << "IP=" << config.listenIp <<
                      ", Port=" << config.listenPort);
    res = TlsTCPCommunication::create(
      dynamic_cast<const TlsTcpConfig&>(config));
#endif
    break;
  }

  return res;
}
