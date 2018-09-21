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


using bftEngine::CommFactory;
using bftEngine::CommType;
using bftEngine::config_type;
using bftEngine::ICommunication;
using bftEngine::PlainTcpConfig;
using bftEngine::PlainUdpConfig;
using bftEngine::TlsTcpConfig;

PlainUdpConfig
create_config_impl(uint32_t maxMsgSize,
                   NodeMap nodes,
                   uint16_t port,
                   std::string ip) {
  PlainUdpConfig config(move(ip), port, maxMsgSize, move(nodes));
  return config;
}

PlainTcpConfig
create_config_impl(uint32_t maxMsgSize,
                   NodeMap nodes,
                   uint16_t port,
                   std::string ip,
                   int32_t maxServerId,
                   NodeNum selfId) {
  PlainTcpConfig config(std::move(ip),
                        port,
                        maxMsgSize,
                        std::move(nodes),
                        maxServerId,
                        selfId);
  return config;
}

TlsTcpConfig
create_config_impl(uint32_t maxMsgSize,
                   NodeMap nodes,
                   uint16_t port,
                   std::string ip,
                   int32_t maxServerId,
                   NodeNum selfId,
                   std::string certRootPath) {
  TlsTcpConfig config(std::move(ip),
                      port,
                      maxMsgSize,
                      std::move(nodes),
                      maxServerId,
                      selfId,
                      certRootPath);
  return config;
}

ICommunication*
CommFactory::create(const BaseCommConfig &config) {
  ICommunication *res = nullptr;
  switch (config.commType) {
  case CommType::PlainUdp:
    res = PlainUDPCommunication::create(
      dynamic_cast<const PlainUdpConfig&>(config));
    break;
  case CommType::SimpleAuthUdp:
    break;
  case CommType::PlainTcp:
#ifdef USE_COMM_PLAIN_TCP
    res = PlainTCPCommunication::create(
      dynamic_cast<const PlainTcpConfig&>(config));
#endif
    break;
  case CommType::SimpleAuthTcp:
    break;
  case CommType::TlsTcp:
#ifdef USE_COMM_TLS_TCP
    res = TlsTCPCommunication::create(
      dynamic_cast<const TlsTcpConfig&>(config));
#endif
    break;
  }

  return res;
}

template<CommType T, typename... Args>
static typename config_type<T>::type
create_config(Args... args) {
  auto res = create_config_impl(args...);
  return res;
}
