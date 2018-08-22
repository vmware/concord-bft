// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include "CommFactory.hpp"
#include <string>

using namespace std;
using namespace bftEngine;

ICommunication *
CommFactory::create(BaseCommConfig &config)
{
   ICommunication *res = nullptr;
   switch (config.commType)
   {
      case CommType::PlainUdp:
         res = PlainUDPCommunication::create(
                 dynamic_cast<PlainUdpConfig&>(config));
         break;
#ifdef USE_COMM_PLAIN_TCP
      case CommType::PlainTcp:
         res = PlainTCPCommunication::create(
                 dynamic_cast<PlainTcpConfig&>(config));
         break;
#endif
#ifdef USE_COMM_TLS_TCP
      case CommType::TlsTcp:
         res = TlsTCPCommunication::create(
                 dynamic_cast<TlsTcpConfig&>(config));
         break;
#endif
   }

   return res;
}

