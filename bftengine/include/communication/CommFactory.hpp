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

#ifndef BYZ_COMMFACTORY_HPP
#define BYZ_COMMFACTORY_HPP

#include "CommImpl.hpp"
#include <type_traits>

namespace bftEngine
{
   template<CommType T>
   struct config_type{

   };

   template<>
   struct config_type<PlainUdp>{
      using type = PlainUdpConfig;
   };

   template<>
   struct config_type<PlainTcp>{
      using type = PlainTcpConfig;
   };

   template<>
   struct config_type<TlsTcp>{
      using type = TlsTcpConfig;
   };

   PlainUdpConfig
   create_config_impl(uint32_t maxMsgSize,
                      NodeMap nodes,
                      uint16_t port,
                      std::string ip);

   PlainTcpConfig
   create_config_impl(uint32_t maxMsgSize,
                      NodeMap nodes,
                      uint16_t port,
                      std::string ip,
                      int32_t maxServerId,
                      NodeNum selfId);

   TlsTcpConfig
   create_config_impl(uint32_t maxMsgSize,
                      NodeMap nodes,
                      uint16_t port,
                      std::string ip,
                      int32_t maxServerId,
                      NodeNum selfId,
                      std::string certRootPath);

   class CommFactory
   {
   public:
      static ICommunication *
      create(BaseCommConfig &config);

      template<CommType T, typename... Args>
      static typename config_type<T>::type
      create_config(Args... args);
   };
}

#endif //BYZ_COMMFACTORY_HPP
