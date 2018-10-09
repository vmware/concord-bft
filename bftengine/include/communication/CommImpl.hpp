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

#ifndef BYZ_COMMIMPL_HPP
#define BYZ_COMMIMPL_HPP

#if defined(_WIN32)
#include <WinSock2.h>
#include <ws2def.h>
#else
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#include <string>
#include <stdint.h>
#include <unordered_map>
#include "ICommunication.hpp"

typedef struct sockaddr_in Addr;
typedef std::unordered_map <NodeNum, std::tuple<std::string,uint16_t>> NodeMap;

namespace bftEngine
{
   enum CommType
   {
      PlainUdp,
      SimpleAuthUdp,
      PlainTcp,
      SimpleAuthTcp,
      TlsTcp
   };

   struct BaseCommConfig
   {
      CommType  commType;
      std::string listenIp;
      uint16_t listenPort;
      uint32_t bufferLength;
      NodeMap nodes;

      BaseCommConfig(CommType type,
                     std::string ip,
                     uint16_t port,
                     uint32_t bufLength,
                     NodeMap _nodes
                     ) :
                        commType{type},
                        listenIp{std::move(ip)},
                        listenPort{port},
                        bufferLength{bufLength},
                        nodes{std::move(_nodes)}
      {
      }

      virtual ~BaseCommConfig()
      {}
   };

   struct PlainUdpConfig : BaseCommConfig
   {
      PlainUdpConfig(std::string ip,
                     uint16_t port,
                     uint32_t bufLength,
                     NodeMap _nodes) :
                        BaseCommConfig(CommType::PlainUdp,
                                       std::move(ip),
                                       port,
                                       bufLength,
                                       std::move(_nodes))
      {
      }
   };

   struct PlainTcpConfig : BaseCommConfig
   {
      int32_t maxServerId;
      NodeNum selfId;

      PlainTcpConfig(std::string ip,
                     uint16_t port,
                     uint32_t bufLength,
                     NodeMap _nodes,
                     int32_t _maxServerId,
                     NodeNum _selfId) :
                        BaseCommConfig(CommType::PlainTcp,
                                       std::move(ip),
                                       port,
                                       bufLength,
                                       std::move(_nodes)),
                                       maxServerId{_maxServerId},
                                       selfId{_selfId}
      {
      }
   };

   struct TlsTcpConfig : PlainTcpConfig
   {
      std::string certificatesRootPath;

      TlsTcpConfig(  std::string ip,
                     uint16_t port,
                     uint32_t bufLength,
                     NodeMap _nodes,
                     int32_t _maxServerId,
                     NodeNum _selfId,
                     std::string certRootPath) :
                        PlainTcpConfig (move(ip),
                                        port,
                                        bufLength,
                                        std::move(_nodes),
                                        _maxServerId,
                                        _selfId),
                                        certificatesRootPath{move(certRootPath)}
      {
         commType = CommType::TlsTcp;
      }
   };

   class PlainUDPCommunication : public ICommunication
   {
   public:
      static PlainUDPCommunication *create(const PlainUdpConfig &config);

      int getMaxMessageSize() override;
      int Start() override;
      int Stop() override;
      bool isRunning() const override;
      ConnectionStatus getCurrentConnectionStatus(
                                    const NodeNum node) const override;

      int sendAsyncMessage(const NodeNum destNode,
                                   const char *const message,
                                   const size_t messageLength) override;

      void setReceiver(NodeNum receiverNum,
                               IReceiver *receiver) override;

      virtual ~PlainUDPCommunication();
   private:
      class PlainUdpImpl;

      //TODO(IG): convert to smart ptr
      PlainUdpImpl *_ptrImpl = nullptr;

      explicit PlainUDPCommunication(const PlainUdpConfig &config);
   };

   class PlainTCPCommunication : public ICommunication
   {
   public:
      static PlainTCPCommunication *create(const PlainTcpConfig &config);

      int getMaxMessageSize() override;
      int Start() override;
      int Stop() override;
      bool isRunning() const override;
      ConnectionStatus getCurrentConnectionStatus(
              const NodeNum node) const override;

      int sendAsyncMessage(const NodeNum destNode,
                                   const char *const message,
                                   const size_t messageLength) override;

      void setReceiver(NodeNum receiverNum,
                               IReceiver *receiver) override;

      virtual ~PlainTCPCommunication();
   private:
      class PlainTcpImpl;
      PlainTcpImpl *_ptrImpl = nullptr;

      explicit PlainTCPCommunication(const PlainTcpConfig &config);
   };

   class TlsTCPCommunication : public ICommunication
   {
   public:
      static TlsTCPCommunication *create(const TlsTcpConfig &config);

      int getMaxMessageSize() override;
      int Start() override;
      int Stop() override;
      bool isRunning() const override;
      ConnectionStatus getCurrentConnectionStatus(
              const NodeNum node) const override;

      int sendAsyncMessage(const NodeNum destNode,
                           const char *const message,
                           const size_t messageLength) override;

      void setReceiver(NodeNum receiverNum,
                       IReceiver *receiver) override;

      virtual ~TlsTCPCommunication();
   private:
      class TlsTcpImpl;
      TlsTcpImpl *_ptrImpl = nullptr;

      explicit TlsTCPCommunication(const TlsTcpConfig &config);
   };
}

#endif //BYZ_COMMIMPL_HPP
