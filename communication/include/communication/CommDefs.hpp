// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#pragma once

#include <string>
#include <cstdint>
#include <memory>
#include <unordered_map>

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include "communication/ICommunication.hpp"
#include "communication/StatusInfo.h"

namespace bft::communication {
typedef struct sockaddr_in Addr;

struct NodeInfo {
  std::string host;
  std::uint16_t port;
  bool isReplica;
};

typedef std::unordered_map<NodeNum, NodeInfo> NodeMap;

enum CommType { PlainUdp, SimpleAuthUdp, PlainTcp, SimpleAuthTcp, TlsTcp };

struct BaseCommConfig {
  CommType commType;
  std::string listenHost;
  uint16_t listenPort;
  uint32_t bufferLength;
  NodeMap nodes;
  UPDATE_CONNECTIVITY_FN statusCallback;
  NodeNum selfId;

  BaseCommConfig(CommType type,
                 std::string host,
                 uint16_t port,
                 uint32_t bufLength,
                 NodeMap _nodes,
                 NodeNum _selfId,
                 UPDATE_CONNECTIVITY_FN _statusCallback = nullptr)
      : commType{type},
        listenHost{std::move(host)},
        listenPort{port},
        bufferLength{bufLength},
        nodes{std::move(_nodes)},
        statusCallback{_statusCallback},
        selfId{_selfId} {}

  virtual ~BaseCommConfig() = default;
};

struct PlainUdpConfig : BaseCommConfig {
  PlainUdpConfig(std::string host,
                 uint16_t port,
                 uint32_t bufLength,
                 NodeMap _nodes,
                 NodeNum _selfId,
                 UPDATE_CONNECTIVITY_FN _statusCallback = nullptr)
      : BaseCommConfig(
            CommType::PlainUdp, std::move(host), port, bufLength, std::move(_nodes), _selfId, _statusCallback) {}
};

struct PlainTcpConfig : BaseCommConfig {
  int32_t maxServerId;

  PlainTcpConfig(std::string host,
                 uint16_t port,
                 uint32_t bufLength,
                 NodeMap _nodes,
                 int32_t _maxServerId,
                 NodeNum _selfId,
                 UPDATE_CONNECTIVITY_FN _statusCallback = nullptr)
      : BaseCommConfig(
            CommType::PlainTcp, std::move(host), port, bufLength, std::move(_nodes), _selfId, _statusCallback),
        maxServerId{_maxServerId} {}
};

struct TlsTcpConfig : PlainTcpConfig {
  std::string certificatesRootPath;

  // set specific suite or list of suites, as described in OpenSSL
  // https://www.openssl.org/docs/man1.0.2/man1/ciphers.html
  std::string cipherSuite;

  TlsTcpConfig(std::string host,
               uint16_t port,
               uint32_t bufLength,
               NodeMap _nodes,
               int32_t _maxServerId,
               NodeNum _selfId,
               std::string certRootPath,
               std::string ciphSuite,
               UPDATE_CONNECTIVITY_FN _statusCallback = nullptr)
      : PlainTcpConfig(move(host), port, bufLength, std::move(_nodes), _maxServerId, _selfId, _statusCallback),
        certificatesRootPath{std::move(certRootPath)},
        cipherSuite{std::move(ciphSuite)} {
    commType = CommType::TlsTcp;
  }
};

class PlainUDPCommunication : public ICommunication {
 public:
  static PlainUDPCommunication *create(const PlainUdpConfig &config);

  int getMaxMessageSize() override;
  int Start() override;
  int Stop() override;
  bool isRunning() const override;
  ConnectionStatus getCurrentConnectionStatus(NodeNum node) override;

  int sendAsyncMessage(NodeNum destNode, const char *const message, size_t messageLength) override;

  void setReceiver(NodeNum receiverNum, IReceiver *receiver) override;

  ~PlainUDPCommunication() override;

 private:
  class PlainUdpImpl;

  // TODO(IG): convert to smart ptr
  PlainUdpImpl *_ptrImpl = nullptr;

  explicit PlainUDPCommunication(const PlainUdpConfig &config);
};

class PlainTCPCommunication : public ICommunication {
 public:
  static PlainTCPCommunication *create(const PlainTcpConfig &config);

  int getMaxMessageSize() override;
  int Start() override;
  int Stop() override;
  bool isRunning() const override;
  ConnectionStatus getCurrentConnectionStatus(NodeNum node) override;

  int sendAsyncMessage(NodeNum destNode, const char *const message, size_t messageLength) override;

  void setReceiver(NodeNum receiverNum, IReceiver *receiver) override;

  ~PlainTCPCommunication() override;

 private:
  class PlainTcpImpl;
  PlainTcpImpl *_ptrImpl = nullptr;

  explicit PlainTCPCommunication(const PlainTcpConfig &config);
};

class TlsTCPCommunication : public ICommunication {
 public:
  static TlsTCPCommunication *create(const TlsTcpConfig &config);

  int getMaxMessageSize() override;
  int Start() override;
  int Stop() override;
  bool isRunning() const override;
  ConnectionStatus getCurrentConnectionStatus(NodeNum node) override;

  int sendAsyncMessage(NodeNum destNode, const char *const message, size_t messageLength) override;

  void setReceiver(NodeNum receiverNum, IReceiver *receiver) override;

  ~TlsTCPCommunication() override;

 private:
  class TlsTcpImpl;
  std::shared_ptr<TlsTcpImpl> _ptrImpl = nullptr;

  explicit TlsTCPCommunication(const TlsTcpConfig &config);
};

}  // namespace bft::communication
