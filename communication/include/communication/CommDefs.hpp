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
#include "secret_data.h"

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
                 const std::string &host,
                 uint16_t port,
                 uint32_t bufLength,
                 NodeMap _nodes,
                 NodeNum _selfId,
                 UPDATE_CONNECTIVITY_FN _statusCallback = nullptr)
      : commType{type},
        listenHost{host},
        listenPort{port},
        bufferLength{bufLength},
        nodes{std::move(_nodes)},
        statusCallback{std::move(_statusCallback)},
        selfId{_selfId} {}

  virtual ~BaseCommConfig() = default;
};

struct PlainUdpConfig : BaseCommConfig {
  PlainUdpConfig(const std::string &host,
                 uint16_t port,
                 uint32_t bufLength,
                 NodeMap _nodes,
                 NodeNum _selfId,
                 UPDATE_CONNECTIVITY_FN _statusCallback = nullptr)
      : BaseCommConfig(
            CommType::PlainUdp, host, port, bufLength, std::move(_nodes), _selfId, std::move(_statusCallback)) {}
};

struct PlainTcpConfig : BaseCommConfig {
  int32_t maxServerId;

  PlainTcpConfig(const std::string &host,
                 uint16_t port,
                 uint32_t bufLength,
                 NodeMap _nodes,
                 int32_t _maxServerId,
                 NodeNum _selfId,
                 UPDATE_CONNECTIVITY_FN _statusCallback = nullptr)
      : BaseCommConfig(
            CommType::PlainTcp, host, port, bufLength, std::move(_nodes), _selfId, std::move(_statusCallback)),
        maxServerId{_maxServerId} {}
};

struct TlsTcpConfig : PlainTcpConfig {
  std::string certificatesRootPath;

  // set specific suite or list of suites, as described in OpenSSL
  // https://www.openssl.org/docs/man1.1.1/man1/ciphers.html
  std::string cipherSuite;

  std::optional<concord::secretsmanager::SecretData> secretData;

  TlsTcpConfig(const std::string &host,
               uint16_t port,
               uint32_t bufLength,
               NodeMap _nodes,
               int32_t _maxServerId,
               NodeNum _selfId,
               const std::string &certRootPath,
               const std::string &ciphSuite,
               UPDATE_CONNECTIVITY_FN _statusCallback = nullptr,
               std::optional<concord::secretsmanager::SecretData> decryptionSecretData = std::nullopt)
      : PlainTcpConfig(host, port, bufLength, std::move(_nodes), _maxServerId, _selfId, std::move(_statusCallback)),
        certificatesRootPath{certRootPath},
        cipherSuite{ciphSuite},
        secretData{std::move(decryptionSecretData)} {
    commType = CommType::TlsTcp;
  }
};

class PlainUDPCommunication : public ICommunication {
 public:
  static PlainUDPCommunication *create(const PlainUdpConfig &config);

  int getMaxMessageSize() override;
  int start() override;
  int stop() override;
  bool isRunning() const override;
  ConnectionStatus getCurrentConnectionStatus(NodeNum node) override;

  int send(NodeNum destNode, std::vector<uint8_t> &&msg) override;
  std::set<NodeNum> send(std::set<NodeNum> dests, std::vector<uint8_t> &&msg) override;

  void setReceiver(NodeNum receiverNum, IReceiver *receiver) override;

  void dispose(NodeNum i) override{};
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
  int start() override;
  int stop() override;
  bool isRunning() const override;
  ConnectionStatus getCurrentConnectionStatus(NodeNum node) override;

  int send(NodeNum destNode, std::vector<uint8_t> &&msg) override;
  std::set<NodeNum> send(std::set<NodeNum> dests, std::vector<uint8_t> &&msg) override;

  void setReceiver(NodeNum receiverNum, IReceiver *receiver) override;

  void dispose(NodeNum i) override {}
  ~PlainTCPCommunication() override;

 private:
  class PlainTcpImpl;
  PlainTcpImpl *_ptrImpl = nullptr;

  explicit PlainTCPCommunication(const PlainTcpConfig &config);
};

namespace tls {
class Runner;
}

class TlsTCPCommunication : public ICommunication {
 public:
  static TlsTCPCommunication *create(const TlsTcpConfig &config);

  int getMaxMessageSize() override;
  int start() override;
  int stop() override;
  bool isRunning() const override;
  ConnectionStatus getCurrentConnectionStatus(NodeNum node) override;

  int send(NodeNum destNode, std::vector<uint8_t> &&msg) override;
  std::set<NodeNum> send(std::set<NodeNum> dests, std::vector<uint8_t> &&msg) override;

  void setReceiver(NodeNum receiverNum, IReceiver *receiver) override;

  void dispose(NodeNum i) override;
  ~TlsTCPCommunication() override;

 private:
  TlsTcpConfig config_;
  std::unique_ptr<tls::Runner> runner_;

  explicit TlsTCPCommunication(const TlsTcpConfig &config);
};

}  // namespace bft::communication
