// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
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

class BaseCommConfig {
 public:
  BaseCommConfig(CommType type,
                 const std::string &host,
                 uint16_t port,
                 uint32_t bufLength,
                 NodeMap nodes,
                 NodeNum selfId,
                 UPDATE_CONNECTIVITY_FN statusCallback = nullptr)
      : commType_{type},
        listenHost_{host},
        listenPort_{port},
        bufferLength_{bufLength},
        nodes_{std::move(nodes)},
        statusCallback_{std::move(statusCallback)},
        selfId_{selfId} {}

  virtual ~BaseCommConfig() = default;

 public:
  CommType commType_;
  std::string listenHost_;
  uint16_t listenPort_;
  uint32_t bufferLength_;
  NodeMap nodes_;
  UPDATE_CONNECTIVITY_FN statusCallback_;
  NodeNum selfId_;
};

class PlainUdpConfig : public BaseCommConfig {
 public:
  PlainUdpConfig(const std::string &host,
                 uint16_t port,
                 uint32_t bufLength,
                 NodeMap nodes,
                 NodeNum selfId,
                 UPDATE_CONNECTIVITY_FN statusCallback = nullptr)
      : BaseCommConfig(CommType::PlainUdp, host, port, bufLength, std::move(nodes), selfId, std::move(statusCallback)) {
  }
};

class PlainTcpConfig : public BaseCommConfig {
 public:
  PlainTcpConfig(const std::string &host,
                 uint16_t port,
                 uint32_t bufLength,
                 NodeMap nodes,
                 int32_t maxServerId,
                 NodeNum selfId,
                 UPDATE_CONNECTIVITY_FN statusCallback = nullptr)
      : BaseCommConfig(CommType::PlainTcp, host, port, bufLength, std::move(nodes), selfId, std::move(statusCallback)),
        maxServerId_{maxServerId} {}

 public:
  int32_t maxServerId_;
};

class TlsTcpConfig : public PlainTcpConfig {
 public:
  // set specific suite or list of suites, as described in OpenSSL
  // https://www.openssl.org/docs/man1.1.1/man1/ciphers.html
  TlsTcpConfig(const std::string &host,
               uint16_t port,
               uint32_t bufLength,
               NodeMap nodes,
               int32_t maxServerId,
               NodeNum selfId,
               const std::string &certRootPath,
               const std::string &cipherSuite,
               UPDATE_CONNECTIVITY_FN statusCallback = nullptr,
               std::optional<concord::secretsmanager::SecretData> decryptionSecretData = std::nullopt)
      : PlainTcpConfig(host, port, bufLength, std::move(nodes), maxServerId, selfId, std::move(statusCallback)),
        certificatesRootPath_{certRootPath},
        cipherSuite_{cipherSuite},
        secretData_{std::move(decryptionSecretData)} {
    commType_ = CommType::TlsTcp;
  }

 public:
  std::string certificatesRootPath_;
  std::string cipherSuite_;
  std::optional<concord::secretsmanager::SecretData> secretData_;
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

  void restartCommunication(NodeNum i) override{};
  ~PlainUDPCommunication() override;

 private:
  class PlainUdpImpl;

  // TODO(IG): convert to smart ptr
  PlainUdpImpl *ptrImpl_ = nullptr;

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

  void restartCommunication(NodeNum i) override {}
  ~PlainTCPCommunication() override;

 private:
  class PlainTcpImpl;
  PlainTcpImpl *ptrImpl_ = nullptr;

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

  void restartCommunication(NodeNum i) override;
  ~TlsTCPCommunication() override;

 private:
  TlsTcpConfig config_;
  std::unique_ptr<tls::Runner> runner_;

  explicit TlsTCPCommunication(const TlsTcpConfig &config);
};

}  // namespace bft::communication
