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
#include <unordered_set>
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include "communication/ICommunication.hpp"
#include "communication/StatusInfo.h"
#include "secret_data.h"
#include "assertUtils.hpp"

namespace bft::communication {
typedef struct sockaddr_in Addr;

struct NodeInfo {
  std::string host;
  std::uint16_t port;
  bool isReplica;
};

#pragma pack(push, 1)
struct Header {
  uint32_t msg_size;
  NodeNum endpoint_num;
};
#pragma pack(pop)

static constexpr size_t MSG_HEADER_SIZE = sizeof(Header);
typedef std::unordered_map<NodeNum, NodeInfo> NodeMap;

enum CommType { PlainUdp, SimpleAuthUdp, PlainTcp, SimpleAuthTcp, TlsTcp, TlsMultiplex };
const static std::unordered_map<CommType, const char *> commTypeToName = {
    {PlainUdp, "PlainUdp"},
    {SimpleAuthUdp, "SimpleAuthUdp"},
    {PlainTcp, "PlainTcp"},
    {SimpleAuthTcp, "SimpleAuthTcp"},
    {TlsTcp, "TlsTcp"},
    {TlsMultiplex, "TlsMultiplex"},
};

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
        nodes_{nodes},
        statusCallback_{std::move(statusCallback)},
        selfId_{selfId} {
    const auto myNode = nodes_.find(selfId_);
    // A replica node is always a part of the configuration. A client node is presented only in replicas' configuration.
    if (myNode != nodes_.end()) amIReplica_ = myNode->second.isReplica;
  }

  bool isClient(NodeNum nodeNum) const {
    const auto node = nodes_.find(nodeNum);
    if (node != nodes_.end()) return !(node->second.isReplica);
    // A client node is presented only in replicas' configuration.
    return false;
  }
  virtual ~BaseCommConfig() = default;

 public:
  CommType commType_;
  std::string listenHost_;
  uint16_t listenPort_;
  uint32_t bufferLength_;
  NodeMap nodes_;
  UPDATE_CONNECTIVITY_FN statusCallback_;
  NodeNum selfId_;
  bool amIReplica_ = false;
};

class PlainUdpConfig : public BaseCommConfig {
 public:
  PlainUdpConfig(const std::string &host,
                 uint16_t port,
                 uint32_t bufLength,
                 NodeMap nodes,
                 NodeNum selfId,
                 UPDATE_CONNECTIVITY_FN statusCallback = nullptr)
      : BaseCommConfig(CommType::PlainUdp, host, port, bufLength, nodes, selfId, std::move(statusCallback)) {}
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
      : BaseCommConfig(CommType::PlainTcp, host, port, bufLength, nodes, selfId, std::move(statusCallback)),
        maxServerId_{maxServerId} {}

 public:
  int32_t maxServerId_;
};

class TlsTcpConfig : public PlainTcpConfig {
 public:
  // Set specific suite or list of suites, as described in OpenSSL
  // https://www.openssl.org/docs/man1.1.1/man1/ciphers.html
  TlsTcpConfig(const std::string &host,
               uint16_t port,
               uint32_t bufLength,
               NodeMap nodes,
               int32_t maxServerId,
               NodeNum selfId,
               const std::string &certRootPath,
               const std::string &cipherSuite,
               bool useUnifiedCerts,
               UPDATE_CONNECTIVITY_FN statusCallback = nullptr,
               std::optional<concord::secretsmanager::SecretData> decryptionSecretData = std::nullopt)
      : PlainTcpConfig(host, port, bufLength, nodes, maxServerId, selfId, std::move(statusCallback)),
        certificatesRootPath_{certRootPath},
        cipherSuite_{cipherSuite},
        useUnifiedCertificates_{useUnifiedCerts},
        secretData_{std::move(decryptionSecretData)} {
    commType_ = CommType::TlsTcp;
  }

 public:
  std::string certificatesRootPath_;
  std::string cipherSuite_;
  bool useUnifiedCertificates_;
  std::optional<concord::secretsmanager::SecretData> secretData_;
};

class TlsMultiplexConfig : public TlsTcpConfig {
 public:
  TlsMultiplexConfig(const std::string &host,
                     uint16_t port,
                     uint32_t bufLength,
                     NodeMap &nodes,
                     int32_t maxServerId,
                     NodeNum selfId,
                     const std::string &certRootPath,
                     const std::string &cipherSuite,
                     bool useUnifiedCerts,
                     std::unordered_map<NodeNum, NodeNum> &endpointIdToNodeIdMap,
                     UPDATE_CONNECTIVITY_FN statusCallback = nullptr,
                     std::optional<concord::secretsmanager::SecretData> secretData = std::nullopt)
      : TlsTcpConfig(host,
                     port,
                     bufLength,
                     nodes,
                     maxServerId,
                     selfId,
                     certRootPath,
                     cipherSuite,
                     useUnifiedCerts,
                     std::move(statusCallback),
                     secretData),
        endpointIdToNodeIdMap_(endpointIdToNodeIdMap) {
    commType_ = CommType::TlsMultiplex;
  }

 public:
  std::unordered_map<NodeNum, NodeNum> endpointIdToNodeIdMap_;
};

class PlainUDPCommunication : public ICommunication {
 public:
  static PlainUDPCommunication *create(const PlainUdpConfig &config);

  int getMaxMessageSize() override;
  int start() override;
  int stop() override;
  bool isRunning() const override;
  ConnectionStatus getCurrentConnectionStatus(NodeNum node) override;
  int send(NodeNum destNode, std::vector<uint8_t> &&msg, NodeNum endpointNum) override;
  std::set<NodeNum> send(std::set<NodeNum> dests, std::vector<uint8_t> &&msg, NodeNum srcEndpointNum) override;
  void setReceiver(NodeNum receiverNum, IReceiver *receiver) override;
  void restartCommunication(NodeNum i) override{};
  ~PlainUDPCommunication() override;

 private:
  class PlainUdpImpl;

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
  int send(NodeNum destNode, std::vector<uint8_t> &&msg, NodeNum endpointNum) override;
  std::set<NodeNum> send(std::set<NodeNum> dests, std::vector<uint8_t> &&msg, NodeNum srcEndpointNum) override;
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
  int send(NodeNum destNode, std::vector<uint8_t> &&msg, NodeNum endpointNum) override;
  std::set<NodeNum> send(std::set<NodeNum> dests, std::vector<uint8_t> &&msg, NodeNum srcEndpointNum) override;
  void setReceiver(NodeNum receiverNum, IReceiver *receiver) override;
  void restartCommunication(NodeNum i) override;
  ~TlsTCPCommunication() override;

 protected:
  logging::Logger logger_;
  TlsTcpConfig *config_;
  std::unique_ptr<tls::Runner> runner_;

  explicit TlsTCPCommunication(const TlsTcpConfig &config);
};

class TlsMultiplexCommunication : public TlsTCPCommunication {
 public:
  static TlsMultiplexCommunication *create(const TlsMultiplexConfig &config);

  int start() override;
  NodeNum getConnectionByEndpointNum(NodeNum destNode, NodeNum endpointNum);
  ConnectionStatus getCurrentConnectionStatus(NodeNum nodeNum) override;
  void setReceiver(NodeNum receiverNum, IReceiver *receiver) override;
  int send(NodeNum destNode, std::vector<uint8_t> &&msg, NodeNum endpointNum) override;
  std::set<NodeNum> send(std::set<NodeNum> dests, std::vector<uint8_t> &&msg, NodeNum srcEndpointNum) override;
  virtual ~TlsMultiplexCommunication() = default;

 private:
  class TlsMultiplexReceiver : public IReceiver {
   public:
    TlsMultiplexReceiver(std::shared_ptr<TlsMultiplexConfig> multiplexConfig)
        : logger_(logging::getLogger("concord-bft.tls.multiplex")), multiplexConfig_(multiplexConfig) {}
    virtual ~TlsMultiplexReceiver() = default;
    void setReceiver(NodeNum receiverNum, IReceiver *receiver);
    void onNewMessage(NodeNum sourceNode,
                      const char *const message,
                      size_t messageLength,
                      NodeNum endpointNum) override;
    void onConnectionStatusChanged(NodeNum node, ConnectionStatus newStatus) override;

   private:
    logging::Logger logger_;
    std::shared_ptr<TlsMultiplexConfig> multiplexConfig_;
    std::unordered_map<NodeNum, IReceiver *> receiversMap_;  // Source endpoint -> receiver object
  };

 private:
  logging::Logger logger_;
  std::shared_ptr<TlsMultiplexReceiver> ownReceiver_;
  std::shared_ptr<TlsMultiplexConfig> multiplexConfig_;
  explicit TlsMultiplexCommunication(const TlsMultiplexConfig &config);
};

}  // namespace bft::communication
